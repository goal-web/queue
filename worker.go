package queue

import (
	"fmt"
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/exceptions"
	"github.com/goal-web/supports/logs"
	"github.com/qbhy/parallel"
	"runtime/debug"
	"time"
)

type Worker struct {
	name             string
	queue            contracts.Queue
	closeChan        chan bool
	exceptionHandler contracts.ExceptionHandler
	workers          *parallel.Workers
	config           WorkerConfig

	db              contracts.DBConnection
	serializer      contracts.ClassSerializer
	failedJobsTable string
	failChan        chan error
	dbIsReady       bool // db 是否准备好了死信队列数据表
}

type WorkerParam struct {
	handler         contracts.ExceptionHandler
	db              contracts.DBConnection
	failedJobsTable string
	config          WorkerConfig
	serializer      contracts.ClassSerializer
	failChan        chan error
}

func NewWorker(name string, queue contracts.Queue, param WorkerParam) contracts.QueueWorker {
	return &Worker{
		db:               param.db,
		dbIsReady:        true,
		failedJobsTable:  param.failedJobsTable,
		serializer:       param.serializer,
		name:             name,
		queue:            queue,
		closeChan:        make(chan bool),
		exceptionHandler: param.handler,
		config:           param.config,
		failChan:         param.failChan,
	}
}

func (worker *Worker) workQueue(queue contracts.Queue) {
	defer func() {
		if err := recover(); err != nil {
			e := exceptions.WithRecover(err)
			logs.WithException(e).Error("worker.workQueue failed")
			worker.failChan <- e
		}
	}()
	var msgPipe = queue.Listen(worker.config.Queue...)
	logs.Default().Info(fmt.Sprintf("queue.Worker.workQueue: %s worker is working...", worker.name))
	for {
		select {
		case msg := <-msgPipe:
			var err = worker.workers.Handle(func() {
				var job = msg.Job
				logs.Default().WithField("job", job).Debug(fmt.Sprintf("queue.Worker.workQueue: processing job"))
				if err := worker.handleJob(job); err != nil {
					logs.Default().WithField("job", job).Debug(fmt.Sprintf("queue.Worker.workQueue: Failed to process job"))
					job.Fail(err)
					if (job.GetMaxTries() > 0 && job.GetAttemptsNum() >= job.GetMaxTries()) || job.GetAttemptsNum() >= worker.config.Tries { // 达到最大尝试次数
						// 保存到死信队列
						if saveErr := worker.saveOnFailedJobs(msg.Job); saveErr != nil {
							panic(err)
						}
					} else {
						// 放回队列中重试
						if err = queue.Later(time.Now().Add(time.Second*time.Duration(job.GetRetryInterval())), job); err != nil {
							logs.WithError(err).Warn("queue.Worker.workQueue: job release failed")
							panic(err)
						}
					}
					msg.Ack()
					worker.exceptionHandler.Handle(&JobException{Err: err})
				} else {
					logs.Default().WithField("job", job).Debug(fmt.Sprintf("queue.Worker.workQueue: Processing job succeeded"))
					msg.Ack()
				}
			})
			if err != nil {
				return
			}
		case <-worker.closeChan:
			queue.Stop()
			return
		}
	}
}

func (worker *Worker) Work() {
	worker.workers = parallel.NewWorkers(worker.config.Processes)
	worker.workQueue(worker.queue)
}

func (worker *Worker) Stop() {
	worker.closeChan <- true
	worker.workers.Stop()
	logs.Default().Info(fmt.Sprintf("queue.Worker.workQueue: %s worker is stopped.", worker.name))
}

// saveOnFailedJobs 保存死信
func (worker *Worker) saveOnFailedJobs(job contracts.Job) (err error) {
	if worker.dbIsReady && worker.db != nil {
		_, exception := worker.db.Exec(
			fmt.Sprintf("insert into %s (connection, queue, payload, exception) values ('%s','%s','%s','%s')",
				worker.failedJobsTable,
				job.GetConnectionName(),
				job.GetQueue(),
				worker.serializer.Serialize(job),
				debug.Stack(),
			),
		)
		if exception != nil {
			err = exception
			logs.WithException(exception).Warn("queue.Worker.saveOnFailedJobs: Failed to save to database")
			worker.dbIsReady = false
		}
	}

	if err != nil || !worker.dbIsReady { // 如果没有配置数据库死信，或者保存到数据库失败了
		if err = worker.queue.Push(job, fmt.Sprintf("deaded_%s", job.GetQueue())); err != nil {
			logs.WithError(err).Error("queue.Worker.saveOnFailedJobs: failed to save")
		}
	}
	return
}

func (worker *Worker) handleJob(job contracts.Job) (err error) {
	defer func() {
		err = exceptions.WithRecover(recover())
	}()

	job.IncrementAttemptsNum()
	job.Handle()

	return nil
}
