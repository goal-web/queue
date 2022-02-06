package queue

import (
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/exceptions"
	"github.com/goal-web/supports/logs"
	"github.com/qbhy/parallel"
)

type Worker struct {
	name             string
	queue            contracts.Queue
	closeChan        chan bool
	exceptionHandler contracts.ExceptionHandler
	workers          *parallel.Workers
	config           WorkerConfig
}

func NewWorker(name string, queue contracts.Queue, config WorkerConfig, handler contracts.ExceptionHandler) contracts.QueueWorker {
	return &Worker{
		name:             name,
		queue:            queue,
		closeChan:        make(chan bool),
		exceptionHandler: handler,
		config:           config,
	}
}

func (worker *Worker) workQueue(queue contracts.Queue) {
	defer func() {
		if err := recover(); err != nil {
			logs.WithException(exceptions.WithRecover(err, nil)).Error("workQueue failed")
		}
	}()
	msgPipe := queue.Listen(worker.config.Queue...)
	for {
		select {
		case msg := <-msgPipe:
			err := worker.workers.Handle(func() {
				job := msg.Job
				if err := worker.handleJob(job); err != nil {
					if job.GetAttemptsNum() >= job.GetMaxTries() { // 达到最大尝试次数
						worker.saveOnFailedJobs(msg.Job) // 保存到死信队列
						msg.Ack()
					} else {
						queue.Release(job) // 放回队列中
					}
					worker.exceptionHandler.Handle(JobException{Exception: exceptions.WithError(err, contracts.Fields{
						"msg": msg,
					})})
				} else {
					msg.Ack()
				}
			})
			if err != nil {
				logs.WithError(err).Warn("workers failed")
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
}

func (worker *Worker) saveOnFailedJobs(job contracts.Job) {
	logs.Default().WithField("job", job).Info("saveOnFailedJobs")
}

func (worker *Worker) handleJob(job contracts.Job) (err error) {
	defer func() {
		if panicValue := recover(); panicValue != nil {
			err = exceptions.ResolveException(panicValue)
		}
	}()

	job.IncrementAttemptsNum()
	job.Handle()

	return nil
}
