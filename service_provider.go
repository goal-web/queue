package queue

import (
	"github.com/goal-web/contracts"
	"github.com/goal-web/queue/drivers"
)

type ServiceProvider struct {
	app     contracts.Application
	workers []contracts.QueueWorker
}

func (this *ServiceProvider) Register(application contracts.Application) {

	application.Singleton("queue.factory", func(config contracts.Config, serializer contracts.JobSerializer) contracts.QueueFactory {
		return &Factory{
			serializer: serializer,
			queues:     map[string]contracts.Queue{},
			queueDrivers: map[string]contracts.QueueDriver{
				"kafka": drivers.KafkaDriver,
				"nsq":   drivers.NsqDriver,
			},
			config: config.Get("queue").(Config),
		}
	})
	application.Singleton("queue", func(factory contracts.QueueFactory) contracts.Queue {
		return factory.Connection()
	})
	application.Singleton("job.serializer", func(serializer contracts.ClassSerializer) contracts.JobSerializer {
		return NewJobSerializer(serializer)
	})
	this.app = application
}

func (this *ServiceProvider) Start() error {
	this.runWorkers()
	return nil
}

// runWorkers 运行所有 worker
func (this *ServiceProvider) runWorkers() {
	this.app.Call(func(factory contracts.QueueFactory, config contracts.Config, handler contracts.ExceptionHandler, db contracts.DBFactory, serializer contracts.ClassSerializer) {
		var (
			queueConfig = config.Get("queue").(Config)
			env         = this.app.Environment()
		)

		if queueConfig.Workers[env] != nil {
			for name, workerConfig := range queueConfig.Workers[env] {
				worker := NewWorker(name, factory.Connection(workerConfig.Connection), WorkerParam{
					handler:         handler,
					db:              db.Connection(queueConfig.Failed.Database),
					failedJobsTable: queueConfig.Failed.Table,
					config:          workerConfig,
					serializer:      serializer,
				})
				this.workers = append(this.workers, worker)
				go worker.Work()
			}
		}
	})
}

func (this *ServiceProvider) Stop() {
	for _, worker := range this.workers {
		worker.Stop()
	}
}
