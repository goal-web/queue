package queue

import (
	"github.com/goal-web/contracts"
	"github.com/goal-web/queue/drivers"
	"github.com/goal-web/supports/logs"
)

type ServiceProvider struct {
	app         contracts.Application
	workers     []contracts.QueueWorker
	stopChan    chan error
	withWorkers bool
}

func NewService(withWorkers bool) contracts.ServiceProvider {
	return &ServiceProvider{withWorkers: withWorkers}
}

func (provider *ServiceProvider) Register(application contracts.Application) {
	provider.app = application
	application.Singleton("queue.manager", func(config contracts.Config, serializer contracts.JobSerializer) contracts.QueueManager {
		return &Manager{
			serializer: serializer,
			queues:     map[string]contracts.Queue{},
			queueDrivers: map[string]contracts.QueueDriver{
				"kafka":     drivers.KafkaDriver,
				"nsq":       drivers.NsqDriver,
				"sync":      drivers.SyncDriver,
				"empty":     drivers.EmptyDriver,
				"goroutine": drivers.GoroutineDriver,
			},
			config: config.Get("queue").(Config),
		}
	})
	application.Singleton("queue", func(factory contracts.QueueManager) contracts.Queue {
		return factory.Connection()
	})
	application.Singleton("job.serializer", func(serializer contracts.ClassSerializer) contracts.JobSerializer {
		return NewJobSerializer(serializer)
	})
}

func (provider *ServiceProvider) Start() error {
	logs.Default().Info("queue.ServiceProvider: starting...")
	if provider.withWorkers {
		err := provider.runWorkers()
		if err != nil {
			logs.Default().WithError(err).Error("queue.ServiceProvider: abnormal exit.")
		} else {
			logs.Default().Info("queue.ServiceProvider: finished.")
		}
		return err
	}
	return nil
}

// runWorkers 运行所有 worker
func (provider *ServiceProvider) runWorkers() error {
	provider.app.Call(func(factory contracts.QueueManager, config contracts.Config, handler contracts.ExceptionHandler, db contracts.DBFactory, serializer contracts.ClassSerializer) {
		var queueConfig = config.Get("queue").(Config)
		var env = config.GetString("app.env")

		if queueConfig.Workers[env] != nil {
			provider.stopChan = make(chan error, len(queueConfig.Workers[env]))
			for name, workerConfig := range queueConfig.Workers[env] {
				worker := NewWorker(name, factory.Connection(workerConfig.Connection), WorkerParam{
					handler:         handler,
					db:              db.Connection(queueConfig.Failed.Database),
					failedJobsTable: queueConfig.Failed.Table,
					config:          workerConfig,
					serializer:      serializer,
					failChan:        provider.stopChan,
				})
				provider.workers = append(provider.workers, worker)
				go worker.Work()
			}
		}
	})
	if provider.stopChan != nil {
		return <-provider.stopChan
	}
	return nil
}

func (provider *ServiceProvider) Stop() {
	logs.Default().Info("queue.ServiceProvider: stopping...")
	if provider.withWorkers {
		for _, worker := range provider.workers {
			worker.Stop()
		}
		logs.Default().Debug("queue.workers closed")
		if provider.stopChan != nil {
			provider.stopChan <- nil
			close(provider.stopChan)
		}
	}
}
