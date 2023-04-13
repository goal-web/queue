package queue

import (
	"fmt"
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/exceptions"
	"github.com/goal-web/supports/utils"
)

type Factory struct {
	queues       map[string]contracts.Queue
	queueDrivers map[string]contracts.QueueDriver
	config       Config
	serializer   contracts.JobSerializer
}

func (factory *Factory) Connection(name ...string) contracts.Queue {
	if len(name) > 0 {
		return factory.Queue(name[0])
	}

	return factory.Queue(factory.config.Defaults.Connection)
}

func (factory *Factory) Extend(name string, driver contracts.QueueDriver) {
	factory.queueDrivers[name] = driver
}

func (factory *Factory) Queue(name string) contracts.Queue {
	if queue, exists := factory.queues[name]; exists {
		return queue
	}

	config := factory.config.Connections[name]
	driver := utils.GetStringField(config, "driver")
	if config["default"] == nil {
		config["default"] = factory.config.Defaults.Queue
	}

	if queueDriver, exists := factory.queueDrivers[driver]; exists {
		factory.queues[name] = queueDriver(name, config, factory.serializer)
		return factory.queues[name]
	}

	panic(DriverException{Err: exceptions.New(fmt.Sprintf("unsupported queue driver：%s", driver))})
}
