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
}

func (factory *Factory) Connection(name ...string) contracts.Queue {
	if len(name) > 0 {
		return factory.Queue(name[0])
	}

	return factory.Queue(factory.config.Default)
}

func (factory *Factory) Queue(name string) contracts.Queue {
	if queue, exists := factory.queues[name]; exists {
		return queue
	}

	config := factory.config.Connections[name]
	driver := utils.GetStringField(config, "driver")

	if queueDriver, exists := factory.queueDrivers[driver]; exists {
		factory.queues[name] = queueDriver(config)
		return factory.queues[name]
	}

	panic(DriverException{
		Exception: exceptions.New(fmt.Sprintf("unsupported queue driver：%s", driver), config),
	})
}
