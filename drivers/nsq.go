package drivers

import (
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/logs"
	"github.com/nsqio/go-nsq"
	"time"
)

func NsqDriver(name string, config contracts.Fields, serializer contracts.JobSerializer) contracts.Queue {
	var (
		nsqConfig *nsq.Config
		ok        bool
	)
	if nsqConfig, ok = config["config"].(*nsq.Config); !ok {
		nsqConfig = nsq.NewConfig()
	}

	return &Nsq{
		config:          nsqConfig,
		name:            name,
		address:         config["address"].(string),
		lookupAddresses: config["lookup_addresses"].([]string),
		defaultQueue:    config["default"].(string),
		serializer:      serializer,
		consumers:       make(map[string]*nsq.Consumer),
	}
}

type Nsq struct {
	name            string
	address         string
	lookupAddresses []string
	defaultQueue    string
	serializer      contracts.JobSerializer
	config          *nsq.Config
	consumers       map[string]*nsq.Consumer
	producer        *nsq.Producer
}

func (client *Nsq) getQueue(queues []string, queue string) string {
	if len(queues) > 0 {
		return queues[0]
	}
	if queue != "" {
		return queue
	}
	return client.defaultQueue
}

func (client *Nsq) getConsumer(queue string) *nsq.Consumer {
	if client.consumers[queue] != nil {
		return client.consumers[queue]
	}
	consumer, err := nsq.NewConsumer(queue, "channel", client.config)
	if err != nil {
		panic(err)
	}

	client.consumers[queue] = consumer
	return consumer
}

func (client *Nsq) getProducer() *nsq.Producer {
	if client.producer != nil {
		return client.producer
	}

	producer, err := nsq.NewProducer(client.address, client.config)

	if err != nil {
		panic(err)
	}

	client.producer = producer

	return client.producer
}

func (client *Nsq) Push(job contracts.Job, queue ...string) error {
	return client.PushOn(client.getQueue(queue, job.GetQueue()), job)
}

func (client *Nsq) PushOn(queue string, job contracts.Job) error {
	job.SetQueue(queue)
	return client.getProducer().Publish(queue, []byte(client.serializer.Serializer(job)))
}

func (client *Nsq) PushRaw(payload, queue string, options ...contracts.Fields) error {
	return client.getProducer().Publish(queue, []byte(payload))
}

func (client *Nsq) Later(delay time.Time, job contracts.Job, queue ...string) error {
	return client.LaterOn(client.getQueue(queue, job.GetQueue()), delay, job)
}

func (client *Nsq) LaterOn(queue string, delay time.Time, job contracts.Job) error {
	job.SetQueue(queue)

	return client.getProducer().DeferredPublish(queue, time.Until(delay), []byte(client.serializer.Serializer(job)))
}

func (client *Nsq) GetConnectionName() string {
	return client.name
}

func (client *Nsq) Release(job contracts.Job, delay ...int) error {
	delayAt := time.Now()
	if len(delay) > 0 {
		delayAt = delayAt.Add(time.Second * time.Duration(delay[0]))
	}

	return client.Later(delayAt, job)
}

func (client *Nsq) Stop() {
	for _, consumer := range client.consumers {
		consumer.Stop()
	}
}

func (client *Nsq) Listen(queue ...string) chan contracts.QueueMsg {
	ch := make(chan contracts.QueueMsg)

	for _, name := range queue {
		client.consume(client.getConsumer(name), ch)
	}

	return ch
}

func (client *Nsq) consume(consumer *nsq.Consumer, ch chan contracts.QueueMsg) {
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		var job, err = client.serializer.Unserialize(string(message.Body))
		if err != nil {
			logs.WithError(err).WithField("msg", string(message.Body)).Error("nsq.consume: Unserialize job failed")
			return nil
		}
		ackChan := make(chan error)
		ch <- contracts.QueueMsg{
			Ack: func() { ackChan <- nil },
			Job: job,
		}
		return <-ackChan
	}))

	if len(client.lookupAddresses) > 0 && client.lookupAddresses[0] != "" {
		if err := consumer.ConnectToNSQLookupds(client.lookupAddresses); err != nil {
			panic(err)
		}
	} else {
		if err := consumer.ConnectToNSQD(client.address); err != nil {
			panic(err)
		}
	}

}
