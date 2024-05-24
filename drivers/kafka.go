package drivers

import (
	"context"
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/logs"
	"github.com/goal-web/supports/utils"
	"github.com/segmentio/kafka-go"
	"time"
)

func KafkaDriver(name string, config contracts.Fields, serializer contracts.JobSerializer) contracts.Queue {
	var (
		dialer *kafka.Dialer
		ok     bool
	)

	if dialer, ok = config["dialer"].(*kafka.Dialer); !ok {
		dialer = &kafka.Dialer{Timeout: 10 * time.Second, DualStack: true}
	}
	return &Kafka{
		name:         name,
		brokers:      config["brokers"].([]string),
		defaultQueue: config["default"].(string),
		delayQueue:   config["delay"].(string),
		serializer:   serializer,
		dialer:       dialer,
		readers:      make(map[string]*kafka.Reader),
	}
}

type Kafka struct {
	delayQueue   string
	name         string
	brokers      []string
	defaultQueue string
	serializer   contracts.JobSerializer
	stopped      bool
	dialer       *kafka.Dialer
	readers      map[string]*kafka.Reader
	writer       *kafka.Writer
}

func (k *Kafka) getQueue(queues []string, queue string) string {
	if len(queues) > 0 {
		return queues[0]
	}
	if queue != "" {
		return queue
	}
	return k.defaultQueue
}

func (k *Kafka) getReader(queue string) *kafka.Reader {
	if k.readers[queue] != nil {
		return k.readers[queue]
	}
	k.readers[queue] = kafka.NewReader(kafka.ReaderConfig{
		Brokers: k.brokers,
		GroupID: k.name,
		Topic:   queue,
		Dialer:  k.dialer,
	})
	return k.readers[queue]
}

func (k *Kafka) getWriter() *kafka.Writer {
	if k.writer != nil {
		return k.writer
	}
	k.writer = &kafka.Writer{
		Addr:     kafka.TCP(k.brokers[0]),
		Balancer: &kafka.LeastBytes{},
	}
	return k.writer
}

func (k *Kafka) Push(job contracts.Job, queue ...string) error {
	return k.PushOn(k.getQueue(queue, job.GetQueue()), job)
}

func (k *Kafka) PushOn(queue string, job contracts.Job) error {
	job.SetQueue(queue)

	err := k.getWriter().WriteMessages(context.Background(), kafka.Message{
		Topic: queue,
		Key:   []byte(job.Uuid()),
		Value: []byte(k.serializer.Serializer(job)),
	})
	if err != nil {
		logs.WithError(err).WithField("job", job).Debug("push on queue failed")
	}
	return err
}

func (k *Kafka) PushRaw(payload, queue string, options ...contracts.Fields) error {
	err := k.getWriter().WriteMessages(context.Background(), kafka.Message{
		Topic: queue,
		Key:   []byte(utils.RandStr(5)),
		Value: []byte(payload),
	})
	if err != nil {
		logs.WithError(err).
			WithField("queue", queue).
			WithField("payload", payload).
			Debug("push on queue failed")
	}
	return err
}

func (k *Kafka) Later(delay time.Time, job contracts.Job, queue ...string) error {
	return k.LaterOn(k.getQueue(queue, job.GetQueue()), delay, job)
}

func (k *Kafka) LaterOn(queue string, delay time.Time, job contracts.Job) error {
	job.SetQueue(queue)

	err := k.getWriter().WriteMessages(context.Background(), kafka.Message{
		Topic: k.delayQueue,
		Key:   []byte(job.Uuid()),
		Value: []byte(k.serializer.Serializer(job)),
		Time:  delay,
	})
	if err != nil {
		logs.WithError(err).WithField("job", job).Debug("push on queue failed")
	}
	return err
}

func (k *Kafka) GetConnectionName() string {
	return k.name
}

func (k *Kafka) Release(job contracts.Job, delay ...int) error {
	delayAt := time.Now()
	if len(delay) > 0 {
		delayAt = delayAt.Add(time.Second * time.Duration(delay[0]))
	}

	return k.Later(delayAt, job)
}

func (k *Kafka) Stop() {
	k.stopped = true
}

func (k *Kafka) Listen(queue ...string) chan contracts.QueueMsg {
	k.stopped = false
	ch := make(chan contracts.QueueMsg)
	for _, name := range queue {
		go k.consume(k.getReader(name), ch)
	}
	go k.maintainDelayQueue()
	return ch
}

func (k *Kafka) consume(reader *kafka.Reader, ch chan contracts.QueueMsg) {
	ctx := context.Background()
	for {
		if k.stopped {
			break
		}
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			logs.WithError(err).WithField("config", reader.Config()).Error("kafka.consume: FetchMessage failed")
			break
		}
		if time.Until(msg.Time) > 0 {
			logs.Default().Info("未来的消息" + msg.Time.String() + string(msg.Value))
			continue
		}
		job, err := k.serializer.Unserialize(string(msg.Value))
		if err != nil {
			logs.WithError(err).WithField("msg", string(msg.Value)).WithField("config", reader.Config()).Error("kafka.consume: Unserialize job failed")
			break
		}
		(func(message kafka.Message) {
			ch <- contracts.QueueMsg{
				Ack: func() {
					if err = reader.CommitMessages(ctx, message); err != nil {
						logs.WithError(err).WithField("message", message).Error("kafka.consume: CommitMessages failed")
					}
				},
				Job: job,
			}
		})(msg)
	}
}

func (k *Kafka) maintainDelayQueue() {
	reader := k.getReader(k.delayQueue)
	ctx := context.Background()
	for {
		if k.stopped {
			break
		}
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			logs.WithError(err).WithField("config", reader.Config()).Error("kafka.maintainDelayQueue: FetchMessage failed")
			break
		}
		job, err := k.serializer.Unserialize(string(msg.Value))
		if err != nil {
			logs.WithError(err).WithField("msg", string(msg.Value)).WithField("config", reader.Config()).Error("kafka.consume: Unserialize job failed")
			break
		}
		if time.Until(msg.Time) > 0 { // 还没到时间
			go (func(message kafka.Message) {
				err = k.LaterOn(job.GetQueue(), msg.Time, job)
				if err != nil {
					logs.WithError(err).WithField("queue", string(message.Value)).Error("kafka.maintainDelayQueue: LaterOn failed")
					return
				}
				if err = reader.CommitMessages(ctx, message); err != nil {
					logs.WithError(err).WithField("message", string(message.Value)).Error("kafka.maintainDelayQueue: CommitMessages failed(delay)")
				}
			})(msg)
		} else {
			go (func(message kafka.Message) {
				if err = k.PushOn(job.GetQueue(), job); err != nil {
					logs.WithError(err).WithField("queue", job.GetQueue()).Error("kafka.maintainDelayQueue: PushOn failed")
					return
				}
				if err = reader.CommitMessages(ctx, message); err != nil {
					logs.WithError(err).WithField("message", string(message.Value)).Error("kafka.maintainDelayQueue: CommitMessages failed")
				}
			})(msg)
		}
	}
}
