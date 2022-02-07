package drivers

import (
	"context"
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/logs"
	"github.com/goal-web/supports/utils"
	"github.com/segmentio/kafka-go"
	"time"
)

func Driver(name string, config contracts.Fields, serializer contracts.JobSerializer) contracts.Queue {
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
		serializer:   serializer,
		dialer:       dialer,
		readers:      make(map[string]*kafka.Reader),
	}
}

type Kafka struct {
	name         string
	brokers      []string
	defaultQueue string
	serializer   contracts.JobSerializer
	stopped      bool
	dialer       *kafka.Dialer
	readers      map[string]*kafka.Reader
	writer       *kafka.Writer
}

func (this *Kafka) getQueue(queues []string, queue string) string {
	if len(queues) > 0 {
		return queues[0]
	}
	if queue != "" {
		return queue
	}
	return this.defaultQueue
}

func (this *Kafka) Size() int64 {
	//TODO implement me
	panic("implement me")
}

func (this *Kafka) getReader(queue string) *kafka.Reader {
	if this.readers[queue] != nil {
		return this.readers[queue]
	}
	this.readers[queue] = kafka.NewReader(kafka.ReaderConfig{
		Brokers: this.brokers,
		GroupID: this.name,
		Topic:   queue,
		Dialer:  this.dialer,
	})
	return this.readers[queue]
}

func (this *Kafka) getWriter() *kafka.Writer {
	if this.writer != nil {
		return this.writer
	}
	this.writer = &kafka.Writer{
		Addr:     kafka.TCP(this.brokers[0]),
		Balancer: &kafka.LeastBytes{},
	}
	return this.writer
}

func (this *Kafka) Push(job contracts.Job, queue ...string) {
	this.PushOn(this.getQueue(queue, job.GetQueue()), job)
}

func (this *Kafka) PushOn(queue string, job contracts.Job) {
	this.LaterOn(queue, time.Now(), job)
}

func (this *Kafka) PushRaw(payload, queue string, options ...contracts.Fields) error {
	err := this.getWriter().WriteMessages(context.Background(), kafka.Message{
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

func (this *Kafka) Later(delay time.Time, job contracts.Job, queue ...string) {
	this.LaterOn(this.getQueue(queue, job.GetQueue()), delay, job)
}

func (this *Kafka) LaterOn(queue string, delay time.Time, job contracts.Job) {
	err := this.getWriter().WriteMessages(context.Background(), kafka.Message{
		Topic: queue,
		Key:   []byte(job.Uuid()),
		Value: []byte(this.serializer.Serializer(job)),
		Time:  delay,
	})
	if err != nil {
		logs.WithError(err).WithField("job", job).Debug("push on queue failed")
	}
}

func (this *Kafka) GetConnectionName() string {
	return this.name
}

func (this *Kafka) Release(job contracts.Job, delay ...int) {
	delayAt := time.Now()
	if len(delay) > 0 {
		delayAt = delayAt.Add(time.Second * time.Duration(delay[0]))
	}

	this.Later(delayAt, job)
}

func (this *Kafka) Delete(job contracts.Job) {
	logs.Default().Info("Delete job" + job.Uuid())
}

func (this *Kafka) Stop() {
	this.stopped = true
}

func (this *Kafka) Listen(queue ...string) chan contracts.Msg {
	ch := make(chan contracts.Msg)
	for _, name := range queue {
		go this.consume(this.getReader(name), ch)
	}
	return ch
}

func (this *Kafka) consume(reader *kafka.Reader, ch chan contracts.Msg) {
	ctx := context.Background()
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			logs.WithError(err).WithField("config", reader.Config()).Error("kafka.consume: FetchMessage failed")
			break
		}
		job, err := this.serializer.Unserialize(string(msg.Value))
		if err != nil {
			logs.WithError(err).WithField("msg", msg).WithField("config", reader.Config()).Error("kafka.consume: Unserialize job failed")
			break
		}
		(func(message kafka.Message) {
			ch <- contracts.Msg{
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
