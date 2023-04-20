package drivers

import (
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/logs"
	"github.com/goal-web/supports/utils"
	"sync"
	"time"
)

type Goroutine struct {
	name            string
	ch              map[string]chan contracts.QueueMsg
	mutex           sync.Mutex
	summaryChannels []chan contracts.QueueMsg
}

func GoroutineDriver(name string, config contracts.Fields, serializer contracts.JobSerializer) contracts.Queue {
	return &Goroutine{
		name: name,
		ch:   make(map[string]chan contracts.QueueMsg),
	}
}

func (goroutine *Goroutine) channel(queue string) chan contracts.QueueMsg {
	goroutine.mutex.Lock()
	defer goroutine.mutex.Unlock()
	if goroutine.ch[queue] == nil {
		goroutine.ch[queue] = make(chan contracts.QueueMsg)
	}
	return goroutine.ch[queue]
}

func (goroutine *Goroutine) Push(job contracts.Job, queue ...string) (err error) {
	return goroutine.PushOn(utils.DefaultString(queue, job.GetQueue()), job)
}

func (goroutine *Goroutine) PushOn(queue string, job contracts.Job) error {
	job.SetQueue(queue)
	go func() {
		goroutine.channel(queue) <- contracts.QueueMsg{
			Ack: func() {},
			Job: job,
		}
	}()
	return nil
}

func (goroutine *Goroutine) PushRaw(payload, queue string, options ...contracts.Fields) error {
	return nil
}

func (goroutine *Goroutine) Later(delay time.Time, job contracts.Job, queue ...string) error {
	go func() {
		time.Sleep(time.Now().Sub(delay))
		if err := goroutine.Push(job, queue...); err != nil {
			logs.Default().
				WithField("conn", goroutine.name).
				WithField("queue", queue).
				WithError(err).
				Error("Failed to process goroutine queue.")
		}
	}()
	return nil
}

func (goroutine *Goroutine) LaterOn(queue string, delay time.Time, job contracts.Job) error {
	return goroutine.Later(delay, job, queue)
}

func (goroutine *Goroutine) GetConnectionName() string {
	return goroutine.name
}

func (goroutine *Goroutine) Release(job contracts.Job, delay ...int) error {
	return nil
}

func (goroutine *Goroutine) Listen(queue ...string) chan contracts.QueueMsg {
	ch := make(chan contracts.QueueMsg)
	for _, tmpName := range queue {
		go func(name string) {
			for msg := range goroutine.channel(name) {
				ch <- msg
			}
		}(tmpName)
	}
	goroutine.summaryChannels = append(goroutine.summaryChannels, ch)
	return ch
}

func (goroutine *Goroutine) Stop() {
	for _, ch := range goroutine.ch {
		close(ch)
	}
	for _, ch := range goroutine.summaryChannels {
		close(ch)
	}
}
