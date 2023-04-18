package drivers

import (
	"github.com/goal-web/contracts"
	"time"
)

type Empty struct {
	name string
	ch   chan contracts.Msg
}

func EmptyDriver(name string, config contracts.Fields, serializer contracts.JobSerializer) contracts.Queue {
	return &Empty{
		name: name,
		ch:   make(chan contracts.Msg),
	}
}

func (sync Empty) Push(job contracts.Job, queue ...string) (err error) {
	return
}

func (sync Empty) PushOn(queue string, job contracts.Job) error {
	return sync.Push(job)
}

func (sync Empty) PushRaw(payload, queue string, options ...contracts.Fields) error {
	return nil
}

func (sync Empty) Later(delay time.Time, job contracts.Job, queue ...string) error {
	return nil
}

func (sync Empty) LaterOn(queue string, delay time.Time, job contracts.Job) error {
	return sync.Later(delay, job, queue)
}

func (sync Empty) GetConnectionName() string {
	return sync.name
}

func (sync Empty) Release(job contracts.Job, delay ...int) error {
	return nil
}

func (sync Empty) Listen(queue ...string) chan contracts.Msg {
	return sync.ch
}

func (sync Empty) Stop() {
	close(sync.ch)
}
