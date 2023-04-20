package drivers

import (
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/exceptions"
	"time"
)

type Sync struct {
	name string
	ch   chan contracts.QueueMsg
}

func SyncDriver(name string, config contracts.Fields, serializer contracts.JobSerializer) contracts.Queue {
	return &Sync{
		name: name,
		ch:   make(chan contracts.QueueMsg),
	}
}

func (sync Sync) Push(job contracts.Job, queue ...string) (err error) {
	defer func() {
		err = exceptions.WithRecover(recover())
	}()
	job.Handle()
	return
}

func (sync Sync) PushOn(queue string, job contracts.Job) error {
	return sync.Push(job)
}

func (sync Sync) PushRaw(payload, queue string, options ...contracts.Fields) error {
	return nil
}

func (sync Sync) Later(delay time.Time, job contracts.Job, queue ...string) error {
	return sync.Push(job, queue...)
}

func (sync Sync) LaterOn(queue string, delay time.Time, job contracts.Job) error {
	return sync.Push(job, queue)
}

func (sync Sync) GetConnectionName() string {
	return sync.name
}

func (sync Sync) Release(job contracts.Job, delay ...int) error {
	return nil
}

func (sync Sync) Listen(queue ...string) chan contracts.QueueMsg {
	return sync.ch
}

func (sync Sync) Stop() {
	close(sync.ch)
}
