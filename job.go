package queue

import (
	"errors"
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/utils"
	"time"
)

var JobFailedErr = errors.New("job failed")

type Job struct {
	UUID          string           `json:"uuid"`
	CreatedAt     int64            `json:"created_at"`
	Queue         string           `json:"queue"`
	Connection    string           `json:"connection"`
	Tries         int              `json:"tries"`
	MaxTries      int              `json:"max_tries"`
	IsDelete      bool             `json:"is_delete"`
	Options       contracts.Fields `json:"options"`
	IsRelease     bool             `json:"is_released"`
	Error         error            `json:"error"`
	Timeout       int
	RetryInterval int
}

func BaseJob(queue string) *Job {
	return &Job{
		UUID:          utils.RandStr(30),
		CreatedAt:     time.Now().Unix(),
		Queue:         queue,
		Tries:         0,
		MaxTries:      0,
		IsDelete:      false,
		Options:       nil,
		RetryInterval: 3,
	}
}

func (job *Job) Handle() {
}

func (job *Job) Uuid() string {
	return job.UUID
}

func (job *Job) GetOptions() contracts.Fields {
	return job.Options
}

func (job *Job) IsReleased() bool {
	return job.IsRelease
}

func (job *Job) IsDeleted() bool {
	return job.IsDelete
}

func (job *Job) IsDeletedOrReleased() bool {
	return job.IsDelete || job.IsRelease
}

func (job *Job) Attempts() int {
	return job.Tries
}

func (job *Job) HasFailed() bool {
	return job.Error != nil
}

func (job *Job) MarkAsFailed() {
	job.Error = JobFailedErr
}

func (job *Job) Fail(err error) {
	job.Error = err
}

func (job *Job) GetMaxTries() int {
	return job.MaxTries
}

func (job *Job) GetAttemptsNum() int {
	return job.Tries
}

func (job *Job) GetRetryInterval() int {
	return job.RetryInterval
}

func (job *Job) IncrementAttemptsNum() {
	job.Tries++
}

func (job *Job) GetTimeout() int {
	return job.Timeout
}

func (job *Job) GetConnectionName() string {
	return job.Connection
}

func (job *Job) GetQueue() string {
	return job.Queue
}

func (job *Job) SetQueue(queue string) {
	job.Queue = queue
}
