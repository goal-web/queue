package queue

import (
	"errors"
	"github.com/goal-web/contracts"
)

var (
	JobUnserializeError = errors.New("unserialize job failed")
)

type Serializer struct {
	serializer contracts.ClassSerializer
}

func NewJobSerializer(serializer contracts.ClassSerializer) contracts.JobSerializer {
	return &Serializer{serializer: serializer}
}

func (this *Serializer) Serializer(job contracts.Job) string {
	return this.serializer.Serialize(job)
}

func (this *Serializer) Unserialize(serialized string) (contracts.Job, error) {
	result, err := this.serializer.Parse(serialized)
	if err != nil {
		return nil, err
	}

	if job, isJob := result.(contracts.Job); isJob {
		return job, nil
	}

	return nil, JobUnserializeError
}
