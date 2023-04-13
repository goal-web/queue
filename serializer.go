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

func (serializer *Serializer) Serializer(job contracts.Job) string {
	return serializer.serializer.Serialize(job)
}

func (serializer *Serializer) Unserialize(serialized string) (contracts.Job, error) {
	var result, err = serializer.serializer.Parse(serialized)
	if err != nil {
		return nil, err
	}

	if job, isJob := result.(contracts.Job); isJob {
		return job, nil
	}

	return nil, JobUnserializeError
}
