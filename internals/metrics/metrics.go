package metrics

import (
	"sync/atomic"
)

type Metrics struct {
	TasksTotal     atomic.Uint64
	TasksCompleted atomic.Uint64
	TasksFailed    atomic.Uint64
	TasksRetried   atomic.Uint64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) TaskReceived() {
	m.TasksTotal.Add(1)
}

func (m *Metrics) TaskCompleted() {
	m.TasksCompleted.Add(1)
}

func (m *Metrics) TaskFailed() {
	m.TasksFailed.Add(1)
}

func (m *Metrics) TaskRetried() {
	m.TasksRetried.Add(1)
}
