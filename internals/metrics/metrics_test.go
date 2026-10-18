package metrics

import (
	"testing"
)

func TestMetrics(t *testing.T) {
	m := NewMetrics()

	m.TaskReceived()
	m.TaskReceived()
	m.TaskRetried()
	m.TaskCompleted()
	m.TaskFailed()

	if m.TasksTotal.Load() != 2 {
		t.Fatalf("expected total 2, got %d", m.TasksTotal.Load())
	}

	if m.TasksRetried.Load() != 1 {
		t.Fatalf("expected retried 1, got %d", m.TasksRetried.Load())
	}

	if m.TasksCompleted.Load() != 1 {
		t.Fatalf("expected completed 1, got %d", m.TasksCompleted.Load())
	}

	if m.TasksFailed.Load() != 1 {
		t.Fatalf("expected failed 1, got %d", m.TasksFailed.Load())
	}
}
