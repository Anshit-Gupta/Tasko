package worker

import (
	"sync"
	"testing"
	"time"

	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/taskstore"
)

func TestRetryDoesNotBlockWorker(t *testing.T) {
	q := queue.NewQueue(10)
	shutdown := make(chan struct{})

	ts, err := taskstore.NewTaskStore("test-tasko.log")
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	go Start(q, 1, &wg, ts, shutdown)

	// This task will fail because the type is invalid.
	failingTask := queue.Task{
		ID:      "failing-task",
		Type:    "invalid",
		Payload: []byte(`{}`),
	}

	// This task should succeed.
	successTask := queue.Task{
		ID:      "success-task",
		Type:    "send_email",
		Payload: []byte(`{"to":"test@example.com"}`),
	}

	if err := ts.Add(failingTask); err != nil {
		t.Fatal(err)
	}

	if err := ts.Add(successTask); err != nil {
		t.Fatal(err)
	}

	q.Enqueue(failingTask)
	q.Enqueue(successTask)

	// Give the worker enough time to process both.
	time.Sleep(500 * time.Millisecond)

	task, exists := ts.Get("success-task")
	if !exists {
		t.Fatal("success task not found")
	}

	if task.State != queue.Completed {
		t.Fatalf("expected success task to be completed, got %s", task.State)
	}

	// The failing task should have been scheduled for retry.
	task, exists = ts.Get("failing-task")
	if !exists {
		t.Fatal("failing task not found")
	}

	if task.RetryCount != 1 {
		t.Fatalf("expected retry count 1, got %d", task.RetryCount)
	}

	// Shut down retry scheduler before closing the queue.
	close(shutdown)
	q.Close()

	wg.Wait()
}