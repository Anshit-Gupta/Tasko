package worker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Anshit-Gupta/Tasko/internals/broker"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/worker"
)

func TestRetryDoesNotBlockWorker(t *testing.T) {
	q := queue.NewQueue(10)
	shutdown := make(chan struct{})

	b := broker.NewBroker("localhost:6379")

	if err := b.Ping(); err != nil {
		t.Fatal("Redis connection failed:", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go worker.Start(q, 1, &wg, b, shutdown)

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

	q.Enqueue(failingTask)
	q.Enqueue(successTask)

	// Wait until we receive the updates we care about.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	var foundRetry bool
	var foundSuccess bool

	for !foundRetry || !foundSuccess {
		update, err := b.ConsumeUpdate(ctx)
		if err != nil {
			t.Fatal("failed to consume task update:", err)
		}

		task := update.Task

		// The failing task should have been scheduled for retry.
		if task.ID == "failing-task" &&
			task.State == queue.Queued &&
			task.RetryCount == 1 {
			foundRetry = true
		}

		// The successful task should eventually be completed.
		if task.ID == "success-task" &&
			task.State == queue.Completed {
			foundSuccess = true
		}
	}

	// Shut down retry scheduler before closing the queue.
	close(shutdown)
	q.Close()

	wg.Wait()
}
