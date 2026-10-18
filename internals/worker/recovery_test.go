package worker_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Anshit-Gupta/Tasko/internals/broker"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/taskstore"
	"github.com/Anshit-Gupta/Tasko/internals/worker"
)

func TestRecoveredTaskIsProcessed(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "recovery.log")

	// --------------------------------------------------
	// Simulate the previous run.
	// A task was being processed when Tasko crashed.
	// --------------------------------------------------

	ts, err := taskstore.NewTaskStore(logPath)
	if err != nil {
		t.Fatalf("failed to create taskstore: %v", err)
	}

	task := queue.Task{
		ID:      "recovered-1",
		Type:    "send_email",
		Payload: []byte(`{"to":"test@email.com"}`),
	}

	if err := ts.Add(task); err != nil {
		t.Fatalf("failed to add task: %v", err)
	}

	if err := ts.Update(task, queue.Processing); err != nil {
		t.Fatalf("failed to update task: %v", err)
	}

	if err := ts.Close(); err != nil {
		t.Fatalf("failed to close taskstore: %v", err)
	}

	// --------------------------------------------------
	// Simulate Tasko starting again.
	// --------------------------------------------------

	ts, err = taskstore.NewTaskStore(logPath)
	if err != nil {
		t.Fatalf("failed to recover taskstore: %v", err)
	}
	defer ts.Close()

	q := queue.NewQueue(10)

	// Redis broker.
	b := broker.NewBroker()

	if err := b.Ping(); err != nil {
		t.Fatalf("Redis connection failed: %v", err)
	}

	// --------------------------------------------------
	// Recovery logic.
	// Processing tasks are recovered as Queued.
	// --------------------------------------------------

	recoveredTasks := ts.QueuedTasks()

	if len(recoveredTasks) != 1 {
		t.Fatalf(
			"expected 1 recovered task, got %d",
			len(recoveredTasks),
		)
	}

	for _, task := range recoveredTasks {
		q.Enqueue(task)
	}

	// --------------------------------------------------
	// Start worker.
	// --------------------------------------------------

	var wg sync.WaitGroup
	wg.Add(1)

	shutdown := make(chan struct{})

	go worker.Start(q, 1, &wg, b, shutdown)

	// No more tasks will be added.
	q.Close()

	// Wait for worker to finish.
	wg.Wait()

	// --------------------------------------------------
	// Verify worker published task updates.
	// --------------------------------------------------

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	var completed bool

	for !completed {
		update, err := b.ConsumeUpdate(ctx)
		if err != nil {
			t.Fatalf("failed to consume task update: %v", err)
		}

		if update.Task.ID == task.ID &&
			update.Task.State == queue.Completed {
			completed = true
		}
	}

	if !completed {
		t.Fatal("expected recovered task to be completed")
	}
}
