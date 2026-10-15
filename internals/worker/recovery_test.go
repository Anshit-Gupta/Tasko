package worker_test

import (
	"path/filepath"
	"sync"
	"testing"

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

	// This is the exact recovery logic from main.
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

	// Start a worker.
	var wg sync.WaitGroup
	wg.Add(1)
   shutdown := make(chan struct{})
	go worker.Start(q, 1, &wg, ts , shutdown)
    close(shutdown)
	// No more tasks will be added.
	q.Close()

	// Wait for worker to finish.
	wg.Wait()

	// --------------------------------------------------
	// Verify that the recovered task was actually processed.
	// --------------------------------------------------

	recovered, ok := ts.Get(task.ID)
	if !ok {
		t.Fatalf("recovered task was not found in taskstore")
	}

	if recovered.State != queue.Completed {
		t.Fatalf(
			"expected recovered task to be Completed, got %v",
			recovered.State,
		)
	}
}
