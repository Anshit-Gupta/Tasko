package taskstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Anshit-Gupta/Tasko/internals/queue"
)

func TestPersistence(t *testing.T) {
	// Testing if data survives when store is closed.

	dir := t.TempDir()
	path := filepath.Join(dir, "TestPersistence.log")

	ts, err := NewTaskStore(path)
	if err != nil {
		t.Fatalf("Error creating new Taskstore: %v", err)
	}

	newtask := queue.Task{
		ID:      "1",
		Type:    "send_email",
		Payload: []byte(`{"to":"anshit@email.com"}`),
	}

	err = ts.Add(newtask)
	if err != nil {
		t.Fatalf("Error Adding to taskstore: %v", err)
	}

	err = ts.Update(newtask, queue.Processing)
	if err != nil {
		t.Fatalf("Error Updating taskstore: %v", err)
	}

	// Close the store.
	if err := ts.Close(); err != nil {
		t.Fatalf("Failed to close the taskstore: %v", err)
	}

	// Reopen the same path and check if data persisted.
	ts, err = NewTaskStore(path)
	if err != nil {
		t.Fatalf("Error creating new Taskstore: %v", err)
	}
	defer ts.Close()

	task, exists := ts.Get(newtask.ID)
	if !exists {
		t.Fatalf("Error Getting task: %v", err)
	}

	if task.ID != newtask.ID {
		t.Fatalf("Expected value: %s got: %s", newtask.ID, task.ID)
	}

	if task.Type != newtask.Type {
		t.Fatalf("Expected value: %s got: %s", newtask.Type, task.Type)
	}

	if string(task.Payload) != string(newtask.Payload) {
		t.Fatalf(
			"Expected value: %s got: %s",
			string(newtask.Payload),
			string(task.Payload),
		)
	}

	if task.RetryCount != newtask.RetryCount {
		t.Fatalf(
			"Expected value: %d got: %d",
			newtask.RetryCount,
			task.RetryCount,
		)
	}

	// Processing tasks are recovered as Queued.
	if task.State != queue.Queued {
		t.Fatalf("expected state %q, got %q", queue.Queued, task.State)
	}
}

func TestGetMissingKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MissingKey.log")

	ts, err := NewTaskStore(path)
	if err != nil {
		t.Fatalf("Error creating new TaskStore: %v", err)
	}
	defer ts.Close()

	task, ok := ts.Get("1234")

	if ok {
		t.Fatalf("expected task to not exist, got: %+v", task)
	}
}

func TestReplayMultipleTasks(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "multiple.log")

	ts, err := NewTaskStore(logPath)
	if err != nil {
		t.Fatalf("failed to create taskstore: %v", err)
	}

	task1 := queue.Task{
		ID:      "1",
		Type:    "send_email",
		Payload: []byte(`{"to":"a@email.com"}`),
	}

	task2 := queue.Task{
		ID:      "2",
		Type:    "resize_image",
		Payload: []byte(`{"image":"photo.jpg"}`),
	}

	task3 := queue.Task{
		ID:      "3",
		Type:    "send_email",
		Payload: []byte(`{"to":"b@email.com"}`),
	}

	if err := ts.Add(task1); err != nil {
		t.Fatal(err)
	}

	if err := ts.Add(task2); err != nil {
		t.Fatal(err)
	}

	if err := ts.Add(task3); err != nil {
		t.Fatal(err)
	}

	if err := ts.Update(task1, queue.Processing); err != nil {
		t.Fatal(err)
	}

	if err := ts.Update(task2, queue.Failed); err != nil {
		t.Fatal(err)
	}

	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and replay the log.
	ts, err = NewTaskStore(logPath)
	if err != nil {
		t.Fatalf("failed to recover taskstore: %v", err)
	}
	defer ts.Close()

	got1, ok := ts.Get("1")
	if !ok {
		t.Fatal("task 1 was not recovered")
	}

	got2, ok := ts.Get("2")
	if !ok {
		t.Fatal("task 2 was not recovered")
	}

	got3, ok := ts.Get("3")
	if !ok {
		t.Fatal("task 3 was not recovered")
	}

	// Processing → Queued after recovery.
	if got1.State != queue.Queued {
		t.Fatalf("task 1: expected Queued, got %v", got1.State)
	}

	// Failed is terminal and should remain Failed.
	if got2.State != queue.Failed {
		t.Fatalf("task 2: expected Failed, got %v", got2.State)
	}

	// Queued remains Queued.
	if got3.State != queue.Queued {
		t.Fatalf("task 3: expected Queued, got %v", got3.State)
	}
}

func TestCorruptMiddleLog(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "corrupt-middle.log")

	ts, err := NewTaskStore(logPath)
	if err != nil {
		t.Fatal(err)
	}

	task := queue.Task{
		ID:      "1",
		Type:    "send_email",
		Payload: []byte(`{"to":"a@email.com"}`),
	}

	if err := ts.Add(task); err != nil {
		t.Fatal(err)
	}

	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a corrupted record in the middle.
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.WriteString(`this is not valid json` + "\n"); err != nil {
		t.Fatal(err)
	}

	// Add another valid record after the corruption.
	if _, err := file.WriteString(
		`{"Op":"Update","Task":{"ID":"1","Type":"send_email","Payload":"eyJ0byI6ImFAZW1haWwuY29tIn0=","State":"completed","RetryCount":0}}` + "\n",
	); err != nil {
		t.Fatal(err)
	}

	file.Close()

	// Middle corruption should cause recovery to fail.
	_, err = NewTaskStore(logPath)

	if err == nil {
		t.Fatal("expected recovery to fail because of corrupted middle log entry")
	}
}

func TestCorruptFinalLogEntry(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "corrupt-final.log")

	ts, err := NewTaskStore(logPath)
	if err != nil {
		t.Fatal(err)
	}

	task := queue.Task{
		ID:      "1",
		Type:    "send_email",
		Payload: []byte(`{"to":"a@email.com"}`),
	}

	if err := ts.Add(task); err != nil {
		t.Fatal(err)
	}

	if err := ts.Update(task, queue.Processing); err != nil {
		t.Fatal(err)
	}

	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate an incomplete final write.
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.WriteString(`{"Op":"Update","Task":`); err != nil {
		t.Fatal(err)
	}

	file.Close()

	// Final corrupted record should be ignored.
	ts, err = NewTaskStore(logPath)
	if err != nil {
		t.Fatalf("expected recovery to succeed, got: %v", err)
	}
	defer ts.Close()

	got, ok := ts.Get(task.ID)
	if !ok {
		t.Fatal("task was not recovered")
	}

	// The last valid record was Processing,
	// which is recovered as Queued.
	if got.State != queue.Queued {
		t.Fatalf("expected Queued, got %v", got.State)
	}
}

func TestQueuedTasksAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "recovery.log")

	ts, err := NewTaskStore(logPath)
	if err != nil {
		t.Fatalf("failed to create taskstore: %v", err)
	}

	task := queue.Task{
		ID:      "1",
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

	// Simulate restart.
	ts, err = NewTaskStore(logPath)
	if err != nil {
		t.Fatalf("failed to recover taskstore: %v", err)
	}
	defer ts.Close()

	tasks := ts.QueuedTasks()

	if len(tasks) != 1 {
		t.Fatalf("expected 1 queued task, got %d", len(tasks))
	}

	if tasks[0].ID != task.ID {
		t.Fatalf("expected task %s, got %s", task.ID, tasks[0].ID)
	}

	if tasks[0].State != queue.Queued {
		t.Fatalf("expected task to be Queued, got %v", tasks[0].State)
	}
}
