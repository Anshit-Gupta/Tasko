package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Anshit-Gupta/Tasko/internals/broker"
	"github.com/Anshit-Gupta/Tasko/internals/dispatcher"
	"github.com/Anshit-Gupta/Tasko/internals/metrics"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/server"
	"github.com/Anshit-Gupta/Tasko/internals/taskstore"
	"github.com/Anshit-Gupta/Tasko/internals/worker"
)

func TestTaskoEndToEnd(t *testing.T) {
	// --------------------------------------------------
	// Setup
	// --------------------------------------------------

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := broker.NewBroker()

	if err := b.Ping(); err != nil {
		t.Fatalf("Redis connection failed: %v", err)
	}

	logPath := filepath.Join(t.TempDir(), "integration.log")

	ts, err := taskstore.NewTaskStore(logPath)
	if err != nil {
		t.Fatalf("failed to create taskstore: %v", err)
	}
	defer ts.Close()

	q := queue.NewQueue(10)
	m := metrics.NewMetrics()
	srv := server.NewServer(b, ts, m)

	// --------------------------------------------------
	// Start dispatcher
	// --------------------------------------------------

	dispatcherDone := make(chan struct{})

	go func() {
		dispatcher.Start(ctx, b, q)
		close(dispatcherDone)
	}()

	// --------------------------------------------------
	// Start worker
	// --------------------------------------------------

	shutdown := make(chan struct{})

	var workerWG sync.WaitGroup

	workerWG.Add(1)

	go worker.Start(
		q,
		1,
		&workerWG,
		b,
		shutdown,
	)

	// --------------------------------------------------
	// Start server update consumer
	// --------------------------------------------------

	updateDone := make(chan struct{})

	go func() {
		defer close(updateDone)

		for {
			update, err := b.ConsumeUpdate(ctx)

			if err != nil {
				return
			}

			task := update.Task

			if err := ts.Update(task, task.State); err != nil {
				t.Errorf(
					"failed to update task %s: %v",
					task.ID,
					err,
				)
			}
		}
	}()

	// --------------------------------------------------
	// Start HTTP test server
	// --------------------------------------------------

	httpServer := httptest.NewServer(srv.Handler())
	defer httpServer.Close()

	// --------------------------------------------------
	// Create task through HTTP API
	// --------------------------------------------------

	requestBody := map[string]interface{}{
		"type": "send_email",
		"payload": map[string]string{
			"to": "integration@test.com",
		},
	}

	body, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	resp, err := http.Post(
		httpServer.URL+"/tasks",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	if resp.StatusCode != http.StatusCreated {
		resp.Body.Close()

		t.Fatalf(
			"expected status %d, got %d",
			http.StatusCreated,
			resp.StatusCode,
		)
	}

	var createResponse struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&createResponse); err != nil {
		resp.Body.Close()

		t.Fatalf(
			"failed to decode create response: %v",
			err,
		)
	}

	resp.Body.Close()

	if createResponse.ID == "" {
		t.Fatal("expected task ID")
	}

	if createResponse.Status != "queued" {
		t.Fatalf(
			"expected initial status queued, got %s",
			createResponse.Status,
		)
	}

	// --------------------------------------------------
	// Wait for task to become completed
	// --------------------------------------------------

	deadline := time.Now().Add(5 * time.Second)

	for {
		task, exists := ts.Get(createResponse.ID)

		if exists && task.State == queue.Completed {
			break
		}

		if time.Now().After(deadline) {
			task, exists := ts.Get(createResponse.ID)

			if !exists {
				t.Fatal("task disappeared from taskstore")
			}

			t.Fatalf(
				"task did not complete, final state: %s",
				task.State,
			)
		}

		time.Sleep(50 * time.Millisecond)
	}

	// --------------------------------------------------
	// Verify GET /tasks/{id}
	// --------------------------------------------------

	resp, err = http.Get(
		httpServer.URL + "/tasks/" + createResponse.ID,
	)
	if err != nil {
		t.Fatalf("failed to get task: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusOK,
			resp.StatusCode,
		)
	}

	var getResponse struct {
		ID         string          `json:"id"`
		Type       string          `json:"type"`
		Status     queue.TaskState `json:"status"`
		RetryCount int             `json:"retryCount"`
		LastError  string          `json:"lastError"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&getResponse); err != nil {
		t.Fatalf(
			"failed to decode GET response: %v",
			err,
		)
	}

	if getResponse.ID != createResponse.ID {
		t.Fatalf(
			"expected ID %s, got %s",
			createResponse.ID,
			getResponse.ID,
		)
	}

	if getResponse.Type != "send_email" {
		t.Fatalf(
			"expected type send_email, got %s",
			getResponse.Type,
		)
	}

	if getResponse.Status != queue.Completed {
		t.Fatalf(
			"expected status completed, got %s",
			getResponse.Status,
		)
	}

	if getResponse.RetryCount != 0 {
		t.Fatalf(
			"expected retry count 0, got %d",
			getResponse.RetryCount,
		)
	}

	// --------------------------------------------------
	// Shutdown
	// --------------------------------------------------

	cancel()

	<-dispatcherDone

	close(shutdown)
	q.Close()

	workerWG.Wait()

	<-updateDone
}
