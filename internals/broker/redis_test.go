package broker

import (
	"context"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"reflect"
	"testing"
	"time"
)

func TestPing(t *testing.T) {

	broker := NewBroker("localhost:6379")

	err := broker.Ping()

	if err != nil {
		t.Fatal(err)
	}
}

func TestPublish(t *testing.T) {
	broker := NewBroker("localhost:6379")
	err := broker.Ping()

	if err != nil {
		t.Fatal(err)
	}

	task := queue.Task{
		ID:         "test-1",
		Type:       "send_email",
		Payload:    []byte(`{"to":"test@example.com"}`),
		State:      queue.Queued,
		RetryCount: 0,
		LastError:  "",
	}

	err = broker.Publish(task)
	if err != nil {
		t.Fatal(err)
	}

}

func TestConsume(t *testing.T) {
	broker := NewBroker("localhost:6379")

	err := broker.Ping()
	if err != nil {
		t.Fatal(err)
	}

	// Clear leftovers from previous experiments.
	broker.client.Del(context.Background(), "task_queue")

	task := queue.Task{
		ID:         "test-1",
		Type:       "send_email",
		Payload:    []byte(`{"to":"test@example.com"}`),
		State:      queue.Queued,
		RetryCount: 0,
		LastError:  "",
	}

	err = broker.Publish(task)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	newTask, err := broker.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(*newTask, task) {
		t.Fatal("consumed task does not match published task")
	}
}

func TestConsumeCancellation(t *testing.T) {
	broker := NewBroker("localhost:6379")

	err := broker.Ping()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error)

	go func() {

		_, err := broker.Consume(ctx)

		done <- err
	}()

	// Give Consume a moment to reach BLPOP.
	time.Sleep(100 * time.Millisecond)

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected Consume to return an error after cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not stop after context cancellation")
	}
}
