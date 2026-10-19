package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Anshit-Gupta/Tasko/internals/broker"
	"github.com/Anshit-Gupta/Tasko/internals/metrics"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/server"
	"github.com/Anshit-Gupta/Tasko/internals/taskstore"
)

func main() {
	sigChan := make(chan os.Signal, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	broker := broker.NewBroker(redisAddr)

	if err := broker.Ping(); err != nil {
		log.Fatal("Redis connection failed:", err)
	}

	ts, err := taskstore.NewTaskStore("data/Tasko.log")
	if err != nil {
		log.Fatal("error opening the file:", err)
	}
	defer ts.Close()

	m := metrics.NewMetrics()

	srv := server.NewServer(broker, ts, m)

	// Consume task updates from Redis
	updateDone := make(chan struct{})

	go func() {
		defer close(updateDone)

		for {
			update, err := broker.ConsumeUpdate(ctx)

			if err != nil {
				return
			}

			task := update.Task
			//update metrics
			switch task.State {
			case queue.Processing:
				if task.RetryCount == 0 {
					m.TaskReceived()
				}

			case queue.Queued:
				if task.RetryCount > 0 {
					m.TaskRetried()
				}

			case queue.Completed:
				m.TaskCompleted()

			case queue.Failed:
				m.TaskFailed()
			}

			if err := ts.Update(task, task.State); err != nil {
				fmt.Println("failed to update task:", err)
			}
		}
	}()

	// HTTP server
	go func() {
		if err := srv.Start(); err != nil {
			log.Println("HTTP server stopped:", err)
		}
	}()

	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	fmt.Println("Received:", sig)

	shutdownCtx, cancelShutdown := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancelShutdown()

	// Stop HTTP server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		fmt.Println("Server shutdown error:", err)
	}

	// Stop Redis update consumer
	cancel()
	<-updateDone

	fmt.Println("Server shutdown complete")
}
