package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Anshit-Gupta/Tasko/internals/broker"
	"github.com/Anshit-Gupta/Tasko/internals/dispatcher"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/worker"
)

func main() {
	sigChan := make(chan os.Signal, 1)
	shutdown := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	broker := broker.NewBroker()

	if err := broker.Ping(); err != nil {
		fmt.Println("Redis connection failed:", err)
		return
	}

	q := queue.NewQueue(10)

	// dispatcher
	dispatcherDone := make(chan struct{}) // Wait for dispatcher to stop before closing the queue.

	go func() {
		dispatcher.Start(ctx, broker, q)
		close(dispatcherDone)
	}()
	// workers
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go worker.Start(q, i, &wg, broker, shutdown)
	}
	// wait for shutdown
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	sig := <-sigChan
	fmt.Println("Received:", sig)

	cancel()
	<-dispatcherDone

	close(shutdown)
	q.Close()

	wg.Wait()
}
