package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/server"
	"github.com/Anshit-Gupta/Tasko/internals/taskstore"
	"github.com/Anshit-Gupta/Tasko/internals/worker"
)

func main() {
	sigChan := make(chan os.Signal, 1)

	var wg sync.WaitGroup

	q := queue.NewQueue(10)

	ts, err := taskstore.NewTaskStore("data/Tasko.log")
	if err != nil {
		log.Fatal("error opening the file", err)
	}

	//http server

	srv := server.NewServer(q, ts)

	go func() {
		if err = srv.Start(); err != nil {
			log.Panicln("HTTP server stopped:", err)
		}
	}()

	queuedTasks := ts.QueuedTasks()

	for _, t := range queuedTasks {
		q.Enqueue(t)
	}

	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go worker.Start(q, i, &wg, ts) //as ts is already a pointer
	}

	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	fmt.Println("Received:", sig)
	// got the shutdown signal so stop taking any more task into the queue

	q.Close()

	//wait for all the task to be completed before existing main
	wg.Wait()
	ts.Close()

}
