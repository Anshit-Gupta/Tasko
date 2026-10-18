package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/Anshit-Gupta/Tasko/internals/broker"
	"github.com/Anshit-Gupta/Tasko/internals/executor"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
)

func Start(
	q *queue.Queue,
	workerID int,
	wg *sync.WaitGroup,
	b *broker.Broker,
	shutdown <-chan struct{},
) {
	defer wg.Done()

	for {
		task, ok := q.Dequeue()

		if !ok {
			return
		}

		// Task started processing
		task.State = queue.Processing
		if err := b.PublishUpdate(task); err != nil {
			fmt.Println("failed to publish processing update:", err)
		}

		err := executor.Execute(&task)

		if err != nil {
			// Retry
			if task.RetryCount < 3 {
				task.LastError = err.Error()
				task.RetryCount++
				task.State = queue.Queued

				if err := b.PublishUpdate(task); err != nil {
					fmt.Println("failed to publish retry update:", err)
				}

				wg.Add(1)
				go retryScheduler(task, q, wg, shutdown)

				continue
			}

			// Permanently failed
			fmt.Println("task failed:", task.ID, err)

			task.LastError = err.Error()
			task.State = queue.Failed

			if err := b.PublishUpdate(task); err != nil {
				fmt.Println("failed to publish failed update:", err)
			}

			continue
		}

		// Completed
		fmt.Println("task completed:", task.ID)

		task.State = queue.Completed

		if err := b.PublishUpdate(task); err != nil {
			fmt.Println("failed to publish completed update:", err)
		}
	}
}

func retryScheduler(
	task queue.Task,
	q *queue.Queue,
	wg *sync.WaitGroup,
	shutdown <-chan struct{},
) {
	defer wg.Done()

	delay := 2 * (1 << (task.RetryCount - 1))

	timer := time.NewTimer(time.Duration(delay) * time.Second)
	defer timer.Stop()

	select {
	case <-timer.C:
		q.Enqueue(task)
		fmt.Println("retrying")

	case <-shutdown:
		return
	}
}
