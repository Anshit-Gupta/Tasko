package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/Anshit-Gupta/Tasko/internals/executor"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/taskstore"
)

func Start(q *queue.Queue, workerID int, wg *sync.WaitGroup, ts *taskstore.TaskStore, shutdown <-chan struct{}) {

	defer wg.Done()

	for {
		task, ok := q.Dequeue()

		if !ok { //ok == false means the channel is closed and there are no more values to receive.
			return
		}

		ts.Update(task, queue.Processing)

		err := executor.Execute(&task)

		if err != nil {
			//retry
			if task.RetryCount < 3 {
				task.LastError = err.Error() //update to most recent faliure
				task.RetryCount++
				ts.Update(task, queue.Queued)
				wg.Add(1)
				go retryScheduler(task, q, wg, shutdown)
				continue // or else it will go to completed
			} else {

				fmt.Println("task failed:", task.ID, err)
				task.LastError = err.Error()
				ts.Update(task, queue.Failed)
				continue
			}

		}

		fmt.Println("task completed:", task.ID)

		ts.Update(task, queue.Completed)

	}

}

func retryScheduler(task queue.Task, q *queue.Queue, wg *sync.WaitGroup, shutdown <-chan struct{}) {
	defer wg.Done()
	delay := 2 * (1 << (task.RetryCount - 1)) //first try - 2sec , second try - 4sec , then 8
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
