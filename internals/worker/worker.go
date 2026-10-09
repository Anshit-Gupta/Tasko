package worker

import (
	"fmt"
	"sync"

	"github.com/Anshit-Gupta/Tasko/internals/executor"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
)

func Start(q *queue.Queue , workerID int , wg *sync.WaitGroup){
     defer wg.Done()
	for{
		task,ok := q.Dequeue()

		if !ok{ //ok == false means the channel is closed and there are no more values to receive.
           return 
		}
     
		err := executor.Execute(&task)

		if err != nil {
    fmt.Println("task failed:", task.ID, err)
    continue
}

fmt.Println("task completed:", task.ID)
		
	}
	
}
