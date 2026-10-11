package worker

import (
	"fmt"
	"sync"

	"github.com/Anshit-Gupta/Tasko/internals/executor"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/taskstore"
)

func Start(q *queue.Queue, workerID int, wg *sync.WaitGroup , ts *taskstore.TaskStore) {

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
           if task.RetryCount <3 {
			task.RetryCount++
			ts.Update(task,queue.Queued)
			q.Enqueue(task)
           fmt.Println("retrying")
			continue  // or else it will go to completed 
		   }else{
            
            fmt.Println("task failed:", task.ID, err)
			
			ts.Update(task, queue.Failed) 
			continue
		   }


			
		}

		fmt.Println("task completed:", task.ID)
		
		ts.Update(task, queue.Completed) 

	}

}
