package worker

import (
	"fmt"

	"github.com/Anshit-Gupta/Tasko/internals/queue"
)

func Start(q *queue.Queue){
    
	for{
		task := q.Dequeue()

		fmt.Println("processing task:",task.ID)
	}
	
}
