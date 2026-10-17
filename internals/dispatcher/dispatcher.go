package dispatcher

import (
	"context"

	"github.com/Anshit-Gupta/Tasko/internals/broker"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
)

func Start(ctx context.Context , broker *broker.Broker , q *queue.Queue){
	for{
		task,err := broker.Consume(ctx)

		if err!=nil{
          return 
		}

		q.Enqueue(*task)
	}
}