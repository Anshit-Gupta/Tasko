package dispatcher

import (
	"context"
	"fmt"
	"github.com/Anshit-Gupta/Tasko/internals/broker"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
)

func Start(ctx context.Context, broker *broker.Broker, q *queue.Queue) {
	for {
		task, err := broker.Consume(ctx)

		if err != nil {
			fmt.Println("dispatcher stopped:", err)
			return
		}

		q.Enqueue(*task)
	}
}
