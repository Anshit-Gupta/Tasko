package broker

import (
	"context"
	"encoding/json"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/redis/go-redis/v9" //go library to communicate with redis
	"time"
)

type Broker struct {
	client *redis.Client
}

type TaskUpdate struct {
	Task queue.Task `json:"task"`
}

//constructor

func NewBroker() *Broker {
	client := redis.NewClient(&redis.Options{ // creates our Go-side Redis client
		Addr: "localhost:6379",
	})

	return &Broker{
		client: client,
	}
}

func (b *Broker) Ping() error {
	ctx := context.Background()
	_, err := b.client.Ping(ctx).Result()
	return err
}

func (b *Broker) Publish(task queue.Task) error {

	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	ctx := context.Background()
	//push to tail of the list
	_, err = b.client.RPush(ctx, "task_queue", string(data)).Result()
	if err != nil {
		return err
	}

	return nil

}

func (b *Broker) Consume(ctx context.Context) (*queue.Task, error) {

	for {
		result, err := b.client.BLPop(ctx, 1*time.Second, "task_queue").Result()

		if err == redis.Nil {
			// Redis timed out waiting for a task.
			// Check whether our application is shutting down.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			continue
		}

		if err != nil {
			return nil, err
		}

		var task queue.Task
		err = json.Unmarshal([]byte(result[1]), &task)
		if err != nil {
			return nil, err
		}

		return &task, nil
	}
}

func (b *Broker) PublishUpdate(task queue.Task) error {
	update := TaskUpdate{
		Task: task,
	}

	data, err := json.Marshal(update)
	if err != nil {
		return err
	}

	ctx := context.Background()

	_, err = b.client.RPush(
		ctx,
		"task_updates",
		string(data),
	).Result()

	return err
}

func (b *Broker) ConsumeUpdate(ctx context.Context) (*TaskUpdate, error) {
	for {
		result, err := b.client.BLPop(
			ctx,
			1*time.Second,
			"task_updates",
		).Result()

		if err != nil {
			if err == redis.Nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				continue
			}

			return nil, err
		}

		var update TaskUpdate

		err = json.Unmarshal([]byte(result[1]), &update)
		if err != nil {
			return nil, err
		}

		return &update, nil
	}
}
