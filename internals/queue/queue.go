package queue

import ()

type TaskState string

const (
	Queued     TaskState = "queued"
	Processing TaskState = "processing"
	Completed  TaskState = "completed"
	Failed     TaskState = "failed"
)

// task one unit of work
type Task struct {
	ID         string
	Type       string
	Payload    []byte
	State      TaskState
	RetryCount int // number of retries already performed
}

type Queue struct {
	tasks chan Task
}

// constructor
func NewQueue(capacity int) *Queue {
	//Queue = the thing managing the tasks
	//tasks = the channel containing many Tasks
	tasks := make(chan Task, capacity)
	return &Queue{
		tasks: tasks,
	}

}

func (q *Queue) Enqueue(task Task) {
	task.State = Queued
	q.tasks <- task
}

func (q *Queue) Dequeue() (Task, bool) {
	task, ok := <-q.tasks

	return task, ok
}

func (q *Queue) Close() {
	close(q.tasks)
}
