package queue

import (
	
)

// task one unit of work 
type Task struct{
	ID string
	Type string
	Payload []byte
}

type Queue struct{
	tasks chan Task 
}

//constructor 
func NewQueue(capacity int )*Queue{
	//Queue = the thing managing the tasks 
    //tasks = the channel containing many Tasks
	 tasks:=make(chan Task ,capacity)
	 return  &Queue{
		tasks: tasks,
	}
     
}

func (q *Queue)Enqueue(task Task){
	q.tasks <- task
}

func (q *Queue)Dequeue()Task{
	return <-q.tasks
}