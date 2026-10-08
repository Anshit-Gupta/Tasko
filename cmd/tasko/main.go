package main

import (
	"time"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/worker"
)

func main(){

	q:=queue.NewQueue(10)
	go worker.Start(q)
   newtask:= queue.Task{
       ID: "1",
	   Type: "send_email",
	   Payload: []byte(`{"to":"anshit@email.com"}`),
   }
   newtask2:= queue.Task{
       ID: "2",
	   Type: "send_email",
	   Payload: []byte(`{"to":"anshit@email.com"}`),
   }
	q.Enqueue(newtask)
	q.Enqueue(newtask2)
    
	
   time.Sleep(time.Second)


}