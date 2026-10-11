package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/taskstore"
	"github.com/Anshit-Gupta/Tasko/internals/worker"
)

func main(){
    sigChan := make(chan os.Signal,1)
    
	var wg sync.WaitGroup

	q:=queue.NewQueue(10)

	ts := taskstore.NewTaskStore()
	for i:=1;i<=3;i++{
		wg.Add(1)
		go worker.Start(q , i , &wg , ts )  //as ts is already a pointer 
	}
	
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
    newtask3:= queue.Task{
       ID: "3",
	   Type: "send_email",
	   Payload: []byte(`{"to":"anshit@email.com"}`),
   }
    newtask4:= queue.Task{
       ID: "4",
	   Type: "send",
	   Payload: []byte(`{"to":"anshit@email.com"}`),
   }
    ts.Add(newtask)
	q.Enqueue(newtask)
	ts.Add(newtask2)
	q.Enqueue(newtask2)
	ts.Add(newtask3)
	q.Enqueue(newtask3)
	ts.Add(newtask4)
	q.Enqueue(newtask4)


	task4,_ := ts.Get("4")
	fmt.Println(task4)

	signal.Notify(sigChan , os.Interrupt , syscall.SIGTERM)
    
   sig := <- sigChan
    fmt.Println("Received:", sig)
	// got the shutdown signal so stop taking any more task into the queue 

	q.Close()
   
	//wait for all the task to be completed before existing main
	wg.Wait()
  


}