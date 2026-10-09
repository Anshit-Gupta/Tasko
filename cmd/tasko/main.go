package main

import (
	
	"os"
	"os/signal"
	"syscall"
    "fmt"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/worker"
	"sync"
)

func main(){
    sigChan := make(chan os.Signal,1)
    
	var wg sync.WaitGroup

	q:=queue.NewQueue(10)
	for i:=1;i<=3;i++{
		wg.Add(1)
		go worker.Start(q , i , &wg)  
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
	   Type: "send_email",
	   Payload: []byte(`{"to":"anshit@email.com"}`),
   }
	q.Enqueue(newtask)
	q.Enqueue(newtask2)
	q.Enqueue(newtask3)
	q.Enqueue(newtask4)

	signal.Notify(sigChan , os.Interrupt , syscall.SIGTERM)
    
   sig := <- sigChan
    fmt.Println("Received:", sig)
	// got the shutdown signal so stop taking any more task into the queue 

	q.Close()
   
	//wait for all the task to be completed before existing main
	wg.Wait()
  


}