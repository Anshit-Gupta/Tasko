package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type task struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func main() {
	var wg sync.WaitGroup
	newTask := task{
		Type:    "send_email",
		Payload: json.RawMessage(`{"to":"test@example.com"}`),
	}

	jsonData, err := json.Marshal(newTask)
	if err != nil {
		panic(err)
	}
	
	var success atomic.Int64
	var failed atomic.Int64
	initialCompleted,err:= getCompletedTask()
	if err!=nil{
		panic(err)
	}
	start := time.Now()
	for i := 1; i <= 900; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			res, err := http.Post(
				"http://localhost:8080/tasks",
				"application/json",
				bytes.NewBuffer(jsonData),
			)
			if err != nil {

				fmt.Printf("Request %d failed: %v\n", i, err)
				failed.Add(1)
				return
			}

			success.Add(1)

			res.Body.Close()

		}(i)

	}

	wg.Wait()


	for{
		completed,err:=getCompletedTask()
		if err!=nil{
			fmt.Println("metrics request failed:", err)
        continue
		}
     target := initialCompleted + success.Load()
    if completed >= target {
        break
        }

    time.Sleep(100 * time.Millisecond)
	}

	elapsed := time.Since(start)



	fmt.Printf("\n--- Load Test Results ---\n")
	fmt.Printf("Total requests: 900\n")
	fmt.Printf("Successful: %d\n", success.Load())
	fmt.Printf("Failed: %d\n", failed.Load())
	fmt.Printf("Total time: %v\n", elapsed)
	fmt.Printf("Tasks/sec: %.2f\n", float64(success.Load())/elapsed.Seconds())
}


func getCompletedTask()(int64,error){
	res,err:=http.Get("http://localhost:8080/metrics")

	if err!=nil{
		return 0,err
	}

	defer res.Body.Close()

	var metrics struct{
		TaskCompleted int64 `json:"tasks_completed"`
	}

	if err:=json.NewDecoder(res.Body).Decode(&metrics);err!=nil{
		return 0,err
	}

	return metrics.TaskCompleted,nil
}