package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/google/uuid"
)

type CreateTaskRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"` // preserve payload as raw JSON
}

type CreateTaskResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type GetTaskResponse struct{
	ID string `json:"id"`
	Type string `json:"type"`
	Status queue.TaskState `json:"status"`
	RetryCount int `json:"retryCount"`
}



func (s *Server) CreateTaskHandler(w http.ResponseWriter, r *http.Request) {
    
	if r.Method != http.MethodPost {
    http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
    return
   }

	// we  recived task as json
	var newTask CreateTaskRequest
	err := json.NewDecoder(r.Body).Decode(&newTask)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	//validating the input
	if len(newTask.Payload) == 0 || newTask.Type == "" {
		http.Error(w, "type and payload are required", http.StatusBadRequest)
		return
	}

	task := queue.Task{
		ID:      uuid.New().String(),
		Type:    newTask.Type,
		Payload: newTask.Payload,
	}

	err = s.taskstore.Add(task)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.queue.Enqueue(task)

	//restus status
	response := CreateTaskResponse{ID: task.ID, Status: "queued"}
	//set header data
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func (s *Server)GetTaskHandler(w http.ResponseWriter , r *http.Request){

	if r.Method != http.MethodGet {
    http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
    return 
    }
	

	//we will get the id from the url 
    taskId := strings.TrimPrefix(r.URL.Path,"/tasks/")
	if taskId==""{
		http.Error(w,"task id  cannot be empty",http.StatusBadRequest)
		return
	}
    

	task , exists :=s.taskstore.Get(taskId)

	if !exists{
		http.Error(w,"task not found  ",http.StatusNotFound)
		return
	}

	response := GetTaskResponse{ID:task.ID , Type: task.Type ,Status: task.State , RetryCount: task.RetryCount  }
    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}



