package server

import (
	"encoding/json"
	"net/http"

	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/google/uuid"
)

type CreateTaskRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"` //accepts arbitrary json as []byte
}

type CreateTaskResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func (s *Server) CreateTaskHandler(w http.ResponseWriter, r *http.Request) {

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
