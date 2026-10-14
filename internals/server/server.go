package server

import (
	"net/http"
    "context"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
	"github.com/Anshit-Gupta/Tasko/internals/taskstore"
)

type Server struct {
	httpServer *http.Server
	queue      *queue.Queue
	taskstore  *taskstore.TaskStore
}

//CREATE + CONFIGURE
func NewServer(q *queue.Queue, ts *taskstore.TaskStore) *Server {
	mux := http.NewServeMux() //custom mux
	//register routes

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	s := &Server{
		httpServer: httpServer,
		queue:      q,
		taskstore:  ts,
	}

	mux.HandleFunc("/tasks", s.CreateTaskHandler)
    mux.HandleFunc("/tasks/{id}",s.GetTaskHandler)
	return s
}

//start
func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server)Shutdown(ctx context.Context)error{
	return s.httpServer.Shutdown(ctx)
}
