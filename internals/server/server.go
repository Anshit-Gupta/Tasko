package server

import (
	"context"
	"net/http"

	"github.com/Anshit-Gupta/Tasko/internals/broker"

	"github.com/Anshit-Gupta/Tasko/internals/taskstore"
)

type Server struct {
	httpServer *http.Server
	broker *broker.Broker
	taskstore  *taskstore.TaskStore
}

//CREATE + CONFIGURE
func NewServer(b *broker.Broker, ts *taskstore.TaskStore) *Server {
	mux := http.NewServeMux() //custom mux
	//register routes

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	s := &Server{
		httpServer: httpServer,
		broker:      b,
		taskstore:  ts,
	}

	mux.HandleFunc("/tasks", s.CreateTaskHandler)
    mux.HandleFunc("/tasks/{id}",s.GetTaskHandler)
	mux.HandleFunc("/Alltasks",s.GetAllTaskHandler)
	return s
}

//start
func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server)Shutdown(ctx context.Context)error{
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}