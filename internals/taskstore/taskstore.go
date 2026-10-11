package taskstore

//keep a record of every task and it's current state
import (
	"fmt"
	"sync"

	"github.com/Anshit-Gupta/Tasko/internals/queue"
)

//remembers Tasks and their states


type TaskStore struct{
	records map[string]queue.Task
	mu sync.RWMutex
}



//constructor 

func NewTaskStore()*TaskStore{
	return &TaskStore{
		records: make(map[string]queue.Task),
	}
}


func (s * TaskStore)Add(t queue.Task){
	s.mu.Lock()
	defer s.mu.Unlock()

	t.State = queue.Queued 
	s.records[t.ID]=t
}


func (s *TaskStore)Update(t queue.Task , state queue.TaskState)error{
	t.State = state
	s.mu.Lock()
	defer s.mu.Unlock()

   if _,exists:= s.records[t.ID] ; !exists{
       return fmt.Errorf("task %s does not exist", t.ID)
   }

	s.records[t.ID]=t
	return  nil
}


func (s *TaskStore)Get( taskId string) (*queue.Task , error){
       
	s.mu.RLock()
	defer s.mu.RUnlock()
     
    task,exists := s.records[taskId];
   if !exists{
       return nil,fmt.Errorf("id not found")
   }

   return &task,nil  

}