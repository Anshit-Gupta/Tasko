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
	mu sync.Mutex
}



//constructor 

func NewTaskStore()*TaskStore{
	return &TaskStore{
		records: make(map[string]queue.Task),
	}
}


func (s * TaskStore)Add(t queue.Task){
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
