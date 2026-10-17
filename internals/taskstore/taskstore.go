package taskstore

//keep a record of every task and it's current state
import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
    "path/filepath"
	"github.com/Anshit-Gupta/Tasko/internals/queue"
)

//remembers Tasks and their states

type TaskStore struct {
	records map[string]queue.Task
	mu      sync.RWMutex

	logFile *os.File
}

//constructor

func NewTaskStore(path string) (*TaskStore, error) {
    dir := filepath.Dir(path)

    if err := os.MkdirAll(dir, 0755); err != nil {
        return nil, err
    }
    file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	
	ts := &TaskStore{
		logFile: file,
		records: make(map[string]queue.Task),
	}

	//Replay the log into the map
	if err := ts.logReplay(path); err != nil {
		file.Close() //since relay failed we close the file before returning err
		return nil, err
	}

	return ts, nil

}

// GET
func (s *TaskStore) Get(taskId string) (*queue.Task, bool) {

	s.mu.RLock()
	defer s.mu.RUnlock()

	task, exists := s.records[taskId]
	if !exists {
		return nil, false
	}

	return &task, true

}

// POST
func (s *TaskStore) Add(t queue.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t.State = queue.Queued

	newlog := logEntry{Op: "Add", Task: t}
	newjson, err := json.Marshal(newlog) //converting struct to json

	if err != nil {

		return err
	}

	newjson = append(newjson, '\n')

	//mow we append the newjson into the file
	byteCount, err := s.logFile.Write(newjson)
	if err != nil {

		return err
	}

	//chances are very low but it is possible that short write than requested so we cross check :
	if byteCount != len(newjson) {

		return fmt.Errorf("short write")
	}

	//sync before updating the map

	err = s.logFile.Sync()

	if err != nil {

		return fmt.Errorf("error in sync: %w", err)
	}

	//now that the log is updated we can finnaly update the map

	s.records[t.ID] = t

	return nil
}

// put
func (s *TaskStore) Update(t queue.Task, state queue.TaskState) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	//first we confim that task exits

	if _, exists := s.records[t.ID]; !exists {
		return fmt.Errorf("task %s does not exist", t.ID)
	}

	t.State = state

	newlog := logEntry{Op: "Update", Task: t}
	newjson, err := json.Marshal(newlog)

	if err != nil {

		return err
	}

	newjson = append(newjson, '\n')

	//mow we append the newjson into the file
	byteCount, err := s.logFile.Write(newjson)
	if err != nil {

		return err
	}

	//chances are very low but it is possible that short write than requested so we cross check :
	if byteCount != len(newjson) {

		return fmt.Errorf("short write")
	}

	//sync before updating the map

	err = s.logFile.Sync()

	if err != nil {

		return fmt.Errorf("error in sync: %w", err)
	}

	s.records[t.ID] = t
	return nil
}

//close

func (s *TaskStore) Close() error {
	return s.logFile.Close()
}

func (s *TaskStore) QueuedTasks() []queue.Task {

	s.mu.RLock()
	defer s.mu.RUnlock()
	var tasks []queue.Task

	for _, task := range s.records {
		if task.State == queue.Queued {
			tasks = append(tasks, task)
		}
	}

	return tasks

}

//for tasks list endpoint 
func (s *TaskStore) Tasks() []queue.Task {
    s.mu.RLock()
    defer s.mu.RUnlock()

    var tasks []queue.Task

    for _, task := range s.records {
        tasks = append(tasks, task)
    }

    return tasks
}