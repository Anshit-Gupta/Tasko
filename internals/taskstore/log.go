package taskstore

import (
	"bufio"
	 "fmt"
	"os"
	"encoding/json"

	"github.com/Anshit-Gupta/Tasko/internals/queue"
)

type logEntry struct{
	Op string
	Task queue.Task 
}

func (s *TaskStore)logReplayHelper(entry logEntry) error {
	if entry.Op =="Add"{
		s.records[entry.Task.ID]=entry.Task
	}else if entry.Op == "Update"{
		s.records[entry.Task.ID]=entry.Task
	}else{
		return fmt.Errorf("unknown operation detected %q", entry.Op)
	}

	return nil
}


func( s *TaskStore)logReplay(path string)error{
	//open file in read 
	file,err := os.Open(path)

	if err!=nil{
		return err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	if !scanner.Scan() {
		return nil
	}

	currentLine := scanner.Text()

	for{
		if scanner.Scan() {//returns a bool , we check if the next line exist 
		var entry logEntry  

		err:= json.Unmarshal([]byte(currentLine),&entry)

		if err != nil {
				fmt.Printf("error in json->go: %q\n", currentLine)
				return err
			}

		err=s.logReplayHelper(entry)	
		if err != nil {
				return fmt.Errorf("error doing SET/DELETE operation: %w", err)
		}

      currentLine = scanner.Text() //move the current to next

		}else{
			//here we know that the currentLine is the last one 
			 var entry logEntry
			 if err := json.Unmarshal([]byte(currentLine), &entry);err!=nil{
				fmt.Printf("error in final json: %q\n", currentLine)
				break
			 }

			err = s.logReplayHelper(entry) 
			if err != nil {
				return fmt.Errorf("error doing SET/DELETE operation: %w", err)
			}
			break


		}


	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}





