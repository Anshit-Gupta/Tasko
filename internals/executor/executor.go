package executor

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Anshit-Gupta/Tasko/internals/queue"
)

type EmailPayload struct {
	To string `json:"to"`
}

type ImagePayload struct {
	Image string `json:"image"`
}

func Execute(t *queue.Task) error {

	switch t.Type {
	case "send_email":
		return SendMail(t)
	case "resize_image":
		return ResizeImage(t)
	default:
		return fmt.Errorf("unknown task type :%s", t.Type)
	}
}

func SendMail(t *queue.Task) error {
  
	var newPayload EmailPayload
	err := json.Unmarshal(t.Payload, &newPayload)
	if err != nil {
		return err
	}

	email := newPayload.To

	fmt.Printf("sendind mail to %v", email)
	fmt.Println("Email sent")

	return nil

}

func ResizeImage(t *queue.Task) error {
	var newPayload ImagePayload
	err := json.Unmarshal(t.Payload, &newPayload)
	if err != nil {
		fmt.Println("error:", err)
		return err
	}

	image := newPayload.Image

	fmt.Printf("Resizing the %v image ", image)
	time.Sleep(time.Second)
	fmt.Println("Image resized!")

	return nil
}
