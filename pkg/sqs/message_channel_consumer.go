package sqs

import (
	"fmt"
)

// MessageChannelConsumer is a consumer for a message channel
type MessageChannelConsumer struct {
	MessageChannel <-chan *Message
	StopChannel    <-chan Stop
}

// NewMessageChannelConsumer creates a new message channel consumer
func NewMessageChannelConsumer(messageChannel <-chan *Message, stopChannel <-chan Stop) *MessageChannelConsumer {
	return &MessageChannelConsumer{
		MessageChannel: messageChannel,
		StopChannel:    stopChannel,
	}
}

// Run runs the message channel consumer
func (mcc *MessageChannelConsumer) Run() {
	defer mcc.recoverFromPanic()

	for {
		select {
		case message := <-mcc.MessageChannel:
			{
				// Process the message
				fmt.Printf("Self: %v", mcc)
				fmt.Printf("Processing message: %s\n", message.Body)
			}
		case <-mcc.StopChannel:
			{
				fmt.Println("Handling the stop signal")
				fmt.Println("Stopping the consumer")
				return
			}
		}
	}
}

func (mcc *MessageChannelConsumer) recoverFromPanic() {
	if r := recover(); r != nil {
		fmt.Printf("Recovered from panic: %v", r)
		// Restart the consumer
		go mcc.Run()
	}
}
