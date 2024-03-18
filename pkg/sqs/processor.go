package sqs

import (
	"fmt"
	"time"
)

// Processor is a processor for SQS messages
type Processor struct {
	QueueURL string
	Router   *MessageChannelRouter
}

// NewProcessor creates a new processor
func NewProcessor(queueURL string, parallelConsumerCount int) *Processor {
	return &Processor{
		QueueURL: queueURL,
		Router:   NewMessageChannelRouter(parallelConsumerCount),
	}
}

// Start starts the processor
func (p *Processor) Start() {
	p.Router.Start()

	// Start the SQS consumer
	go p.consumeMessages()
}

// consumeMessages consumes messages from the SQS queue
func (p *Processor) consumeMessages() {
	// Consume messages from the SQS queue
	// and send them to the message channel router

	// For example:
	// for {
	// 	message := receiveMessage(p.QueueURL)
	// 	p.Router.IncomingMessageChannel <- message
	// }

	// For the sake of this example, we will simulate
	// the consumption of messages by sending a message
	// to the message channel router every second
	for {
		message := &Message{Body: fmt.Sprintf("Hello, World! random: %d", time.Now().Unix())}
		p.Router.IncomingMessageChannel <- message
		// Sleep for 1 second
		time.Sleep(1 * time.Second)
	}
}

// Stop stops the processor
func (p *Processor) Stop() {
	p.Router.Stop()
}
