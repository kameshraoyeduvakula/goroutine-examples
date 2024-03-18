package main

import (
	"goroutine-examples/pkg/sqs"
	"os"
)

func main() {
	// SQS Processor
	// Create a new processor
	// processor := sqs.NewProcessor("https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue", 5)
	// Start the processor
	// processor.Start()
	// Stop the processor
	// processor.Stop()

	processor := sqs.NewProcessor("https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue", 5)
	processor.Start()

	// Handle ctrl-c signal
	signalChan := make(chan os.Signal, 1)
	<-signalChan
}
