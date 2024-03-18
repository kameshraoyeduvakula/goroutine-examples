package sqs

import (
	"fmt"
	"hash/fnv"
)

// Stop is a message to stop the consumer
type Stop struct{}

// MessageChannelRouter is a router for messages to be processed by multiple consumers
type MessageChannelRouter struct {
	parallelConsumerCount  int
	IncomingMessageChannel chan *Message
	MessageChannelMap      []chan *Message
	StopChannelMap         []chan Stop
	ChannelConsumerMap     []*MessageChannelConsumer
}

// NewMessageChannelRouter creates a new message channel router
func NewMessageChannelRouter(parallelConsumerCount int) *MessageChannelRouter {
	messageChannelMap := make([]chan *Message, parallelConsumerCount)
	stopChannelMap := make([]chan Stop, parallelConsumerCount)
	channelConsumerMap := make([]*MessageChannelConsumer, parallelConsumerCount)
	for i := 0; i < parallelConsumerCount; i++ {
		messageChannelMap[i] = make(chan *Message)
		stopChannelMap[i] = make(chan Stop)
		channelConsumerMap[i] = NewMessageChannelConsumer(messageChannelMap[i], stopChannelMap[i])
	}

	return &MessageChannelRouter{
		MessageChannelMap:      messageChannelMap,
		StopChannelMap:         stopChannelMap,
		ChannelConsumerMap:     channelConsumerMap,
		parallelConsumerCount:  parallelConsumerCount,
		IncomingMessageChannel: make(chan *Message),
	}
}

// Start starts the message channel router
func (mcr *MessageChannelRouter) Start() {
	for _, channelConsumer := range mcr.ChannelConsumerMap {
		go channelConsumer.Run()
	}

	go mcr.Run()
}

// Run runs the message channel router
func (mcr *MessageChannelRouter) Run() {
	defer mcr.recoverFromPanic()

	for {
		select {
		case message := <-mcr.IncomingMessageChannel:
			{
				hash := mcr.hash(message.Body)
				mcr.MessageChannelMap[hash] <- message
			}
		}
	}
}

// Stop stops the message channel router
func (mcr *MessageChannelRouter) Stop() {
	for i := 0; i < mcr.parallelConsumerCount; i++ {
		mcr.StopChannelMap[i] <- Stop{}
	}
}

// hash hashes the string to return the index of the message channel
func (mcr *MessageChannelRouter) hash(s string) uint32 {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0
	}
	return h.Sum32() % uint32(mcr.parallelConsumerCount)
}

// recoverFromPanic recovers the message channel router from panic
func (mcr *MessageChannelRouter) recoverFromPanic() {
	if r := recover(); r != nil {
		fmt.Printf("Recovered from panic: %v", r)
		// Restart the router
		go mcr.Run()
	}
}
