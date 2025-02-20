package main

import (
	"github.com/segmentio/kafka-go"
	"go-event-service/consumer"
	"go-event-service/model"
	"log"
)

const (
	Broker        = "localhost:19092"
	ConsumerGroup = "go-event-service"
	Topic         = "events"
)

func main() {
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{Broker},
		GroupID: ConsumerGroup,
		Topic:   Topic,
		//MaxBytes: 10e6, // 10MB
	})
	eventsConsumer := consumer.NewEventsConsumer(kafkaReader)
	defer eventsConsumer.Close()

	eventsChannel := make(chan model.Event)
	go eventsConsumer.Process(eventsChannel)

	for {
		select {
		case event := <-eventsChannel:
			log.Printf("Recieved event: %+v", event)
		}
	}
}
