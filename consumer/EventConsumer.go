package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go-event-service/interfaces"
	"go-event-service/model"
	"log"
)

type EventConsumer struct {
	kafkaReader  *kafka.Reader
	eventService interfaces.EventService
}

func NewEventsConsumer(kafkaReader *kafka.Reader, eventService interfaces.EventService) EventConsumer {
	return EventConsumer{
		kafkaReader:  kafkaReader,
		eventService: eventService,
	}
}

func (consumer EventConsumer) Process() {
	for {
		ctx := context.Background()
		message, err := consumer.kafkaReader.FetchMessage(ctx)
		if err != nil {
			log.Printf(err.Error())
			break
		}
		var event model.Event
		err = json.Unmarshal(message.Value, &event)
		if err != nil {
			fmt.Println(err.Error())
			break
		}
		log.Printf("Message at topic/partition/offset %v/%v/%v: %s = %+v\n", message.Topic, message.Partition, message.Offset, string(message.Key), event)

		_, err = consumer.eventService.Save(event)
		if err != nil {
			break
		}
		err = consumer.kafkaReader.CommitMessages(ctx, message)
		if err != nil {
			log.Printf("Error while commiting message, %s", err.Error())
		}
	}
}

func (consumer EventConsumer) Close() {
	if err := consumer.kafkaReader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
