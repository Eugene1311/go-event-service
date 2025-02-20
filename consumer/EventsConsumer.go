package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go-event-service/model"
	"log"
)

type EventsConsumer struct {
	kafkaReader *kafka.Reader
}

func NewEventsConsumer(kafkaReader *kafka.Reader) EventsConsumer {
	return EventsConsumer{
		kafkaReader: kafkaReader,
	}
}

func (consumer EventsConsumer) Process(eventsChannel chan model.Event) {
	for {
		m, err := consumer.kafkaReader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		var event model.Event
		err = json.Unmarshal(m.Value, &event)
		if err != nil {
			fmt.Println(err.Error())
		}
		log.Printf("message at topic/partition/offset %v/%v/%v: %s = %+v\n", m.Topic, m.Partition, m.Offset, string(m.Key), event)
		eventsChannel <- event
	}
}

func (consumer EventsConsumer) Close() {
	if err := consumer.kafkaReader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
