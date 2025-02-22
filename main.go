package main

import (
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/segmentio/kafka-go"
	"go-event-service/consumer"
	"go-event-service/model"
	"go-event-service/repository"
	"log"
	"os"
)

func main() {
	var config model.Config

	err := cleanenv.ReadConfig("config/config.yml", &config)
	if err != nil {
		log.Fatal(err)
	}

	logger := log.New(os.Stdout, "kafka reader: ", 3)
	errorLogger := log.New(os.Stderr, "kafka reader: ", 3)
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     config.App.Kafka.Brokers,
		GroupID:     config.App.Kafka.ConsumerGroup,
		Topic:       config.App.Kafka.Topic,
		Logger:      logger,
		ErrorLogger: errorLogger,
		//MaxBytes: 10e6, // 10MB
	})
	eventsConsumer := consumer.NewEventsConsumer(kafkaReader)
	defer eventsConsumer.Close()

	eventsChannel := make(chan model.Event)
	go eventsConsumer.Process(eventsChannel)

	//cfg := elasticsearch.Config{
	//	Addresses:         config.App.Elastic.Addresses,
	//	Username:          config.App.Elastic.User,
	//	Password:          config.App.Elastic.Password,
	//	EnableDebugLogger: true,
	//	//CACert:   cert,
	//}
	//elasticSearchClient, err := elasticsearch.NewClient(cfg)
	elasticSearchClient, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatal("Failed to create Elasticsearch client:", err)
	}
	eventRepository := repository.NewElasticEventRepository(elasticSearchClient, config.App.Elastic.EventsIndex)

	for {
		select {
		case event := <-eventsChannel:
			log.Printf("Recieved event: %+v", event)
			_, _ = eventRepository.Save(event)
		}
	}
}
