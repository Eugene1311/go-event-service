package main

import (
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/segmentio/kafka-go"
	"go-event-service/consumer"
	"go-event-service/model"
	"go-event-service/repository"
	"go-event-service/service"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

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
	elasticSearchClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Username: config.App.Elastic.User,
		Password: config.App.Elastic.Password,
	})
	if err != nil {
		log.Fatal("Failed to create Elasticsearch client:", err)
	}
	eventRepository := repository.NewElasticEventRepository(elasticSearchClient, config.App.Elastic.EventsIndex)
	eventService := service.NewDomainEventService(eventRepository)

	eventsConsumer := consumer.NewEventsConsumer(kafkaReader, eventService)
	go eventsConsumer.Process()

	exitSignal := <-signalCh
	log.Printf("Received signal %+v", exitSignal)
	eventsConsumer.Close()
}
