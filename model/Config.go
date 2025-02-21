package model

type Config struct {
	App AppConfig `yaml:"app"`
}

type AppConfig struct {
	Kafka   KafkaConfig   `yaml:"kafka"`
	Elastic ElasticConfig `yaml:"elastic"`
}

type KafkaConfig struct {
	Brokers       []string `yaml:"brokers"`
	ConsumerGroup string   `yaml:"consumer-group"`
	Topic         string   `yaml:"topic"`
}

type ElasticConfig struct {
	Addresses   []string `yaml:"addresses"`
	User        string   `yaml:"user"`
	Password    string   `yaml:"password"`
	EventsIndex string   `yaml:"events-index"`
}
