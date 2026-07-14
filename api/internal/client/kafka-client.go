package client

import (
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const server = "localhost:9094"
const clientId = "API"
const acks = "all"

var kafkaClient *kafka.Producer

func GetKafkaProducer() *kafka.Producer {
	var err error = nil
	if kafkaClient == nil {
		kafkaClient, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": server,
			"client.id":         clientId})
		if err != nil {
			log.Fatalf("Falha ao criar produtor kafka: %s", err)
			os.Exit(1)
		}
		return kafkaClient
	}
	return kafkaClient
}
