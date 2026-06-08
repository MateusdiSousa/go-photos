package comom_kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func NewKafkaProducer(server string, groupId string) (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": server,
		"group.id":          groupId,
	})
	if err != nil {
		return nil, err
	}

	return producer, nil
}
