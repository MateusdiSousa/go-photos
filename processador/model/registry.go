package model

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/dsoprea/go-logging"
)

var (
	commandProcessor *CommandProcessor
)

type Evento struct {
	Topic    string
	Key      string
	Mensagem []byte
}

type HandlerFunc func(ctx context.Context, msg *sarama.ConsumerMessage) ([]Evento, error)

type CommandProcessor struct {
	Registry map[string]HandlerFunc
}

func newCommandProcessor() *CommandProcessor {
	return &CommandProcessor{
		Registry: make(map[string]HandlerFunc),
	}
}

func init() {
	commandProcessor = newCommandProcessor()
}

func RegistrarExecuta[T any](commandName string, fn func(ctx context.Context, msg *sarama.ConsumerMessage, cmd T) ([]Evento, error)) {
	commandProcessor.Registry[commandName] = func(ctx context.Context, msg *sarama.ConsumerMessage) ([]Evento, error) {
		var cmd T
		if err := json.Unmarshal(msg.Value, &cmd); err != nil {
			return nil, fmt.Errorf("Falha a deserializar mensagem: %s", err)
		}
		return fn(ctx, msg, cmd)
	}
}

func Executa(ctx context.Context, commandName string, msg *sarama.ConsumerMessage, producer *kafka.Producer) error {
	handler, exists := commandProcessor.Registry[commandName]
	if !exists {
		return fmt.Errorf("Comando %s não foi encontrado.", commandName)
	}

	eventos, err := handler(ctx, msg)
	if err != nil {
		return err
	}

	if len(eventos) > 0 {
		for _, evento := range eventos {
			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &evento.Topic, Partition: kafka.PartitionAny},
				Key:            []byte(evento.Key),
				Value:          evento.Mensagem,
			}, nil)
			if err != nil {
				log.Errorf("Erro ao publicar mensagem para o tópico kafka %s: %s", evento.Topic, evento.Mensagem)
			}
		}
	}

	return nil
}
