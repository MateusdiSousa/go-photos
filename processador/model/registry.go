package model

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/dsoprea/go-logging"
)

var (
	commandProcessor *CommandProcessor
)

type MensagemKafka struct {
	Topic    string
	Key      string
	Mensagem []byte
}

type HandlerFunc func(ctx context.Context, msg *sarama.ConsumerMessage) ([]MensagemKafka, []MensagemKafka)

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

func RegistrarExecuta[T any](commandName string, fn func(ctx context.Context, msg *sarama.ConsumerMessage, cmd T) ([]MensagemKafka, []MensagemKafka)) {
	commandProcessor.Registry[commandName] = func(ctx context.Context, msg *sarama.ConsumerMessage) ([]MensagemKafka, []MensagemKafka) {
		var cmd T
		if err := json.Unmarshal(msg.Value, &cmd); err != nil {
			return nil, []MensagemKafka{{Mensagem: []byte("Falha ao desserializar mensagem.")}}
		}
		return fn(ctx, msg, cmd)
	}
}

func Executa(ctx context.Context, commandName string, msg *sarama.ConsumerMessage, producer *kafka.Producer) error {
	handler, exists := commandProcessor.Registry[commandName]
	if !exists {
		return fmt.Errorf("Comando %s não foi encontrado.", commandName)
	}

	eventos, erros := handler(ctx, msg)
	if len(erros) > 0 {
		EnviarMensagens(ctx, erros, producer)
		return nil
	}

	if len(eventos) > 0 {
		EnviarMensagens(ctx, eventos, producer)
		return nil
	}

	return nil
}

func EnviarMensagens(ctx context.Context, mensagens []MensagemKafka, producer *kafka.Producer) {
	if producer != nil {
		for _, evento := range mensagens {
			err := producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &evento.Topic, Partition: kafka.PartitionAny},
				Key:            []byte(evento.Key),
				Value:          evento.Mensagem,
			}, nil)
			if err != nil {
				log.Errorf("Erro ao publicar mensagem para o tópico kafka %s: %s", evento.Topic, evento.Mensagem)
				ctx.Err()
			}
		}
	}
}

func NewMensagemKafkaRejeitada[T any](cmd *registro.Comando[T], topico string, chave string, err error) MensagemKafka {
	cmd.Status = "rejeitado"
	cmd.Erros = append(cmd.Erros, err.Error())

	data, _ := json.Marshal(cmd)
	return MensagemKafka{
		Topic:    topico,
		Key:      chave,
		Mensagem: data,
	}
}
