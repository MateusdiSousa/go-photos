package model

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
)

var (
	commandProcessor *CommandProcessor
)

type HandlerFunc func(ctx context.Context, msg *sarama.ConsumerMessage) error

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

func RegistrarExecuta[T any](commandName string, fn func(ctx context.Context, msg *sarama.ConsumerMessage, cmd T) error) {
	commandProcessor.Registry[commandName] = func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		var cmd T
		if err := json.Unmarshal(msg.Value, &cmd); err != nil {
			return fmt.Errorf("Falha a deserializar mensagem: %s", err)
		}
		return fn(ctx, msg, cmd)
	}
}

func Executa(ctx context.Context, commandName string, msg *sarama.ConsumerMessage) error {
	handler, exists := commandProcessor.Registry[commandName]
	if !exists {
		return fmt.Errorf("Comando %s não foi encontrado.", commandName)
	}

	return handler(ctx, msg)
}
