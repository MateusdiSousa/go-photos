package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	registro_model "github.com/MateusdiSousa/go-photos/processador/internal/registro/model"
	"github.com/MateusdiSousa/go-photos/processador/model"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var TOPICOS_REGISTRO = []string{"registro.comando"}

type Consumer struct {
	Ready    chan bool
	Producer *kafka.Producer
}

func InitRegistroWorker(ctx context.Context, client sarama.ConsumerGroup, p *kafka.Producer) error {
	registro_model.SetupRegistroModel()
	consumer := NewConsumer(make(chan bool), p)

	go func() {
		for {
			if err := client.Consume(ctx, TOPICOS_REGISTRO, consumer); err != nil {
				log.Printf("Falha ao consumir dos tópicos kafka: %s", err)
			}

			if ctx.Err() != nil {
				return
			}

			consumer.Ready = make(chan bool)
		}
	}()

	<-consumer.Ready

	log.Println("Processador de registro está rodando ...")
	return nil
}

func NewConsumer(channel chan bool, p *kafka.Producer) *Consumer {
	return &Consumer{
		Ready:    channel,
		Producer: p,
	}
}

func (c *Consumer) ProcessMessage(msg *sarama.ConsumerMessage) error {
	switch msg.Topic {
	case "registro.comando":
		var comando registro.Comando[registro.RegistroMedia]
		err := json.Unmarshal(msg.Value, &comando)
		if err != nil {
			return fmt.Errorf("Falha ao converter a mensagem: %s", err)
		}

		switch comando.Status {
		case "rejeitado":
			log.Println("Não implementado ainda.")
		case "executado":
			log.Println("Não implementado ainda.")
			// Caso seja executado, gerar evento de registro.media e atualizar o banco de dados de registro
		case "pendente":
			if err := model.Executa(context.Background(), comando.TipoCmd, msg, c.Producer); err != nil {
				log.Println(err.Error())
				return sarama.ErrInvalidMessage
			}
		}
	default:
	}

	return nil
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.Ready)
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Print("Canal de mensagens fechado!")
				return nil
			}

			log.Printf("Mensagem recebida do tópico: %s", message.Topic)

			err := c.ProcessMessage(message)
			if err != nil {
				log.Printf("Falha ao processar mensagem: %s", err)
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
