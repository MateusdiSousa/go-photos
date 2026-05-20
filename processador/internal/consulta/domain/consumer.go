package domain

import (
	"log"

	"github.com/IBM/sarama"
)

type Consumer struct {
	Ready chan bool
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	c.Ready <- true
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ProcessMessage(message []byte) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Print("Canal de mensagens fechado!")
			}

			log.Printf("Mensagem recebida do tópico: %s", message.Topic)

			err := c.ProcessMessage(message.Value)
			if err != nil {
				log.Printf("Falha ao processar a mensagem: %s", err)
				return err
			}

			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
