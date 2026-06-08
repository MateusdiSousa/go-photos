package worker

import (
	"context"
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
	registro "github.com/MateusdiSousa/go-photos/api/domain/registro"
	"github.com/MateusdiSousa/go-photos/processador/internal/consulta/repository"
	"github.com/MateusdiSousa/go-photos/processador/internal/database"
)

var TOPICOS_CONSULTA = []string{"registro.media"}

func InitConsultaWorker(ctx context.Context, client sarama.ConsumerGroup) error {
	postgresConn, err := database.GetInstace()
	if err != nil {
		log.Printf("Falha ao conectar com banco de dados: %s", err)
		return err
	}

	consultaRepository, err := repository.NewConsultaRepository(postgresConn)
	if err != nil {
		log.Printf("Falha ao criar repositorio de consulta: %s", err)
		return err
	}

	consumer := NewConsumer(make(chan bool), consultaRepository)

	go func() {
		for {
			if err := client.Consume(ctx, TOPICOS_CONSULTA, consumer); err != nil {
				log.Printf("Falha ao consumir dos tópicos Kafka: %s", err)
			}

			if ctx.Err() != nil {
				return
			}

			consumer.Ready = make(chan bool)
		}
	}()

	<-consumer.Ready

	log.Println("Processador de consulta está rodando...")
	return nil
}

type Consumer struct {
	Ready      chan bool
	repository *repository.ConsultaRepository
}

func NewConsumer(channel chan bool, repo *repository.ConsultaRepository) *Consumer {
	return &Consumer{
		Ready:      channel,
		repository: repo}
}

func (c *Consumer) ProcessMessage(message []byte) error {
	var data registro.RegistroComando
	err := json.Unmarshal(message, &data)
	if err != nil {
		log.Printf("Falha ao converter mensagem: %s", err)
	}

	err = c.repository.SaveRegistroMedia(context.Background(), data.Cadastro)
	if err != nil {
		log.Printf("Falha ao salvar arquivo no banco de dados: %s", err)
	} else {
		log.Print("Mensagem processada com sucesso.")
	}
	return nil
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	c.Ready <- true
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
