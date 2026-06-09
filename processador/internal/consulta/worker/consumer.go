package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	"github.com/MateusdiSousa/go-photos/processador/internal/consulta/repository"
	"github.com/MateusdiSousa/go-photos/processador/internal/database"
)

const (
	REGISTRO_MEDIA = "registro.media"

	EVENTO_REGISTRO_UPLOAD = "registro-upload"
)

var TOPICOS_CONSULTA = []string{REGISTRO_MEDIA}

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

func (c *Consumer) ProcessMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {

	topico := msg.Topic
	log.Printf("MENSAGEM CHEGOU NO TOPICO: %s", topico)

	switch topico {
	case REGISTRO_MEDIA:
		log.Println("Chegou")
		var evento registro.Evento[registro.RegistroMedia]
		err := json.Unmarshal(msg.Value, &evento)
		if err != nil {
			log.Printf("Falha ao desserializar evento: %s", err)
			return err
		}

		log.Printf("EVENT TYPE: %s", evento.EventType)

		log.Printf("EVENT TYPE ESPERADO: %s", EVENTO_REGISTRO_UPLOAD)
		if evento.EventType == EVENTO_REGISTRO_UPLOAD {
			log.Println("SALVANDO DADOS NO BANCO DE DADOS")
			err = c.repository.SaveRegistroMedia(ctx, evento.Dados)
			if err != nil {
				log.Printf("Falha ao salvar arquivo no banco de dados: %s", err)
				return fmt.Errorf("Falha ao salvar Midia no banco de dados")
			}
			log.Println("DADOS SALVOS COM SUCESSO")
		}
		return nil
	default:
		log.Println("Não Chegou")
		return nil
	}
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

			err := c.ProcessMessage(session.Context(), message)
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
