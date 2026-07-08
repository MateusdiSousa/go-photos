package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	"github.com/MateusdiSousa/go-photos/processador/internal/workers/consulta/repository"
	storage "github.com/MateusdiSousa/go-photos/processador/s3"
	"github.com/jackc/pgx/v5"
)

const (
	REGISTRO_MEDIA = "registro.media"

	EVENTO_REGISTRO_UPLOAD = "registro-upload"
	EVENTO_REGISTRO_DELETE = "registro-delete"
)

var TOPICOS_CONSULTA = []string{REGISTRO_MEDIA}

func InitConsultaWorker(ctx context.Context, client sarama.ConsumerGroup, postgresConn *pgx.Conn) error {

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
	processed  map[string]bool
	mu         sync.RWMutex
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func (c *Consumer) Shutdown() {
	log.Println("Iniciando graceful shutdown")
	c.cancel()
	c.Ready <- true
	c.wg.Wait()
	log.Println("Worker finalizado")
}

func NewConsumer(channel chan bool, repo *repository.ConsultaRepository) *Consumer {
	return &Consumer{
		Ready:      channel,
		repository: repo,
		processed:  make(map[string]bool),
	}
}

func validarEvento(msg *sarama.ConsumerMessage) (registro.Evento[any], error) {
	if len(msg.Value) == 0 {
		return registro.Evento[any]{}, fmt.Errorf("Payload vazio.")
	}

	var evento registro.Evento[any]
	err := json.Unmarshal(msg.Value, &evento)
	if err != nil {
		return registro.Evento[any]{}, fmt.Errorf("Erro ao deserializar evento: %s", err)
	}

	if evento.EventType == "" {
		return registro.Evento[any]{}, fmt.Errorf("Tipo de evento não especificado.")
	}

	return evento, nil
}

func (c *Consumer) ProcessMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	// IDEMPOTENCIA
	msgKey := fmt.Sprintf("%s-%v-%v", msg.Topic, msg.Partition, msg.Offset)
	c.mu.Lock()
	if c.processed[msgKey] {
		c.mu.Unlock()
		log.Print("Mensagem já processada. ignorando...")
		return nil
	}
	c.mu.Unlock()

	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := c.processMessageWithRetry(ctx, msg)
		if err == nil {
			return nil
		}

		if attempt < maxRetries-1 {
			backoff := time.Duration(attempt*attempt) * time.Second
			log.Printf("Tentativa %d falhou, retentando em %v: %v", attempt+1, backoff, err)
			time.Sleep(backoff)
		}
	}
	return fmt.Errorf("falha após %d tentativas", maxRetries)
}

func (c *Consumer) processMessageWithRetry(ctx context.Context, msg *sarama.ConsumerMessage) error {
	evento, err := validarEvento(msg)
	if err != nil {
		return err
	}

	topico := msg.Topic
	switch topico {
	case REGISTRO_MEDIA:
		switch evento.EventType {
		case EVENTO_REGISTRO_UPLOAD:
			var registroMedia registro.Evento[registro.RegistroMedia]
			err := json.Unmarshal(msg.Value, &registroMedia)
			if err != nil {
				log.Printf("Falha ao deserializar evento registro-upload: %s", err)
				return fmt.Errorf("Falha ao deserializar evento")
			}

			err = c.repository.SaveRegistroMedia(ctx, registroMedia.Dados)
			if err != nil {
				log.Printf("Falha ao salvar arquivo no banco de dados: %s", err)
				return fmt.Errorf("Falha ao salvar Midia no banco de dados")
			}

		case EVENTO_REGISTRO_DELETE:
			var registroUser registro.Evento[registro.RegistroUser]
			err := json.Unmarshal(msg.Value, &registroUser)
			if err != nil {
				return fmt.Errorf("Falha ao deserializar evento registro-delete: %s", err)
			}

			// Iniciar transação
			tx, err := c.repository.BeginTx(ctx)
			if err != nil {
				return fmt.Errorf("Falha ao iniciar transação: %s", err)
			}
			defer tx.Rollback(ctx)

			// Criar repositório com transação
			txRepo := repository.NewConsultaRepositoryTx(tx)

			// Operações dentro da transação
			err = txRepo.DeleteRegistroMedia(ctx, registroUser.Dados.FileId, registroUser.Dados.UserId)
			if err != nil {
				return fmt.Errorf("Falha ao deletar registro: %s", err)
			}

			count, err := txRepo.CountRegistroByHash(ctx, registroUser.Dados.HashSha256)
			if err != nil {
				return fmt.Errorf("Falha ao contar registros por hash: %s", err)
			}

			// Se for o último registro, deletar do storage
			if count == 0 {
				if err = storage.DeletePhotoAndThumbnailByID(ctx, registroUser.Dados.HashSha256); err != nil {
					// Verificar se é erro de "não encontrado" (aceitável)
					if !strings.Contains(err.Error(), "não encontrado") {
						return fmt.Errorf("Falha ao deletar thumbnail e imagem do armazenamento: %s", err)
					}
					log.Printf("Arquivo não encontrado no storage, ignorando: %v", err)
				}
				log.Printf("Arquivo deletado do S3 com sucesso: %s", registroUser.Dados.HashSha256)
			}

			// Commit da transação
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("Falha ao commitar transação: %s", err)
			}

			log.Printf("Registro deletado com sucesso: %s", registroUser.Dados.FileId)
			return nil
		}

		return nil
	default:
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
	c.wg.Add(1)
	defer c.wg.Done()

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
