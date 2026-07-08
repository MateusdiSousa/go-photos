// internal/workers/archiver/worker/archiver.go
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	"github.com/MateusdiSousa/go-photos/processador/internal/workers/archiver/repository"
	"github.com/jackc/pgx/v5"
)

const (
	REGISTRO_MEDIA = "registro.media"

	EVENTO_REGISTRO_UPLOAD = "registro-upload"
	EVENTO_REGISTRO_DELETE = "registro-delete"

	maxRetries = 3
)

var TOPICOS_ARCHIVER = []string{REGISTRO_MEDIA}

func InitArchiverWorker(ctx context.Context, client sarama.ConsumerGroup, postgresConn *pgx.Conn) error {
	eventStoreRepo, err := repository.NewEventStoreRepository(postgresConn)
	if err != nil {
		log.Printf("Falha ao criar repositório de event store: %s", err)
		return err
	}

	consumer := NewArchiverConsumer(make(chan bool), eventStoreRepo)

	go func() {
		for {
			if err := client.Consume(ctx, TOPICOS_ARCHIVER, consumer); err != nil {
				log.Printf("Falha ao consumir dos tópicos Kafka: %s", err)
			}

			if ctx.Err() != nil {
				return
			}

			consumer.Ready = make(chan bool)
		}
	}()

	<-consumer.Ready

	log.Println("Processador de archiver está rodando...")
	return nil
}

type ArchiverConsumer struct {
	Ready      chan bool
	repository *repository.EventStoreRepository
	mu         sync.RWMutex
	processed  map[string]bool
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewArchiverConsumer(channel chan bool, repo *repository.EventStoreRepository) *ArchiverConsumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &ArchiverConsumer{
		Ready:      channel,
		repository: repo,
		processed:  make(map[string]bool),
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (c *ArchiverConsumer) Shutdown() {
	log.Println("Iniciando shutdown do archiver...")
	c.cancel()
	c.wg.Wait()
	log.Println("Archiver finalizado")
}

func (c *ArchiverConsumer) ProcessMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	// Verificar se mensagem já foi processada
	msgKey := fmt.Sprintf("%s-%d-%d", msg.Topic, msg.Partition, msg.Offset)
	c.mu.RLock()
	if c.processed[msgKey] {
		c.mu.RUnlock()
		log.Printf("Mensagem %s já processada, ignorando", msgKey)
		return nil
	}
	c.mu.RUnlock()

	// Retry com backoff
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := c.processMessageWithRetry(ctx, msg); err != nil {
			lastErr = err
			if attempt < maxRetries-1 {
				backoff := time.Duration(attempt*attempt) * time.Second
				log.Printf("Tentativa %d falhou, retentando em %v: %v", attempt+1, backoff, err)
				time.Sleep(backoff)
			}
			continue
		}

		// Marcar como processada
		c.mu.Lock()
		c.processed[msgKey] = true
		c.mu.Unlock()

		return nil
	}

	return fmt.Errorf("falha após %d tentativas: %w", maxRetries, lastErr)
}

func (c *ArchiverConsumer) processMessageWithRetry(ctx context.Context, msg *sarama.ConsumerMessage) error {
	// Validar evento
	evento, err := c.validateEvent(msg)
	if err != nil {
		return err
	}

	topico := msg.Topic
	switch topico {
	case REGISTRO_MEDIA:
		switch evento.EventType {
		case EVENTO_REGISTRO_UPLOAD:
			return c.handleUploadEvent(ctx, msg.Value)
		case EVENTO_REGISTRO_DELETE:
			return c.handleDeleteEvent(ctx, msg.Value)
		default:
			log.Printf("Tipo de evento desconhecido: %s", evento.EventType)
			return nil
		}
	default:
		log.Printf("Tópico desconhecido: %s", topico)
		return nil
	}
}

func (c *ArchiverConsumer) validateEvent(msg *sarama.ConsumerMessage) (*registro.Evento[any], error) {
	if len(msg.Value) == 0 {
		return nil, fmt.Errorf("payload vazio")
	}

	var evento registro.Evento[any]
	if err := json.Unmarshal(msg.Value, &evento); err != nil {
		return nil, fmt.Errorf("erro ao unmarshal evento: %w", err)
	}

	if evento.EventType == "" {
		return nil, fmt.Errorf("tipo de evento não especificado")
	}

	if evento.EventId == "" {
		return nil, fmt.Errorf("event_id não especificado")
	}

	if evento.AggregateId == "" {
		return nil, fmt.Errorf("aggregate_id não especificado")
	}

	return &evento, nil
}

func (c *ArchiverConsumer) handleUploadEvent(ctx context.Context, data []byte) error {
	var registroMedia registro.Evento[registro.RegistroMedia]
	if err := json.Unmarshal(data, &registroMedia); err != nil {
		return fmt.Errorf("falha ao deserializar evento registro-upload: %w", err)
	}

	// Converter para Evento[any] para salvar no event store
	event := registro.Evento[any]{
		EventId:     registroMedia.EventId,
		AggregateId: registroMedia.AggregateId,
		EventType:   registroMedia.EventType,
		Version:     registroMedia.Version,
		Dados:       registroMedia.Dados,
		CreatedAt:   registroMedia.CreatedAt,
	}

	// Salvar no event store
	if err := c.repository.SaveEvent(ctx, event); err != nil {
		return fmt.Errorf("falha ao salvar evento no event store: %w", err)
	}

	log.Printf("Evento de upload arquivado com sucesso: %s (aggregate: %s)",
		registroMedia.EventId, registroMedia.AggregateId)
	return nil
}

func (c *ArchiverConsumer) handleDeleteEvent(ctx context.Context, data []byte) error {
	var registroUser registro.Evento[registro.RegistroUser]
	if err := json.Unmarshal(data, &registroUser); err != nil {
		return fmt.Errorf("falha ao deserializar evento registro-delete: %w", err)
	}

	// Validar dados
	if registroUser.Dados.FileId == "" {
		return fmt.Errorf("file_id não pode ser vazio")
	}
	if registroUser.Dados.UserId == "" {
		return fmt.Errorf("user_id não pode ser vazio")
	}
	if registroUser.Dados.HashSha256 == "" {
		return fmt.Errorf("hash_sha256 não pode ser vazio")
	}

	// Converter para Evento[any] para salvar no event store
	event := registro.Evento[any]{
		EventId:     registroUser.EventId,
		AggregateId: registroUser.AggregateId,
		EventType:   registroUser.EventType,
		Version:     registroUser.Version,
		Dados:       registroUser.Dados,
		CreatedAt:   registroUser.CreatedAt,
	}

	// Salvar no event store
	if err := c.repository.SaveEvent(ctx, event); err != nil {
		return fmt.Errorf("falha ao salvar evento no event store: %w", err)
	}

	log.Printf("Evento de delete arquivado com sucesso: %s (aggregate: %s, file: %s)",
		registroUser.EventId, registroUser.AggregateId, registroUser.Dados.FileId)
	return nil
}

func (c *ArchiverConsumer) Setup(sarama.ConsumerGroupSession) error {
	c.Ready <- true
	return nil
}

func (c *ArchiverConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ArchiverConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.wg.Add(1)
	defer c.wg.Done()

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Print("Canal de mensagens fechado!")
				return nil
			}

			log.Printf("Mensagem recebida no archiver do tópico: %s, partição: %d, offset: %d",
				message.Topic, message.Partition, message.Offset)

			// Processar com timeout
			ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
			err := c.ProcessMessage(ctx, message)
			cancel()

			if err != nil {
				log.Printf("Falha ao processar mensagem no archiver: %v", err)
				// Não para o worker em caso de erro
				continue
			}

			session.MarkMessage(message, "")
		case <-session.Context().Done():
			log.Println("Contexto da sessão do archiver cancelado")
			return nil
		case <-c.ctx.Done():
			log.Println("Contexto do archiver cancelado")
			return nil
		}
	}
}
