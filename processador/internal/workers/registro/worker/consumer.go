// internal/workers/registro/worker/registro.go
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
	dispatcher "github.com/MateusdiSousa/go-photos/processador/dispatcher"
	registro_model "github.com/MateusdiSousa/go-photos/processador/internal/workers/registro/model"
	"github.com/MateusdiSousa/go-photos/processador/internal/workers/registro/repository"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v5"
)

const (
	maxRetries = 3
)

var TOPICOS_REGISTRO = []string{"registro.comando"}

func InitRegistroWorker(ctx context.Context, client sarama.ConsumerGroup, p *kafka.Producer, conn *pgx.Conn) error {
	repositorio, err := repository.NewRegistroRepository(conn)
	if err != nil {
		log.Printf("Falha ao criar repositório de registro: %s", err)
		return err
	}

	registro_model.SetupRegistroModel(repositorio)

	consumer := NewRegistroConsumer(make(chan bool), p)

	go func() {
		for {
			if err := client.Consume(ctx, TOPICOS_REGISTRO, consumer); err != nil {
				log.Printf("Falha ao consumir dos tópicos Kafka: %s", err)
			}

			if ctx.Err() != nil {
				return
			}

			consumer.Ready = make(chan bool)
		}
	}()

	<-consumer.Ready

	log.Println("Processador de registro está rodando...")
	return nil
}

type RegistroConsumer struct {
	Ready     chan bool
	Producer  *kafka.Producer
	mu        sync.RWMutex
	processed map[string]bool
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewRegistroConsumer(channel chan bool, p *kafka.Producer) *RegistroConsumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &RegistroConsumer{
		Ready:     channel,
		Producer:  p,
		processed: make(map[string]bool),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Shutdown realiza o graceful shutdown do consumer
func (c *RegistroConsumer) Shutdown() {
	log.Println("Iniciando shutdown do processador de registro...")
	c.cancel()
	c.wg.Wait()
	log.Println("Processador de registro finalizado")
}

func (c *RegistroConsumer) ProcessMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	msgKey := fmt.Sprintf("%s-%d-%d", msg.Topic, msg.Partition, msg.Offset)
	c.mu.RLock()
	if c.processed[msgKey] {
		c.mu.RUnlock()
		log.Printf("Mensagem %s já processada, ignorando", msgKey)
		return nil
	}
	c.mu.RUnlock()

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

		c.mu.Lock()
		c.processed[msgKey] = true
		c.mu.Unlock()

		return nil
	}

	return fmt.Errorf("falha após %d tentativas: %w", maxRetries, lastErr)
}

func (c *RegistroConsumer) processMessageWithRetry(ctx context.Context, msg *sarama.ConsumerMessage) error {
	switch msg.Topic {
	case "registro.comando":
		return c.handleRegistroComando(ctx, msg)
	default:
		log.Printf("Tópico desconhecido: %s", msg.Topic)
		return nil
	}
}

// handleRegistroComando processa comandos de registro
func (c *RegistroConsumer) handleRegistroComando(ctx context.Context, msg *sarama.ConsumerMessage) error {
	// Validar payload
	if len(msg.Value) == 0 {
		return fmt.Errorf("payload vazio")
	}

	var comando registro.Comando[registro.RegistroMedia]
	if err := json.Unmarshal(msg.Value, &comando); err != nil {
		return fmt.Errorf("falha ao converter a mensagem: %w", err)
	}

	// Validar campos obrigatórios
	if comando.TipoCmd == "" {
		return fmt.Errorf("tipo de comando não especificado")
	}

	log.Printf("Processando comando: tipo=%s, status=%s", comando.TipoCmd, comando.Status)

	switch comando.Status {
	case "rejeitado":
		log.Printf("Comando rejeitado: %s", comando.TipoCmd)
		// TODO: Implementar lógica para comandos rejeitados
		return nil

	case "executado":
		// Processar comando executado
		if comando.TipoCmd == "registro-upload" {
			if err := dispatcher.Executa(ctx, fmt.Sprintf("%s-executado", comando.TipoCmd), msg, c.Producer); err != nil {
				log.Printf("Erro ao executar dispatcher para comando executado: %v", err)
				return fmt.Errorf("falha ao executar dispatcher: %w", err)
			}
		}

		log.Printf("Comando executado com sucesso: %s", comando.TipoCmd)
		return nil

	case "pendente":
		// Processar comando pendente
		if err := dispatcher.Executa(ctx, comando.TipoCmd, msg, c.Producer); err != nil {
			log.Printf("Erro ao executar dispatcher para comando pendente: %v", err)
			return fmt.Errorf("falha ao executar dispatcher: %w", err)
		}
		log.Printf("Comando pendente processado: %s", comando.TipoCmd)
		return nil

	default:
		return fmt.Errorf("status de comando desconhecido: %s", comando.Status)
	}
}

// Setup é chamado quando o consumer é iniciado
func (c *RegistroConsumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.Ready)
	return nil
}

// Cleanup é chamado quando o consumer é finalizado
func (c *RegistroConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim consome as mensagens de uma partição
func (c *RegistroConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.wg.Add(1)
	defer c.wg.Done()

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Print("Canal de mensagens fechado!")
				return nil
			}

			log.Printf("Mensagem recebida do tópico: %s, partição: %d, offset: %d",
				message.Topic, message.Partition, message.Offset)

			// Processar com timeout
			ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
			err := c.ProcessMessage(ctx, message)
			cancel()

			if err != nil {
				log.Printf("Falha ao processar mensagem: %v", err)
				// Não para o worker em caso de erro
				continue
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			log.Println("Contexto da sessão cancelado")
			return nil

		case <-c.ctx.Done():
			log.Println("Contexto do worker cancelado")
			return nil
		}
	}
}
