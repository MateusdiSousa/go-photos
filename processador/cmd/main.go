package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	consulta "github.com/MateusdiSousa/go-photos/processador/internal/consulta/domain"
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V4_2_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Configurações de Retentativa para o Coordinator
	config.Metadata.Retry.Max = 10       // Tenta reaver os metadados do cluster até 10 vezes
	config.Metadata.Retry.Backoff = 2000 // Aguarda 2 segundos entre as tentativas
	//	config.Consumer.MaxProcessingTime = time.Duration(50) // Tempo máximo de espera para o broker responder

	// Garante que o grupo de consumidores vai esperar o coordinator estabilizar
	//	config.Consumer.Group.Heartbeat.Interval = 3000

	brokers := []string{"localhost:9094"}

	groupId := "go-photos-processors"

	client, err := sarama.NewConsumerGroup(brokers, groupId, config)
	if err != nil {
		log.Fatalf("Falha ao criar grupo de consumo: %s", err)
	}
	defer client.Close()

	consumer := consulta.Consumer{
		Ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		topicos := []string{"registro.media"}
		for {
			if err := client.Consume(ctx, topicos, &consumer); err != nil {
				log.Fatalf("Falha ao consumir dos tópicos Kafka: %s", err)
			}

			if ctx.Err() != nil {
				return
			}

			consumer.Ready = make(chan bool)
		}

	}()

	<-consumer.Ready
	log.Println("Processador de consulta está rodando...")

	// Gracious Shutdown para parar o consumer sem corromper offsets
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	log.Println("Encerrando o processador de forma segura...")
}
