package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	consulta_worker "github.com/MateusdiSousa/go-photos/processador/internal/consulta/worker"
	"github.com/MateusdiSousa/go-photos/processador/internal/database"
	registro_worker "github.com/MateusdiSousa/go-photos/processador/internal/registro/worker"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Iniciando o Kafka
	config := sarama.NewConfig()
	config.Version = sarama.V3_6_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Configurações de Retentativa para o Coordinator
	config.Metadata.Retry.Max = 10       // Tenta reaver os metadados do cluster até 10 vezes
	config.Metadata.Retry.Backoff = 2000 // Aguarda 2 segundos entre as tentativas
	//	config.Consumer.MaxProcessingTime = time.Duration(50) // Tempo máximo de espera para o broker responder

	// Garante que o grupo de consumidores vai esperar o coordinator estabilizar
	//	config.Consumer.Group.Heartbeat.Interval = 3000

	workerName := flag.String("proc", "consulta", "Qual processador será criado?")
	flag.Parse()

	brokers := []string{"localhost:9094"}

	groupId := "go-photos-processors-" + *workerName

	// Configurando cliente kafka
	client, err := sarama.NewConsumerGroup(brokers, groupId, config)
	if err != nil {
		log.Fatalf("Falha ao criar grupo de consumo: %s", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch *workerName {
	case "consulta":
		log.Println("Iniciando processador de consulta...")
		postgresConn, err := database.GetInstace()
		if err != nil {
			log.Fatalf("Falha ao iniciar conexão com banco de dados: %s", err)
		}
		defer postgresConn.Close(ctx)
		err = consulta_worker.InitConsultaWorker(ctx, client, postgresConn)
		if err != nil {
			log.Fatalf("Falha ao iniciar processador de consulta: %s", err)
		}

	case "registro":
		log.Println("Iniciando processador de registro...")
		p, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": brokers[0],
			"group.id":          groupId,
		})
		if err != nil {
			log.Fatalf("Falha ao iniciar processador de registro: %s", err)
		}

		err = registro_worker.InitRegistroWorker(ctx, client, p)
		if err != nil {
			log.Fatalf("Falha ao iniciar processador de registro: %s", err)
		}

	default:
		log.Printf("Não existe processador com o nome '%s'.", *workerName)
		os.Exit(0)
	}

	// Gracious Shutdown para parar o consumer sem corromper offsets
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	log.Println("Encerrando o processador de forma segura...")
}
