package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	consulta "github.com/MateusdiSousa/go-photos/processador/internal/consulta/domain"
)

func main() {
	// Iniciando o Kafka
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

	// Configurando cliente kafka
	client, err := sarama.NewConsumerGroup(brokers, groupId, config)
	if err != nil {
		log.Fatalf("Falha ao criar grupo de consumo: %s", err)
	}
	defer client.Close()

	workerName := flag.String("processador", "consulta", "Qual processador será criado?")

	switch *workerName {
	case "consulta":
		log.Println("Iniciando processador de consulta...")

		err = consulta.InitConsultaWorker(client)
		if err != nil {
			log.Fatalf("Falha ao iniciar processador de consulta: %s", err)
		}
	default:
		log.Printf("Não existe processador com o nome '%s'.")
		os.Exit(0)
	}

	// Gracious Shutdown para parar o consumer sem corromper offsets
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	log.Println("Encerrando o processador de forma segura...")
}
