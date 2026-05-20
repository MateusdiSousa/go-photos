package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/grpc"

	api "go-photos.api/internal/api"
	"go-photos.api/internal/client"
	storagev1 "go-photos.api/internal/proto"
	"go-photos.api/internal/storage"
)

var (
	port = flag.Int("port", 50051, "Server Port")
)

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Falha ao ouvir na porta %d: %s", port, err)
	}

	clientMinio := storage.GetClientMinio()
	producer := client.GetKafkaProducer()

	server := grpc.NewServer()

	storageService := api.NewStorageHandler(clientMinio, producer)

	storagev1.RegisterStorageServiceServer(server, storageService)

	go func() {
		for events := range producer.Events() {
			switch ev := events.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Fatalf("Falha ao enviar mensagem para tópico: %s", ev.TopicPartition)
				} else {
					log.Printf("Mensagem enviada para o tópico %s, chave = %-10s value = %s\n",
						*ev.TopicPartition.Topic,
						string(ev.Key),
						string(ev.Value))

				}

			}

		}
	}()

	log.Printf("Servidor ouvindo em: %s", lis.Addr())

	if err := server.Serve(lis); err != nil {
		log.Fatalf("Erro ao iniciar o servidor: %s", err)
	}
}
