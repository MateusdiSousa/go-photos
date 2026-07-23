package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"

	api "github.com/MateusdiSousa/go-photos/api/internal/api"
	"github.com/MateusdiSousa/go-photos/api/internal/client"
	"github.com/MateusdiSousa/go-photos/api/internal/interceptor"
	storagev1 "github.com/MateusdiSousa/go-photos/api/internal/proto"
	"github.com/MateusdiSousa/go-photos/api/internal/repository"
	"github.com/MateusdiSousa/go-photos/api/internal/service"
	"github.com/MateusdiSousa/go-photos/api/internal/storage"
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
	godotenv.Load()
	databaseUrl := os.Getenv("POSTGRES_DATABASE_URL")

	// Conexões, Clientes e Repositórios

	connPostgres, err := pgx.Connect(context.Background(), databaseUrl)
	if err != nil {
		log.Fatalf("Falha ao criar conexão com banco postgres na url %s: %s", databaseUrl, err)
	}

	clientMinio, err := storage.GetClientMinio()
	if err != nil {
		log.Fatalf("Falha ao instaciar cliente do MinioIO: %s", err)
	}

	producer := client.GetKafkaProducer()

	mediaRepository, err := repository.NewMediaRepository(connPostgres)
	if err != nil {
		log.Fatalf("Falha ao criar repositorio de media: %s", err)
	}

	// SERVICES
	mediaService := service.NewMediaService(mediaRepository, clientMinio)

	// SERVER
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.ServerInterceptor))
	storageServer := api.NewStorageHandler(clientMinio, producer, mediaService)

	storagev1.RegisterStorageServiceServer(server, storageServer)

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
