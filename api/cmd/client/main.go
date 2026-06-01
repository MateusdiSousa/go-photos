package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"

	storagev1 "github.com/MateusdiSousa/go-photos/api/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr = flag.String("addr", "localhost:50051", "the address to connect to")
)

func main() {
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Erro ao criar cliente grpc: %s", err)
	}
	defer conn.Close()

	file, err := os.Open("./teste.jpg")

	if err != nil {
		log.Fatalf("Erro ao abrir arquivo de teste: %s", err)
	}

	client := storagev1.NewStorageServiceClient(conn)

	stream, err := client.Upload(context.Background())

	buffer := make([]byte, 32*1024)

	for {
		n, err := file.Read(buffer)

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("Erro ao ler arquivo de teste: %s", err)
		}

		err = stream.Send(&storagev1.UploadRequest{
			UserId:    "1",
			Filename:  "teste",
			MediaType: "image",
			Mimetype:  "image/jpg",
			Metadados: "",
			Size:      int64(n),
			Chunks:    buffer[:n],
		})

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("Erro ao fazer stream do arquivo de teste: %s", err)
		}

		log.Printf("Enviado chunk de %d bytes", n)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Erro ao fechar stream e receber resposta: %s", err)
	}

	log.Printf("Upload realizado com sucesso!, ID do arquivo salvo: %s", res.CmdId)
}
