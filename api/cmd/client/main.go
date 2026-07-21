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
	"google.golang.org/grpc/metadata" // Importado para injetar o ID real no contexto
)

var (
	addr      = flag.String("addr", "localhost:50051", "the address to connect to")
	mediaType = flag.String("t", "image", "tipo de mídia para testar: 'image' ou 'video'")
)

func main() {
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Erro ao criar cliente grpc: %s", err)
	}
	defer conn.Close()

	// Definição dinâmica dos metadados e caminhos baseados na flag
	var (
		filePath     string
		filename     string
		reqMediaType string
		reqMimetype  string
	)

	switch *mediaType {
	case "video":
		filePath = "./teste.mp4"
		filename = "teste.mp4"
		reqMediaType = "video"
		reqMimetype = "video/mp4"
	case "image":
		fallthrough
	default:
		filePath = "./teste.jpg"
		filename = "teste.jpg"
		reqMediaType = "image"
		reqMimetype = "image/jpg"
	}

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Erro ao abrir arquivo de teste (%s): %s", filePath, err)
	}
	defer file.Close()

	client := storagev1.NewStorageServiceClient(conn)

	// 🔥 Alinhado com a TASK 005: Injetando o x-user-id via Metadata no contexto de saída
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("user-id", "1"))

	stream, err := client.Upload(ctx)
	if err != nil {
		log.Fatalf("Erro ao abrir stream de upload: %s", err)
	}

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
			Filename:  filename,
			MediaType: reqMediaType,
			Mimetype:  reqMimetype,
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
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Erro ao fechar stream e receber resposta: %s", err)
	}

	log.Printf("Upload realizado com sucesso!, ID do arquivo salvo: %s", res.CmdId)
}
