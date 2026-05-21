package api_test

import (
	"context"
	"log"
	"net"
	"os"
	"testing"

	storagev1 "github.com/MateusdiSousa/go-photos/api/internal/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufsize = 1024 * 1024

var lis *bufconn.Listener

func init() {
	lis = bufconn.Listen(bufsize)
	s := grpc.NewServer()
	storagev1.RegisterStorageServiceServer(s, NewStorageHandler())
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Falha ao iniciar o servidor: %s", err)
		}
	}()
}

func bufDialer(c context.Context, s string) (net.Conn, error) {
	return lis.Dial()
}

func TestUpload(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Erro ao abrir conexao com servidor: %s", err)
	}
	defer conn.Close()

	client := storagev1.NewStorageServiceClient(conn)

	// INICIA STREAM DE DADOS
	stream, err := client.Upload(ctx)
	if err != nil {
		t.Fatalf("Erro ao criar stream de dados: %s", err)
	}

	// Simula o envio de chunks
	chunks := [][]byte{
		[]byte("primeira parte "),
		[]byte("segunda parte "),
		[]byte("final."),
	}

	var expectedSize int64

	for _, chunk := range chunks {
		err := stream.Send(&storagev1.UploadRequest{
			Chunks:   chunk,
			Filename: "Teste",
			User:     "Teste",
			Ext:      "txt",
		})
		if err != nil {
			t.Fatalf("Erro fatal ao enviar chunks: %s", err)
		}
		expectedSize += int64(len(chunk))
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("Erro ao fechar e receber resposta: %s", err)
	}

	if res.Size != expectedSize {
		t.Errorf("Tamanho incorreto, esperado: %d, atual: %d", expectedSize, res.Size)
	}

	if res.FileId == "" {
		t.Errorf("FieldID veio vazio")
	}

	// Limpeza: verifica se o arquivo foi criado e remove
	tempPath := "temp/" + res.FileId
	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		t.Errorf("Arquivo temporário não foi encontrado em: %s", tempPath)
	} else {
		os.Remove(tempPath) // Remove após o teste
	}
}
