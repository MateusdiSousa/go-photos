package storage_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/MateusdiSousa/go-photos/api/internal/storage"
)

func TestGetClientMinio(t *testing.T) {
	ctx := context.Background()

	// Use as credenciais que o seu script acabou de imprimir
	client := storage.GetClientMinio()

	// Tenta listar os buckets para ver se a chave funciona
	exists, err := client.BucketExists(ctx, "photos")
	if err != nil {
		t.Fatalf("Erro ao conectar no Garage: %v", err)
	}

	if !exists {
		t.Fatal("O bucket 'photos' não foi encontrado!")
	}

	fmt.Println("Conexão com Garage OK e Bucket encontrado!")
}
