package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	migrationDir := "./migrations"
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9094",
	})
	if err != nil {
		log.Fatalf("Falha ao conectar no Kafka: %v", err)
	}
	defer adminClient.Close()

	// 1. Ler e ordenar arquivos por numeração
	files, err := os.ReadDir(migrationDir)
	if err != nil {
		log.Fatalf("Erro ao ler diretório: %v", err)
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".txt") {
			continue
		}

		fmt.Printf("--- Executando Migração: %s ---\n", file.Name())
		path := filepath.Join(migrationDir, file.Name())
		executeMigrationFile(adminClient, path)
	}
}

func executeMigrationFile(admin *kafka.AdminClient, path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Printf("Erro ao abrir arquivo %s: %v", path, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue // Ignora linhas vazias e comentários
		}

		parts := strings.Fields(line)
		if parts[0] == "create-topic" {
			handleCreateTopic(admin, parts[1:])
		}
	}
}

func handleCreateTopic(admin *kafka.AdminClient, args []string) {
	// Definindo flags locais para interpretar a linha
	f := flag.NewFlagSet("create-topic", flag.ContinueOnError)
	particao := f.Int("particao", 1, "")
	retencao := f.Int("retencao", 7, "") // Dias
	replicacao := f.Int("replicacao", 1, "")
	nome := f.String("nome", "", "")

	err := f.Parse(args)
	if err != nil || *nome == "" {
		log.Printf("Comando inválido: %v", args)
		return
	}

	// Conversão de dias para milissegundos (o que o Kafka entende)
	retentionMs := strconv.Itoa(*retencao * 24 * 60 * 60 * 1000)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{{
		Topic:             *nome,
		NumPartitions:     *particao,
		ReplicationFactor: *replicacao,
		Config:            map[string]string{"retention.ms": retentionMs},
	}})

	if err != nil {
		log.Printf("Erro ao criar tópico %s: %v", *nome, err)
		return
	}

	for _, res := range results {
		if res.Error.Code() == kafka.ErrTopicAlreadyExists {
			fmt.Printf("  [Info] Tópico '%s' já existe.\n", res.Topic)
		} else if res.Error.Code() != kafka.ErrNoError {
			fmt.Printf("  [Erro] Falha no tópico '%s': %v\n", res.Topic, res.Error)
		} else {
			fmt.Printf("  [Sucesso] Tópico '%s' criado (Partições: %d, Retenção: %d dias)\n", res.Topic, *particao, *retencao)
		}
	}
}
