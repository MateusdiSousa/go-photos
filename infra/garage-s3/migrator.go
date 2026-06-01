package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// Estrutura para armazenar temporariamente as credenciais coletadas durante a migração
type EnvVar struct {
	Key   string
	Value string
}

func main() {
	migrateDir := "./migrate"
	envFilePath := ".env"

	// 1. Ler e ordenar os arquivos da pasta migrate
	files, err := os.ReadDir(migrateDir)
	if err != nil {
		log.Fatalf("❌ Erro ao ler diretório de migrações: %v", err)
	}

	var migrateFiles []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".txt") {
			migrateFiles = append(migrateFiles, file.Name())
		}
	}
	sort.Strings(migrateFiles)

	fmt.Printf("🚀 Encontrados %d arquivos de migração para processar.\n\n", len(migrateFiles))

	var collectedEnvVars []EnvVar

	// 2. Processar cada arquivo em ordem
	for _, fileName := range migrateFiles {
		filePath := filepath.Join(migrateDir, fileName)
		fmt.Printf("📁 Executando migração: %s\n", fileName)
		fmt.Println(strings.Repeat("-", 40))

		vars, err := executeMigrationFile(filePath)
		if err != nil {
			log.Fatalf("❌ Falha na migração %s: %v", fileName, err)
		}
		collectedEnvVars = append(collectedEnvVars, vars...)
		fmt.Printf("✅ Migração %s concluída com sucesso!\n\n", fileName)
	}

	// 3. Escrever ou atualizar o arquivo .env se novas chaves foram geradas
	if len(collectedEnvVars) > 0 {
		err := writeEnvFile(envFilePath, collectedEnvVars)
		if err != nil {
			log.Fatalf("❌ Erro ao gravar o arquivo .env: %v", err)
		}
		fmt.Printf("📝 Arquivo '%s' gerado/atualizado com sucesso com as credenciais!\n", envFilePath)
	}
}

func executeMigrationFile(filePath string) ([]EnvVar, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var envVars []EnvVar
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Ignora linhas vazias ou comentários
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmdType := parts[0]
		args := parts[1:]

		vars, err := runGarageCommand(cmdType, args)
		if err != nil {
			return nil, fmt.Errorf("erro na linha '%s': %w", line, err)
		}
		envVars = append(envVars, vars...)
	}

	return envVars, scanner.Err()
}

func runGarageCommand(cmdType string, args []string) ([]EnvVar, error) {
	var garageArgs []string
	var detectedVars []EnvVar

	switch cmdType {
	case "create-bucket":
		if len(args) < 1 {
			return nil, fmt.Errorf("create-bucket exige o nome do bucket")
		}
		fmt.Printf("🪣  Criando bucket: %s\n", args[0])
		garageArgs = []string{"bucket", "create", args[0]}

	case "create-key":
		if len(args) < 1 {
			return nil, fmt.Errorf("create-key exige o nome da chave")
		}
		keyName := args[0]
		fmt.Printf("🔑 Gerando chave de acesso para: %s\n", keyName)

		out, err := execGarage([]string{"key", "create", keyName})
		if err != nil {
			if strings.Contains(err.Error(), "already exists") || strings.Contains(out, "Exists") {
				fmt.Println("⚠️  A chave já existe no cluster. Certifique-se de coletar os tokens caso precise alterá-la.")
				return nil, nil
			}
			return nil, err
		}

		// Faz o parse do output do Garage para capturar Key ID e Secret Key
		keyID, secretKey := parseGarageKeyOutput(out)
		if keyID != "" && secretKey != "" {
			// Padroniza o nome das chaves para Caixa Alta (UPPERCASE) conforme solicitado
			suffix := strings.ToUpper(strings.ReplaceAll(keyName, "-", "_"))

			detectedVars = append(detectedVars, EnvVar{Key: fmt.Sprintf("ID_KEY_%s", suffix), Value: keyID})
			detectedVars = append(detectedVars, EnvVar{Key: fmt.Sprintf("SECRET_KEY_%s", suffix), Value: secretKey})

			fmt.Printf("   📌 Coletado: ID_KEY_%s e SECRET_KEY_%s\n", suffix, suffix)
		}
		return detectedVars, nil

	case "allow-bucket":
		if len(args) < 5 {
			return nil, fmt.Errorf("allow-bucket com sintaxe inválida. Exemplo: allow-bucket photos --read --write --key photos-key")
		}
		bucket := args[0]
		keyName := args[4]

		fmt.Printf("🔐 Atribuindo permissões no bucket '%s' para a chave '%s'...\n", bucket, keyName)
		garageArgs = []string{"bucket", "allow", "--read", "--write", bucket, "--key", keyName}

	default:
		return nil, fmt.Errorf("comando desconhecido ou removido do script: %s", cmdType)
	}

	// Executa comandos que não geram variáveis de ambiente (create-bucket / allow-bucket)
	if len(garageArgs) > 0 {
		out, err := execGarage(garageArgs)
		if err != nil {
			if strings.Contains(out, "already exists") || strings.Contains(out, "Bucket already exists") {
				fmt.Println("⚠️  Recurso já existente, pulando...")
				return nil, nil
			}
			return nil, fmt.Errorf("output: %s, erro: %w", out, err)
		}
	}

	return nil, nil
}

// Executa comandos injetando o "docker exec" para se comunicar com o container
func execGarage(args []string) (string, error) {
	// Equivalente ao comando: docker exec -i garage /garage <args>
	dockerArgs := append([]string{"exec", "-i", "garage", "/garage"}, args...)
	cmd := exec.Command("docker", dockerArgs...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return stdout.String() + stderr.String(), err
	}

	return stdout.String(), nil
}

// Analisa a saída de texto do comando 'garage key create' para capturar os tokens
func parseGarageKeyOutput(output string) (keyID, secretKey string) {
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "Key ID:") {
			keyID = strings.TrimSpace(strings.TrimPrefix(line, "Key ID:"))
		} else if strings.HasPrefix(line, "Secret key:") {
			secretKey = strings.TrimSpace(strings.TrimPrefix(line, "Secret key:"))
		}
	}
	return keyID, secretKey
}

// Salva ou anexa as credenciais descobertas no arquivo .env
func writeEnvFile(filePath string, vars []EnvVar) error {
	// Abre o arquivo no modo Append (anexa no final) ou cria se não existir
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	// Adiciona uma quebra de linha inicial para separar de possíveis configurações anteriores
	writer.WriteString("\n# --- Credenciais geradas automaticamente pelo Garage Migrator ---\n")

	for _, v := range vars {
		line := fmt.Sprintf("%s=%s\n", v.Key, v.Value)
		_, err := writer.WriteString(line)
		if err != nil {
			return err
		}
	}

	return writer.Flush()
}
