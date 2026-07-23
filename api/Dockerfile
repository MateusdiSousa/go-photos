# ==========================================
# Estágio 1: Geração gRPC (Buf) e Build do Binário
# ==========================================
FROM golang:1.26-alpine AS builder

# 1. Pacotes base + ferramentas C necessárias para compilar o Kafka (CGO/librdkafka)
RUN apk add --no-cache git ca-certificates build-base musl-dev

# 2. Instalando o Buf e os geradores do gRPC
RUN go install github.com/bufbuild/buf/cmd/buf@v1.34.0
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

WORKDIR /app

# 3. Baixa as dependências do Go (aproveitando o cache do Docker)
COPY go.mod go.sum ./
RUN go mod download

# 4. Copia o restante do código-fonte
COPY . .

# 5. Gera os stubs gRPC via Buf
RUN buf generate

# 6. Atualiza as dependências geradas
RUN go mod tidy

# 7. Compila o binário ativando o CGO com suporte ao Musl do Alpine
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
    -tags musl \
    -ldflags="-s -w" \
    -o /app/api-server ./cmd/server/main.go

# ==========================================
# Estágio 2: Ambiente de Execução
# ==========================================
FROM alpine:3.20 AS runner

# Certificados TLS e Fuso Horário
RUN apk add --no-cache ca-certificates tzdata

# Usuário sem privilégios de root por segurança
RUN adduser -D -g '' appuser

WORKDIR /app

# Copia apenas o binário final gerado
COPY --from=builder /app/api-server .

USER appuser

EXPOSE 50051

ENTRYPOINT ["./api-server"]