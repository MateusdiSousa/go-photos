package interceptor

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

type contextKey string

const UserIDKey contextKey = "user-id"

// wrappedStream serve para podermos sobrescrever o Context() em gRPC Streams
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

func ServerInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	// Validação de usuário antes da chamada.

	userID, err := ExtractUserID(ctx)
	if err != nil {
		log.Printf("Usuário não autorizado: %s", err)
		return nil, grpc.Errorf(codes.PermissionDenied, "Usuário não autorizado.")
	}

	newCtx := context.WithValue(ctx, UserIDKey, userID)
	// ADICIONAR VALIDAÇÃO DO USER-ID / TOKEN NO FUTURO

	resp, err := handler(newCtx, req)

	log.Printf("Chamada RPC method %s; Duration: %s, Error: %v", info.FullMethod, time.Since(start), err)

	return resp, err
}

func StreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	userID, err := ExtractUserID(stream.Context())
	if err != nil {
		return err
	}

	log.Printf("USER ID: %s", userID)

	newCtx := context.WithValue(stream.Context(), UserIDKey, userID)

	wrapped := &wrappedStream{
		ServerStream: stream,
		ctx:          newCtx,
	}

	return handler(srv, wrapped)
}

func ExtractUserID(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("Sem metadados na chamada")
	}

	userID := md["user-id"]
	if len(userID) == 0 {
		return "", fmt.Errorf("Requisição não possui user id")
	}

	// PROCESSAMENTO DO TOKEN OU DO ID DO USUÁRIO FUTURAMENTE

	return userID[0], nil
}
