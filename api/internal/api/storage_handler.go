package api

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"sync"
	"time"

	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	storagev1 "github.com/MateusdiSousa/go-photos/api/internal/proto"
	"github.com/MateusdiSousa/go-photos/api/internal/service"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StorageHandler struct {
	kafkaProducer *kafka.Producer
	mediaService  *service.MediaService
	storageClient *minio.Client
	storagev1.UnimplementedStorageServiceServer
}

var (
	REGISTRO_CMD = "registro.comando"
	CMD_UPLOAD   = "registro-upload"
	EXPIRY_TIME  = time.Hour * 2
	BUCKET_TEMP  = "temp-media"
)

func NewStorageHandler(clientMinio *minio.Client, kafkaProducer *kafka.Producer, mediaService *service.MediaService) *StorageHandler {
	return &StorageHandler{
		kafkaProducer: kafkaProducer,
		storageClient: clientMinio,
		mediaService:  mediaService}
}

func RegistroMediaToMediaInfo(registros []*registro.RegistroMedia) []*storagev1.MediaInfo {
	quantidadeRegistro := len(registros)
	mediaInfo := make([]*storagev1.MediaInfo, quantidadeRegistro)
	for index, registro := range registros {
		mediaInfo[index] = &storagev1.MediaInfo{
			Filename:  registro.Filename,
			MediaType: registro.MediaType,
			CreatedAt: registro.CreatedAt.String(),
			Metadata:  "",
			Url:       registro.FilePath,
		}
	}

	return mediaInfo
}

func (server *StorageHandler) GetMedia(ctx context.Context, request *storagev1.GetMediaRequest) (*storagev1.GetMediaResponse, error) {
	medias, err := server.mediaService.GetMediaPaged(ctx, request.UserId, int(request.PageSize), int(request.PageNum))
	if err != nil {
		log.Printf("Falha ao encontrar fotos do usuário: %s", err)
		return nil, status.Error(codes.Internal, "Falha ao encontrar fotos do usuário.")
	}
	mediaInfo := RegistroMediaToMediaInfo(medias)
	return &storagev1.GetMediaResponse{
		MediaUrls: mediaInfo}, nil
}

func (server *StorageHandler) Upload(stream storagev1.StorageService_UploadServer) error {
	var once = sync.Once{}

	pipeReader, pipeWriter := io.Pipe()

	uuidFile := uuid.NewString()

	errChan := make(chan error, 1)

	var novoCmd *registro.RegistroComando = nil

	go server.mediaService.WriteObjectOnS3(stream.Context(), BUCKET_TEMP, uuidFile, pipeReader, errChan)

	var size int64

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			pipeWriter.Close()
			break // FIM DA STREAM DE DADOS
		}

		if err != nil {
			pipeWriter.CloseWithError(err)
			log.Printf("Erro ao receber chunk: %s", err)
			break
		}

		once.Do(func() {
			novoCmd = registro.NewRegistroComando(registro.RegistroMedia{
				UserId:    req.UserId,
				FileId:    uuidFile,
				Filename:  req.Filename,
				MediaType: req.MediaType,
				Mimetype:  req.Mimetype,
				Bucket:    BUCKET_TEMP,
				Size:      req.Size}, req.UserId, CMD_UPLOAD)
		})

		n, err := pipeWriter.Write(req.Chunks)
		if err != nil {
			log.Printf("Falha ao escrever bytes no buffer: %s", err)
		}

		log.Printf("Quantidade de bytes escritos: %d bytes", n)
		size += int64(n)
	}

	if err := <-errChan; err != nil {
		log.Printf("Falha fazer upload do arquivo para o servico S3: %s", err)
		return err
	}

	log.Printf("Arquivo salvo com sucesso! Nome: %s , Tamanho: (%d bytes)", uuidFile, size)

	comando, err := json.Marshal(*novoCmd)
	if err != nil {
		log.Printf("Falha ao gerar mensagem de evento: %s", err)
	}

	server.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &REGISTRO_CMD, Partition: kafka.PartitionAny},
		Key:            []byte(uuidFile),
		Value:          comando}, nil)

	return stream.SendAndClose(&storagev1.UploadResponse{
		CmdId: novoCmd.CmdId,
		Size:  int64(size)})
}
