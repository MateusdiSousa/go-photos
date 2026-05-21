package api

import (
	"encoding/json"
	"io"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"go-photos.api/internal/domain/registro"
	storagev1 "go-photos.api/internal/proto"
)

type StorageHandler struct {
	kafkaProducer *kafka.Producer
	storageClient *minio.Client
	storagev1.UnimplementedStorageServiceServer
}

var (
	REGISTRO_MEDIA = "registro.media"
)

func NewStorageHandler(clientMinio *minio.Client, kafkaProducer *kafka.Producer) *StorageHandler {
	return &StorageHandler{storageClient: clientMinio, kafkaProducer: kafkaProducer}
}

var BUCKET_PHOTOS = "photos"

func (service *StorageHandler) Upload(stream storagev1.StorageService_UploadServer) error {
	pipeReader, pipeWriter := io.Pipe()

	uuidFile := uuid.NewString()

	errChan := make(chan error, 1)

	var novoRegistro *registro.RegistroEvent = nil

	go func() {
		defer pipeReader.Close()
		_, err := service.storageClient.PutObject(
			stream.Context(),
			BUCKET_PHOTOS,
			uuidFile,
			pipeReader,
			-1,
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		errChan <- err

	}()

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

		if novoRegistro == nil {
			novoRegistro = registro.NewRegistroEvent(registro.RegistroMedia{
				UserId:    req.UserId,
				FileId:    uuidFile,
				Filename:  req.Filename,
				MediaType: req.MediaType,
				Mimetype:  req.Mimetype,
				Metadata:  req.Metadados,
				Bucket:    BUCKET_PHOTOS,
				Size:      req.Size}, req.UserId)
		}

		n, err := pipeWriter.Write(req.Chunks)

		log.Printf("Quantidade de bytes escritos: %d bytes", n)
		size += int64(n)

	}

	if err := <-errChan; err != nil {
		log.Printf("Falha fazer upload do arquivo para o servico S3: %s", err)
		return err
	}

	log.Printf("Arquivo salvo com sucesso! Nome: %s , Tamanho: (%d bytes)", uuidFile, size)

	evento, err := json.Marshal(*novoRegistro)
	if err != nil {
		log.Printf("Falha ao gerar mensagem de evento: %s", err)
	}

	service.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &REGISTRO_MEDIA, Partition: kafka.PartitionAny},
		Key:            []byte(uuidFile),
		Value:          evento}, nil)

	return stream.SendAndClose(&storagev1.UploadResponse{
		FileId: uuidFile,
		Size:   int64(size)})
}
