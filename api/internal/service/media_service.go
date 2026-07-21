package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"time"

	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	"github.com/MateusdiSousa/go-photos/api/internal/repository"
	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
)

const (
	expirationTime   = time.Hour * 1
	THUMBNAIL_BUCKET = "thumbnails"
)

type MediaService struct {
	repository  *repository.MediaRepository
	minioClient *minio.Client
}

func NewMediaService(repository *repository.MediaRepository, minioClient *minio.Client) *MediaService {
	return &MediaService{
		repository:  repository,
		minioClient: minioClient,
	}
}

func (s *MediaService) WriteObjectOnS3(ctx context.Context, bucket string, uuidFile string, pipeReader *io.PipeReader, errChan chan error) {
	defer pipeReader.Close()
	_, err := s.minioClient.PutObject(ctx, bucket, uuidFile, pipeReader, -1, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	errChan <- err
}

func (service *MediaService) GetMediaPaged(ctx context.Context, userId string, pageSize int, pageNumber int) ([]*registro.RegistroMedia, error) {
	rows, err := service.repository.GetMediaPaged(userId, pageSize, pageNumber)
	if err != nil {
		return nil, fmt.Errorf("Falha ao realizar query 'get_media_paged': %s", err)
	}
	defer rows.Close()

	medias, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[registro.RegistroMedia])
	if err != nil {
		return nil, fmt.Errorf("Falha ao converter ROWs para objeto: %s", err)
	}

	// Instanciamos uma única vez fora do loop para reaproveitar memória
	reqParams := make(url.Values)
	reqParams.Set("response-content-disposition", "inline")

	for _, media := range medias {
		reqParams.Set("response-content-type", "image/webp")
		objectFile, err := service.minioClient.PresignedGetObject(ctx, media.Bucket, media.HashSha256, expirationTime, reqParams)
		if err != nil {
			log.Printf("Falha ao gerar URL para arquivo: %s", media.FileId)
			continue
		}
		filepath := objectFile.String()
		media.FilePath = &filepath

		thumbnailObj, err := service.minioClient.PresignedGetObject(ctx, THUMBNAIL_BUCKET, media.HashSha256, expirationTime, reqParams)
		if err == nil {
			thumbnailUrl := thumbnailObj.String()
			media.ThumbnailPath = &thumbnailUrl
		}
	}

	log.Printf("Quantidade de medias encontradas: %v", len(medias))

	return medias, nil
}
