package storage

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	ENDPOINT         = "localhost:3900"
	once             sync.Once
	clientTempPhotos *minio.Client = nil
)

func getClientTempPhotos() (*minio.Client, error) {
	var err error = nil

	once.Do(func() {
		err = godotenv.Load()
		if err != nil {
			log.Printf("Falha ao carregar arquivo .env: %s", err)
		}

		log.Printf("Conectando ao client S3...")

		clientTempPhotos, err = minio.New(ENDPOINT, &minio.Options{
			Secure: false,
			Creds:  credentials.NewStaticV4(os.Getenv("ID_KEY_TEMP_KEY"), os.Getenv("SECRET_KEY_TEMP_KEY"), ""),
		})

		if err != nil {
			log.Printf("Falha ao conectar com o servidor S3: %s", err)
		}
	})

	if err != nil {
		return nil, err
	}
	return clientTempPhotos, nil
}

func GetTempMedia(ctx context.Context, bucketName string, fileId string) (*minio.Object, error) {
	client, err := getClientTempPhotos()
	if err != nil {
		return nil, fmt.Errorf("Falha ao criar cliente S3: %s", err)
	}

	media, err := client.GetObject(ctx, bucketName, fileId, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("Falha ao buscar objeto no S3: %s", err)
	}

	return media, nil
}
