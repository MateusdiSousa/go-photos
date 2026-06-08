package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	ENDPOINT                             = "localhost:3900"
	clientTempPhotos       *minio.Client = nil
	clientBucketPhotos     *minio.Client = nil
	clientBucketThumbnails *minio.Client = nil
	TEMP_PHOTOS                          = "temp-photos"
	BUCKET_PHOTOS                        = "photos"
	BUCKET_THUMBNAILS                    = "thumbnails"
)

func getClientMinio(bucket string) (*minio.Client, error) {
	var err error = nil
	err = godotenv.Load()
	if err != nil {
		log.Printf("Falha ao carregar arquivo .env: %s", err)
	}

	switch bucket {
	case TEMP_PHOTOS:
		return minio.New(ENDPOINT, &minio.Options{
			Secure: false,
			Creds:  credentials.NewStaticV4(os.Getenv("ID_KEY_TEMP_KEY"), os.Getenv("SECRET_KEY_TEMP_KEY"), ""),
		})

	case BUCKET_PHOTOS:
		return minio.New(ENDPOINT, &minio.Options{
			Secure: false,
			Creds:  credentials.NewStaticV4(os.Getenv("ID_KEY_PHOTOS_KEY"), os.Getenv("SECRET_KEY_PHOTOS_KEY"), ""),
		})
	case BUCKET_THUMBNAILS:
		return minio.New(ENDPOINT, &minio.Options{
			Secure: false,
			Creds:  credentials.NewStaticV4(os.Getenv("ID_KEY_THUMBNAILS_KEY"), os.Getenv("SECRET_KEY_THUMBNAILS_KEY"), ""),
		})
	default:
		return nil, fmt.Errorf("BUCKET INVÁLIDO.")
	}
}

func getClientTempPhotos() (*minio.Client, error) {
	var err error = nil

	if clientTempPhotos == nil {
		clientTempPhotos, err = getClientMinio(TEMP_PHOTOS)
	}

	if err != nil {
		log.Printf("Falha ao conectar com bucket temp_photo: %s", err)
		return nil, fmt.Errorf("Falha ao conectar com servidor S3.")
	}

	return clientTempPhotos, nil
}

func getClientBucketPhotos() (*minio.Client, error) {
	var err error = nil

	if clientBucketPhotos == nil {
		clientBucketPhotos, err = getClientMinio(BUCKET_PHOTOS)
	}

	if err != nil {
		log.Printf("Falha ao conectar com bucket photos: %s", err)
		return nil, fmt.Errorf("Falha ao conectar com servidor S3.")
	}

	return clientBucketPhotos, nil
}

func getClientBucketThumbnail() (*minio.Client, error) {
	var err error = nil

	if clientBucketThumbnails == nil {
		clientBucketThumbnails, err = getClientMinio(BUCKET_THUMBNAILS)
	}

	if err != nil {
		log.Printf("Falha ao conectar com bucket thumbnails: %s", err)
		return nil, fmt.Errorf("Falha ao conectar com servidor S3.")
	}

	return clientBucketThumbnails, nil
}

func AddPhotoBucketPhotos(ctx context.Context, media registro.RegistroMedia, size int64, r io.Reader) error {
	client, err := getClientBucketPhotos()
	if err != nil {
		return fmt.Errorf("Falha ao criar cliente S3: %s", err)
	}

	_, err = client.PutObject(ctx, "photos", media.FileId, r, size, minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	return nil
}

func AddThumbnail(ctx context.Context, media registro.RegistroMedia, size int64, r io.Reader) error {
	client, err := getClientBucketThumbnail()
	if err != nil {
		return fmt.Errorf("Falha ao criar cliente S3: %s", err)
	}

	_, err = client.PutObject(ctx, "thumbnails", media.FileId, r, size, minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	return nil
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
