package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

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

func objectExiste(ctx context.Context, c *minio.Client, bucketName string, filename string) (bool, error) {
	_, err := c.StatObject(ctx, bucketName, filename, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		// Status Code 404 == Not Found
		if errResponse.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("Falha ao verificar se objeto existe: %s", err)
	}
	return true, nil
}

func AddPhotoBucketPhotos(ctx context.Context, filename string, size int64, r io.Reader) error {
	client, err := getClientBucketPhotos()
	if err != nil {
		return fmt.Errorf("Falha ao criar cliente S3: %s", err)
	}

	_, err = client.PutObject(ctx, "photos", filename, r, size, minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	return nil
}

func AddThumbnail(ctx context.Context, filename string, size int64, r io.Reader) error {
	client, err := getClientBucketThumbnail()
	if err != nil {
		return fmt.Errorf("Falha ao criar cliente S3: %s", err)
	}

	_, err = client.PutObject(ctx, "thumbnails", filename, r, size, minio.PutObjectOptions{})
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

func PhotoExiste(ctx context.Context, hash string) (bool, error) {
	clientPhoto, err := getClientBucketPhotos()
	if err != nil {
		return false, fmt.Errorf("Falha ao criar cliente S3: %s", err)
	}

	exists, err := objectExiste(ctx, clientPhoto, BUCKET_PHOTOS, hash)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func ThumbnailExiste(ctx context.Context, hash string) (bool, error) {
	clientPhoto, err := getClientBucketThumbnail()
	if err != nil {
		return false, fmt.Errorf("Falha ao criar cliente S3: %s", err)
	}

	exists, err := objectExiste(ctx, clientPhoto, BUCKET_THUMBNAILS, hash)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// DeletePhotoByID deleta uma foto do bucket photos pelo ID
func DeletePhotoByID(ctx context.Context, fileID string) error {
	client, err := getClientBucketPhotos()
	if err != nil {
		return fmt.Errorf("Falha ao criar cliente S3 para photos: %s", err)
	}

	// Verifica se o objeto existe antes de deletar
	exists, err := objectExiste(ctx, client, BUCKET_PHOTOS, fileID)
	if err != nil {
		return fmt.Errorf("Falha ao verificar existência da foto: %s", err)
	}

	if !exists {
		return fmt.Errorf("Foto com ID %s não encontrada no bucket photos", fileID)
	}

	// Remove o objeto do bucket
	err = client.RemoveObject(ctx, BUCKET_PHOTOS, fileID, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("Falha ao deletar foto do bucket photos: %s", err)
	}

	log.Printf("Foto com ID %s deletada com sucesso do bucket photos", fileID)
	return nil
}

// DeleteThumbnailByID deleta uma thumbnail do bucket thumbnails pelo ID
func DeleteThumbnailByID(ctx context.Context, fileID string) error {
	client, err := getClientBucketThumbnail()
	if err != nil {
		return fmt.Errorf("Falha ao criar cliente S3 para thumbnails: %s", err)
	}

	// Verifica se o objeto existe antes de deletar
	exists, err := objectExiste(ctx, client, BUCKET_THUMBNAILS, fileID)
	if err != nil {
		return fmt.Errorf("Falha ao verificar existência da thumbnail: %s", err)
	}

	if !exists {
		return fmt.Errorf("Thumbnail com ID %s não encontrada no bucket thumbnails", fileID)
	}

	// Remove o objeto do bucket
	err = client.RemoveObject(ctx, BUCKET_THUMBNAILS, fileID, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("Falha ao deletar thumbnail do bucket thumbnails: %s", err)
	}

	log.Printf("Thumbnail com ID %s deletada com sucesso do bucket thumbnails", fileID)
	return nil
}

// DeletePhotoAndThumbnailByID deleta tanto a foto quanto a thumbnail pelo ID
func DeletePhotoAndThumbnailByID(ctx context.Context, fileID string) error {
	var errs []error

	// Tenta deletar a foto
	if err := DeletePhotoByID(ctx, fileID); err != nil {
		errs = append(errs, fmt.Errorf("erro ao deletar foto: %w", err))
	}

	// Tenta deletar a thumbnail
	if err := DeleteThumbnailByID(ctx, fileID); err != nil {
		errs = append(errs, fmt.Errorf("erro ao deletar thumbnail: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("erros ao deletar arquivos: %v", errs)
	}

	log.Printf("Foto e thumbnail com ID %s deletadas com sucesso", fileID)
	return nil
}

// DeleteMultiplePhotos deleta múltiplas fotos do bucket photos
func DeleteMultiplePhotos(ctx context.Context, fileIDs []string) error {
	client, err := getClientBucketPhotos()
	if err != nil {
		return fmt.Errorf("Falha ao criar cliente S3 para photos: %s", err)
	}

	var errors []error
	for _, fileID := range fileIDs {
		if err := client.RemoveObject(ctx, BUCKET_PHOTOS, fileID, minio.RemoveObjectOptions{}); err != nil {
			errors = append(errors, fmt.Errorf("falha ao deletar %s: %w", fileID, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("erros ao deletar múltiplas fotos: %v", errors)
	}

	log.Printf("%d fotos deletadas com sucesso do bucket photos", len(fileIDs))
	return nil
}

// DeleteMultipleThumbnails deleta múltiplas thumbnails do bucket thumbnails
func DeleteMultipleThumbnails(ctx context.Context, fileIDs []string) error {
	client, err := getClientBucketThumbnail()
	if err != nil {
		return fmt.Errorf("Falha ao criar cliente S3 para thumbnails: %s", err)
	}

	var errors []error
	for _, fileID := range fileIDs {
		if err := client.RemoveObject(ctx, BUCKET_THUMBNAILS, fileID, minio.RemoveObjectOptions{}); err != nil {
			errors = append(errors, fmt.Errorf("falha ao deletar %s: %w", fileID, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("erros ao deletar múltiplas thumbnails: %v", errors)
	}

	log.Printf("%d thumbnails deletadas com sucesso do bucket thumbnails", len(fileIDs))
	return nil
}
