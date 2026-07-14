package storage

import (
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	once        sync.Once
	clientMinio *minio.Client = nil
	ENDPOINT                  = "localhost:3900"
)

func GetClientMinio() (*minio.Client, error) {
	var err error = nil

	once.Do(func() {
		log.Print("Carregando variáveis de ambiente...")

		err = godotenv.Load()
		if err != nil {
			log.Printf("Falha ao carregar arquivo .env")
		}

		log.Print("Conectando ao cliente S3...")

		clientMinio, err = minio.New(ENDPOINT, &minio.Options{
			Secure: false,
			Creds:  credentials.NewStaticV4(os.Getenv("ID_KEY_CONSULTA_KEY"), os.Getenv("SECRET_KEY_CONSULTA_KEY"), ""),
		})
		if err != nil {
			log.Printf("Falha ao conectar com o servidor S3: %s", err)
		}
	})

	if err != nil {
		return nil, err
	}
	return clientMinio, nil
}
