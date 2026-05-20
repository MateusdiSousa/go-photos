package storage

import (
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	clientMinio  *minio.Client = nil
	ENDPOINT                   = "localhost:3900"
	ACESS_KEY_ID               = "GK646ccdc35a5b43a4c1d63e7f"
	SECRET_KEY                 = "3c3e4403806b1bb54d66f485caabafb7baebc31181550243fc443d5a2f5cd52d"
)

func GetClientMinio() *minio.Client {
	var err error = nil
	if clientMinio == nil {
		log.Printf("Conectando ao cliente S3...")

		clientMinio, err = minio.New(ENDPOINT, &minio.Options{
			Secure: false,
			Creds:  credentials.NewStaticV4(ACESS_KEY_ID, SECRET_KEY, ""),
		})

		if err != nil {
			log.Fatalf("Falha ao conectar com o servidor S3: %s", err)
		}
		return clientMinio
	}
	return clientMinio
}
