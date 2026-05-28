package storage

import (
	"log"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	once         sync.Once
	clientMinio  *minio.Client = nil
	ENDPOINT                   = "localhost:3900"
	ACESS_KEY_ID               = "GK77b6d93501d4f1e0fbe00478"
	SECRET_KEY                 = "9d7d8172e57e2690e5bf52040bf644be59e95c2449d6ef573fed96108de4a030"
)

func GetClientMinio() (*minio.Client, error) {
	var err error = nil

	once.Do(func() {
		log.Printf("Conectando ao cliente S3...")

		clientMinio, err = minio.New(ENDPOINT, &minio.Options{
			Secure: false,
			Creds:  credentials.NewStaticV4(ACESS_KEY_ID, SECRET_KEY, ""),
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
