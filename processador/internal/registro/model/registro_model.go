package registro_model

import (
	"context"
	"log"

	"github.com/IBM/sarama"

	registro_api "github.com/MateusdiSousa/go-photos/api/domain/registro"
	model "github.com/MateusdiSousa/go-photos/processador/model"
	storage "github.com/MateusdiSousa/go-photos/processador/s3"
)

func SetupRegistroModel() {
	model.RegistrarExecuta("registro-upload",
		func(ctx context.Context, msg *sarama.ConsumerMessage, cmd *registro_api.RegistroComando) error {
			bucket := cmd.Cadastro.Bucket
			filedId := cmd.Cadastro.FileId

			_, err := storage.GetTempMedia(ctx, bucket, filedId)
			if err != nil {
				return err
			}

			log.Println("Arquivo recuperado com sucesso!")

			return nil
		})

	log.Println("Setup do Registro Model finalizado.")
}
