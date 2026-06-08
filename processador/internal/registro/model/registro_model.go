package registro_model

import (
	"bytes"
	"context"
	"io"
	"log"

	"github.com/IBM/sarama"

	registro_api "github.com/MateusdiSousa/go-photos/api/domain/registro"
	helper_media "github.com/MateusdiSousa/go-photos/processador/helper/midia"
	model "github.com/MateusdiSousa/go-photos/processador/model"
	storage "github.com/MateusdiSousa/go-photos/processador/s3"
)

func SetupRegistroModel() {
	model.RegistrarExecuta("registro-upload",
		func(ctx context.Context, msg *sarama.ConsumerMessage, cmd *registro_api.RegistroComando) ([]model.Evento, error) {
			bucket := cmd.Cadastro.Bucket
			filedId := cmd.Cadastro.FileId

			r, err := storage.GetTempMedia(ctx, bucket, filedId)
			if err != nil {
				return nil, err
			}

			metadados, err := helper_media.ExtrairMetadadosImagem(r, cmd.Cadastro)
			if err != nil {
				return nil, err
			}
			r.Seek(0, io.SeekStart)

			hashImagem, err := helper_media.GerarHashSHA256Imagem(r)
			if err != nil {
				return nil, err
			}
			r.Seek(0, io.SeekStart)

			imageFormat, err := helper_media.DetectImageFormat(r)
			if err != nil {
				return nil, err
			}
			r.Seek(0, io.SeekStart)

			thumbnail, err := helper_media.GenerateThumbnailGeneric(r, imageFormat)
			if err != nil {
				return nil, err
			}

			r.Seek(0, io.SeekStart)

			cmd.Cadastro.HashSha256 = string(hashImagem)

			cmd.Cadastro.Metadata = map[string]interface{}{
				"data-criacao":  metadados.DataCriacao,
				"modelo-camera": metadados.ModeloCamera,
				"latitude":      metadados.Latitude,
				"longitude":     metadados.Longitude,
			}

			err = storage.AddPhotoBucketPhotos(ctx, cmd.Cadastro, r)
			if err != nil {
				return nil, err
			}

			err = storage.AddThumbnail(ctx, string(hashImagem), int64(len(thumbnail)), bytes.NewReader(thumbnail))
			if err != nil {
				return nil, err
			}

			return nil, nil
		})

	log.Println("Setup do Registro Model finalizado.")
}
