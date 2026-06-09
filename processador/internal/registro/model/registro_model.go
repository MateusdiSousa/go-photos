package registro_model

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"

	"github.com/IBM/sarama"

	registro_api "github.com/MateusdiSousa/go-photos/api/domain/registro"
	helper_media "github.com/MateusdiSousa/go-photos/processador/helper/midia"
	model "github.com/MateusdiSousa/go-photos/processador/model"
	storage "github.com/MateusdiSousa/go-photos/processador/s3"
)

const (
	REGISTRO_MEDIA = "registro.media"
)

func SetupRegistroModel() {
	model.RegistrarExecuta("registro-upload", registroUpload)

	log.Println("Setup do Registro Model finalizado.")
}

func registroUpload(ctx context.Context, msg *sarama.ConsumerMessage, cmd *registro_api.Comando[registro_api.RegistroMedia]) ([]model.MensagemKafka, []model.MensagemKafka) {
	bucket := cmd.Cadastro.Bucket
	filedId := cmd.Cadastro.FileId
	topico := msg.Topic
	chave := string(msg.Key)

	r, err := storage.GetTempMedia(ctx, bucket, filedId)
	if err != nil {
		return nil, []model.MensagemKafka{
			model.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
		}
	}

	metadados, err := helper_media.ExtrairMetadadosImagem(r, cmd.Cadastro)
	if err != nil {
		return nil, []model.MensagemKafka{
			model.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
		}
	}
	r.Seek(0, io.SeekStart)

	hashImagem, err := helper_media.GerarHashSHA256Imagem(r)
	if err != nil {
		return nil, []model.MensagemKafka{
			model.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
		}
	}
	r.Seek(0, io.SeekStart)

	hashImagemS := hex.EncodeToString(hashImagem)

	photoExiste, err := storage.PhotoExiste(ctx, hashImagemS)
	if err != nil {
		return nil, []model.MensagemKafka{
			model.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
		}
	}

	if !photoExiste {
		imageFormat, err := helper_media.DetectImageFormat(r)
		if err != nil {
			return nil, []model.MensagemKafka{
				model.NewMensagemKafkaRejeitada(cmd, topico, chave, err)}
		}
		r.Seek(0, io.SeekStart)

		thumbnail, err := helper_media.GenerateThumbnailGeneric(r, imageFormat)
		if err != nil {
			return nil, []model.MensagemKafka{
				model.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}
		r.Seek(0, io.SeekStart)

		data, err := io.ReadAll(r)
		if err != nil {
			log.Printf("Falha ao ler dados do buffer da foto.")
			ctx.Err()
			return nil, []model.MensagemKafka{
				model.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}
		rPhoto := bytes.NewReader(data)

		err = storage.AddPhotoBucketPhotos(ctx, hashImagemS, int64(len(data)), rPhoto)
		if err != nil {
			log.Printf("Falha ao adicionar imagem ao bucket de fotos: %s", err)
			return nil, []model.MensagemKafka{
				model.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}

		err = storage.AddThumbnail(ctx, hashImagemS, int64(len(thumbnail)), bytes.NewReader(thumbnail))
		if err != nil {
			log.Printf("Falha ao adicionar imagem ao bucket de thumbnails: %s", err)
			return nil, []model.MensagemKafka{
				model.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}
	}

	cmd.Cadastro.Bucket = "photos"
	cmd.Status = "executado"
	cmd.Cadastro.HashSha256 = hashImagemS
	cmd.Cadastro.Metadata = map[string]any{
		"data-criacao":  metadados.DataCriacao,
		"modelo-camera": metadados.ModeloCamera,
		"latitude":      metadados.Latitude,
		"longitude":     metadados.Longitude,
	}

	evento, _ := json.Marshal(registro_api.NewEvent(cmd.Cadastro, cmd.UserId, cmd.TipoCmd))
	cmdExecutado, _ := json.Marshal(cmd)
	return []model.MensagemKafka{
		{
			Topic:    topico,
			Key:      chave,
			Mensagem: cmdExecutado,
		},
		{
			Topic:    REGISTRO_MEDIA,
			Key:      chave,
			Mensagem: evento,
		},
	}, nil
}
