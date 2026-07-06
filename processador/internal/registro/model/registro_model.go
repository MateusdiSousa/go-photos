package registro_model

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"

	"github.com/IBM/sarama"

	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	registro_api "github.com/MateusdiSousa/go-photos/api/domain/registro"
	dispatcher "github.com/MateusdiSousa/go-photos/processador/dispatcher"
	helper_media "github.com/MateusdiSousa/go-photos/processador/helper/midia"
	"github.com/MateusdiSousa/go-photos/processador/internal/registro/repository"
	storage "github.com/MateusdiSousa/go-photos/processador/s3"
)

type RegistroService struct {
	repository repository.IRegistroRepository
}

const (
	REGISTRO_MEDIA = "registro.media"
)

func SetupRegistroModel(repository repository.IRegistroRepository) {
	registroService := &RegistroService{
		repository: repository,
	}
	dispatcher.RegistrarExecuta("registro-upload", registroService.registroUpload)
	dispatcher.RegistrarExecuta("registro-upload-executado", registroService.updateRegistro)
	dispatcher.RegistrarExecuta("registro-delete", registroService.deleteRegistro)
	log.Println("Setup do Registro Model finalizado.")
}

func (service *RegistroService) updateRegistro(ctx context.Context, msg *sarama.ConsumerMessage, cmd *registro_api.Comando[registro_api.RegistroMedia]) ([]dispatcher.MensagemKafka, []dispatcher.MensagemKafka) {
	err := service.repository.SaveRegistroMedia(ctx, cmd.Cadastro)
	if err != nil {
		return nil, []dispatcher.MensagemKafka{
			dispatcher.NewMensagemKafkaRejeitada(cmd, msg.Topic, string(msg.Key), err),
		}
	}
	return nil, nil
}

func (service *RegistroService) registroUpload(ctx context.Context, msg *sarama.ConsumerMessage, cmd *registro_api.Comando[registro_api.RegistroMedia]) ([]dispatcher.MensagemKafka, []dispatcher.MensagemKafka) {
	bucket := cmd.Cadastro.Bucket
	filedId := cmd.Cadastro.FileId
	topico := msg.Topic
	chave := string(msg.Key)

	r, err := storage.GetTempMedia(ctx, bucket, filedId)
	if err != nil {
		return nil, []dispatcher.MensagemKafka{
			dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
		}
	}

	metadados, err := helper_media.ExtrairMetadadosImagem(r, cmd.Cadastro)
	if err != nil {
		return nil, []dispatcher.MensagemKafka{
			dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
		}
	}
	r.Seek(0, io.SeekStart)

	hashImagem, err := helper_media.GerarHashSHA256Imagem(r)
	if err != nil {
		return nil, []dispatcher.MensagemKafka{
			dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
		}
	}
	r.Seek(0, io.SeekStart)

	hashImagemS := hex.EncodeToString(hashImagem)

	photoExiste, err := storage.PhotoExiste(ctx, hashImagemS)
	if err != nil {
		return nil, []dispatcher.MensagemKafka{
			dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
		}
	}

	if !photoExiste {
		imageFormat, err := helper_media.DetectImageFormat(r)
		if err != nil {
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err)}
		}
		r.Seek(0, io.SeekStart)

		thumbnail, err := helper_media.GenerateThumbnailGeneric(r, imageFormat)
		if err != nil {
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}
		r.Seek(0, io.SeekStart)

		data, err := io.ReadAll(r)
		if err != nil {
			log.Printf("Falha ao ler dados do buffer da foto.")
			ctx.Err()
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}
		rPhoto := bytes.NewReader(data)

		err = storage.AddPhotoBucketPhotos(ctx, hashImagemS, int64(len(data)), rPhoto)
		if err != nil {
			log.Printf("Falha ao adicionar imagem ao bucket de fotos: %s", err)
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}

		err = storage.AddThumbnail(ctx, hashImagemS, int64(len(thumbnail)), bytes.NewReader(thumbnail))
		if err != nil {
			log.Printf("Falha ao adicionar imagem ao bucket de thumbnails: %s", err)
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
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
	return []dispatcher.MensagemKafka{
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

func (service *RegistroService) deleteRegistro(ctx context.Context, msg *sarama.ConsumerMessage, cmd *registro_api.Comando[registro_api.RegistroMedia]) ([]dispatcher.MensagemKafka, []dispatcher.MensagemKafka) {
	topico := msg.Topic
	chave := string(msg.Key)

	media, err := service.repository.FindRegistroByFileIdAndUserId(ctx, cmd.Cadastro.FileId, cmd.Cadastro.UserId)
	if err != nil {
		return nil, []dispatcher.MensagemKafka{dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err)}
	}

	err = service.repository.DeleteRegistroByFileIdAndUserId(ctx, cmd.Cadastro.FileId, cmd.Cadastro.UserId)
	if err != nil {
		return nil, []dispatcher.MensagemKafka{dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err)}
	}

	cmd.Status = "executado"

	evento, _ := json.Marshal(registro.NewEvent(media, cmd.UserId, cmd.TipoCmd))
	cmdExecutado, _ := json.Marshal(cmd)
	return []dispatcher.MensagemKafka{
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
	// CONSULTA VAI ATUALIZAR O BANCO DE DADOS POSTGRES E O GARAGE SE NECESSARIO
}
