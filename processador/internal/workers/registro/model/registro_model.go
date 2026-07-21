package registro_model

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"slices"
	"time"

	"github.com/IBM/sarama"

	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	registro_api "github.com/MateusdiSousa/go-photos/api/domain/registro"
	dispatcher "github.com/MateusdiSousa/go-photos/processador/dispatcher"
	helper_media "github.com/MateusdiSousa/go-photos/processador/helper/midia"
	"github.com/MateusdiSousa/go-photos/processador/internal/workers/registro/repository"
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
	topico := msg.Topic
	chave := string(msg.Key)
	mimetype := cmd.Cadastro.Mimetype

	switch {
	case slices.Contains(helper_media.SUPPORTED_TYPE_IMAGE, mimetype):
		return service.processarFoto(ctx, topico, chave, cmd)

	case slices.Contains(helper_media.SUPPORTED_TYPE_VIDEO, mimetype):
		return service.processarVideo(ctx, topico, chave, cmd)

	default:
		return nil, nil
	}
}

func (service *RegistroService) processarFoto(ctx context.Context, topico, chave string, cmd *registro_api.Comando[registro_api.RegistroMedia]) ([]dispatcher.MensagemKafka, []dispatcher.MensagemKafka) {
	bucket := cmd.Cadastro.Bucket
	filedId := cmd.Cadastro.FileId

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
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
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

	evento, _ := json.Marshal(registro_api.NewEvent(cmd.Cadastro, cmd.UserId, cmd.Cadastro.FileId, cmd.TipoCmd))
	cmdExecutado, _ := json.Marshal(cmd)

	return []dispatcher.MensagemKafka{
		{Topic: topico, Key: chave, Mensagem: cmdExecutado},
		{Topic: REGISTRO_MEDIA, Key: chave, Mensagem: evento},
	}, nil
}

func (service *RegistroService) processarVideo(ctx context.Context, topico, chave string, cmd *registro_api.Comando[registro_api.RegistroMedia]) ([]dispatcher.MensagemKafka, []dispatcher.MensagemKafka) {
	filedId := cmd.Cadastro.FileId
	bucketName := cmd.Cadastro.Bucket
	mimetype := cmd.Cadastro.Mimetype

	log.Printf("MIMETYPE DO VIDEO: %s", mimetype)

	// PEGANDO VIDEO DO BUCKET TEMPORARIO
	tempVideo, err := storage.GetTempMedia(ctx, bucketName, filedId)
	if err != nil {
		return nil, []dispatcher.MensagemKafka{
			dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
		}
	}
	// Fecha o leitor temporário ao sair da função caso ele seja um Closer
	// if closer, ok := tempVideo.(io.Closer); ok {
	// 	defer closer.Close()
	// }

	// GERAR HASHSHA256 do vídeo (via streaming, sem carregar na RAM)
	if _, err := tempVideo.Seek(0, io.SeekStart); err != nil {
		return nil, []dispatcher.MensagemKafka{
			dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
		}
	}

	hasher := sha256.New()
	if _, err := io.Copy(hasher, tempVideo); err != nil {
		return nil, []dispatcher.MensagemKafka{
			dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
		}
	}
	hashVideoS := hex.EncodeToString(hasher.Sum(nil))

	// VERIFICA SE O VIDEO JÁ EXISTE
	existe, err := storage.PhotoExiste(ctx, hashVideoS)
	if err != nil {
		return nil, []dispatcher.MensagemKafka{
			dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
		}
	}

	if !existe {
		// 1. Criar arquivo temporário no diretório /tmp da OS
		if _, err := tempVideo.Seek(0, io.SeekStart); err != nil {
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}

		tempFile, err := os.CreateTemp("", fmt.Sprintf("video-%s-*.mp4", hashVideoS))
		if err != nil {
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, fmt.Errorf("Falha ao criar arquivo temporário: %w", err)),
			}
		}
		defer os.Remove(tempFile.Name())
		defer tempFile.Close()

		// 2. Copia o stream diretamente para o disco (Zero consumo excessivo de RAM)
		if _, err := io.Copy(tempFile, tempVideo); err != nil {
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, fmt.Errorf("Falha ao escrever vídeo no disco: %w", err)),
			}
		}

		// 3. Gerar thumbnail com FFmpeg a partir do arquivo temporário
		thumbnailBytes, err := helper_media.GerarThumbnailFromVideo(tempFile.Name())
		if err != nil {
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}

		// 4. Salvando no bucket de photos e thumbnail
		fileInfo, err := tempFile.Stat()
		if err != nil {
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}

		// Reset do ponteiro do arquivo temporário em disco para o upload
		if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}

		err = storage.AddPhotoBucketPhotos(ctx, hashVideoS, fileInfo.Size(), tempFile)
		if err != nil {
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}

		thumbnailReader := bytes.NewBuffer(thumbnailBytes)
		err = storage.AddThumbnail(ctx, hashVideoS, int64(len(thumbnailBytes)), thumbnailReader)
		if err != nil {
			return nil, []dispatcher.MensagemKafka{
				dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err),
			}
		}
	}

	cmd.Cadastro.Bucket = "photos"
	cmd.Status = "executado"
	cmd.Cadastro.HashSha256 = hashVideoS
	cmd.Cadastro.Metadata = map[string]any{
		"data-criacao": time.Now().Format(time.RFC3339),
	}

	evento, _ := json.Marshal(registro_api.NewEvent(cmd.Cadastro, cmd.UserId, cmd.Cadastro.FileId, cmd.TipoCmd))
	cmdExecutado, _ := json.Marshal(cmd)

	return []dispatcher.MensagemKafka{
		{Topic: topico, Key: chave, Mensagem: cmdExecutado},
		{Topic: REGISTRO_MEDIA, Key: chave, Mensagem: evento},
	}, nil
}

func GerarThumbnailFromVideo(path string) ([]byte, error) {
	var ffmpegOutput bytes.Buffer
	var ffmpegStderr bytes.Buffer

	comandoFfmpeg := exec.Command("ffmpeg",
		"-ss", "00:00:01",
		"-i", path,
		"-vframes", "1",
		"-vf", "scale=320:-1",
		"-f", "image2",
		"-vcodec", "libwebp",
		"pipe:1",
	)

	comandoFfmpeg.Stdout = &ffmpegOutput
	comandoFfmpeg.Stderr = &ffmpegStderr // Captura mensagens de erro do FFmpeg

	if err := comandoFfmpeg.Run(); err != nil {
		return nil, fmt.Errorf("falha ao gerar thumbnail do vídeo: %w | detalhes ffmpeg: %s", err, ffmpegStderr.String())
	}

	return ffmpegOutput.Bytes(), nil
}

func (service *RegistroService) deleteRegistro(ctx context.Context, msg *sarama.ConsumerMessage, cmd *registro_api.Comando[map[string]string]) ([]dispatcher.MensagemKafka, []dispatcher.MensagemKafka) {
	topico := msg.Topic
	chave := string(msg.Key)

	media, err := service.repository.FindRegistroByFileIdAndUserId(ctx, cmd.Cadastro["file-id"], cmd.Cadastro["user-id"])
	if err != nil {
		return nil, []dispatcher.MensagemKafka{dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err)}
	}

	err = service.repository.DeleteRegistroByFileIdAndUserId(ctx, cmd.Cadastro["file-id"], cmd.Cadastro["user-id"])
	if err != nil {
		return nil, []dispatcher.MensagemKafka{dispatcher.NewMensagemKafkaRejeitada(cmd, topico, chave, err)}
	}

	cmd.Status = "executado"

	evento, _ := json.Marshal(registro.NewEvent(media, cmd.UserId, cmd.Cadastro["file-id"], cmd.TipoCmd))
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
