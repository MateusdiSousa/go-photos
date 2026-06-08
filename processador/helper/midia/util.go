package helper_media

import (
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/evanoberholster/imagemeta"

	"github.com/dsoprea/go-exif/v3"
	exifcommon "github.com/dsoprea/go-exif/v3/common"
	"golang.org/x/image/webp"

	"github.com/MateusdiSousa/go-photos/api/domain/registro"
)

var (
	tiposValidos_image        = []string{"image/jpg", "image/jpeg", "image/png", "image/webp", "image/heic", "image/heif"}
	SUPPORTED_TYPES_IMAGEMETA = []string{"image/jpg", "image/jpeg", "image/png", "image/heic", "image/heif"}
)

type MetadadosMidia struct {
	DataCriacao  time.Time
	ModeloCamera string
	Latitude     float64
	Longitude    float64
}

// ExtrairMetaWebP processa especificamente arquivos WebP vasculhando os blocos EXIF
func ExtrairMetaWebP(r io.ReadSeeker) (*MetadadosMidia, error) {
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("falha ao resetar ponteiro: %w", err)
	}

	// 1. Validamos se o arquivo é um WebP decodificável pelo pacote x/image
	_, err := webp.DecodeConfig(r)
	if err != nil {
		return nil, fmt.Errorf("arquivo não é um WebP válido: %w", err)
	}

	// Voltar ao início para ler os bytes do EXIF brutos
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("falha ao resetar ponteiro para leitura de bytes: %w", err)
	}

	meta := &MetadadosMidia{
		DataCriacao:  time.Now(), // Fallback padrão
		ModeloCamera: "Desconhecido",
	}

	// 2. Extraímos os bytes EXIF brutos do arquivo usando a lib dsoprea
	// Ela possui um buscador de headers que consegue varrer o arquivo WebP atrás do marker 'Exif'
	rawExif, err := exif.SearchAndExtractExifWithReader(r)
	if err != nil {
		// Se não achar bloco EXIF, o arquivo WebP foi limpo ou otimizado (comportamento aceitável)
		return meta, nil
	}

	// 3. Fazemos o Parse dos metadados extraídos
	im, err := exifcommon.NewIfdMappingWithStandard()
	if err != nil {
		return meta, nil
	}

	ti := exif.NewTagIndex()
	_, index, err := exif.Collect(im, ti, rawExif)
	if err != nil {
		return meta, nil
	}

	rootIfd := index.RootIfd

	// 4. Mapeamos as tags encontradas para a nossa estrutura estruturada
	if rootIfd != nil {
		// Puxa o modelo da câmera
		if results, err := rootIfd.FindTagWithName("Model"); err == nil && len(results) > 0 {
			if val, err := results[0].Value(); err == nil {
				meta.ModeloCamera = val.(string)
			}
		}
		// Puxa a data de criação original
		if results, err := rootIfd.FindTagWithName("DateTimeOriginal"); err == nil && len(results) > 0 {
			if val, err := results[0].Value(); err == nil {
				// Faz o parse do layout padrão EXIF: "YYYY:MM:DD HH:MM:SS"
				if dt, err := time.Parse("2006:01:02 15:04:05", val.(string)); err == nil {
					meta.DataCriacao = dt
				}
			}
		}
	}

	// 5. Mapeamos o bloco de GPS se ele existir
	if gpsIfd, err := rootIfd.GpsInfo(); err == nil {
		meta.Latitude = gpsIfd.Latitude.Degrees
		meta.Longitude = gpsIfd.Longitude.Degrees
	}

	return meta, nil
}

func ValidarMedia(r io.ReadSeeker, mediaInfo registro.RegistroMedia) (*MetadadosMidia, error) {
	if !slices.Contains(tiposValidos_image, mediaInfo.Mimetype) {
		return nil, fmt.Errorf("Media com Mimetype inválido.")
	}

	switch {
	case slices.Contains(SUPPORTED_TYPES_IMAGEMETA, mediaInfo.Mimetype):
		ex, err := imagemeta.Decode(r)
		if err != nil {
			return nil, fmt.Errorf("Não foi possível decodificar arquivo")
		}

		date := ex.OriginalDate()
		if date.IsZero() {
			date = time.Now()
		}

		return &MetadadosMidia{
			DataCriacao:  date,
			ModeloCamera: ex.CameraMake(),
			Latitude:     ex.GPS.Latitude(),
			Longitude:    ex.GPS.Longitude(),
		}, nil

	case mediaInfo.Mimetype == "image/webp":
		return ExtrairMetaWebP(r)
	default:
		return nil, nil
	}
}
