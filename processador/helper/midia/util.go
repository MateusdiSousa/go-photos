package helper_media

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"log"
	"slices"
	"time"

	"github.com/chai2010/webp"
	"github.com/disintegration/imaging"
	"github.com/evanoberholster/imagemeta"

	"github.com/dsoprea/go-exif/v3"
	exifcommon "github.com/dsoprea/go-exif/v3/common"

	"github.com/rwcarlsen/goexif/exif"

	"github.com/photoprism/photoprism/pkg/media/heif"

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

func ExtrairMetadadosImagem(r io.ReadSeeker, mediaInfo registro.RegistroMedia) (*MetadadosMidia, error) {
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

func GerarHashSHA256Imagem(r io.Reader) ([]byte, error) {
	h := sha256.New()

	if _, err := io.Copy(h, r); err != nil {
		log.Printf("Falha ao gerar hash sha256 do arquivo: %s", err)
		return nil, fmt.Errorf("Falha ao gerar hash do arquivo.")
	}

	return h.Sum(nil), nil
}

// ImageFormat represents supported image formats
type ImageFormat string

const (
	FormatJPEG ImageFormat = "jpeg"
	FormatJPG  ImageFormat = "jpg"
	FormatPNG  ImageFormat = "png"
	FormatHEIC ImageFormat = "heic"
	FormatHEIF ImageFormat = "heif"
	FormatWebP ImageFormat = "webp"
)

// GenerateThumbnailGeneric is the generic function that receives io.Reader and format
// and calls the appropriate specific function
func GenerateThumbnailGeneric(r io.Reader, format ImageFormat) ([]byte, error) {
	if r == nil {
		return nil, errors.New("reader is nil")
	}

	switch format {
	case FormatJPEG, FormatJPG:
		return GenerateThumbnailForJPEG(r)
	case FormatPNG:
		return GenerateThumbnailForPNG(r)
	case FormatHEIC, FormatHEIF:
		return GenerateThumbnailForHEIC(r)
	case FormatWebP:
		return GenerateThumbnailForWebp(r)
	default:
		return nil, fmt.Errorf("Formato de imagem não suportado: %s", format)
	}
}

// GenerateThumbnailForJPEG creates a WebP thumbnail from a JPEG image
func GenerateThumbnailForJPEG(r io.Reader) ([]byte, error) {
	// Read data for EXIF processing
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read JPEG data: %w", err)
	}

	// Decode JPEG
	img, err := jpeg.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to decode JPEG: %w", err)
	}

	// Generate thumbnail
	return encodeToWebPThumbnail(img)
}

// GenerateThumbnailForPNG creates a WebP thumbnail from a PNG image
func GenerateThumbnailForPNG(r io.Reader) ([]byte, error) {
	// Decode PNG
	img, err := png.Decode(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode PNG: %w", err)
	}

	// Generate thumbnail
	return encodeToWebPThumbnail(img)
}

// GenerateThumbnailForHEIC creates a WebP thumbnail from a HEIC/HEIF image
func GenerateThumbnailForHEIC(r io.Reader) ([]byte, error) {
	// Read data for HEIC decoding
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read HEIC data: %w", err)
	}

	// Decode HEIC/HEIF
	img, err := heif.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to decode HEIC/HEIF: %w", err)
	}

	if img == nil {
		return nil, errors.New("failed to decode HEIC/HEIF image")
	}

	// Generate thumbnail
	return encodeToWebPThumbnail(img)
}

// GenerateThumbnailForWebp creates a WebP thumbnail from a WebP image
func GenerateThumbnailForWebp(r io.Reader) ([]byte, error) {
	// Read data for WebP decoding
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read WebP data: %w", err)
	}

	// Decode WebP
	img, err := webp.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to decode WebP: %w", err)
	}

	if img == nil {
		return nil, errors.New("failed to decode WebP image")
	}

	// Generate thumbnail (using same function)
	return encodeToWebPThumbnail(img)
}

// encodeToWebPThumbnail resizes an image to thumbnail size and encodes to WebP
func encodeToWebPThumbnail(img image.Image) ([]byte, error) {
	if img == nil {
		return nil, errors.New("image is nil")
	}

	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()

	var thumbnail image.Image

	// If image is already small enough, use original
	if width <= 300 && height <= 300 {
		thumbnail = img
	} else {
		// Resize preserving aspect ratio, max dimension 300px
		if width > height {
			thumbnail = imaging.Resize(img, 300, 0, imaging.Lanczos)
		} else {
			thumbnail = imaging.Resize(img, 0, 300, imaging.Lanczos)
		}
	}

	// Encode to WebP
	var buf bytes.Buffer
	err := webp.Encode(&buf, thumbnail, &webp.Options{
		Lossless: false,
		Quality:  80,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to encode WebP thumbnail: %w", err)
	}

	return buf.Bytes(), nil
}

// Helper function to get format from file extension or MIME type
func DetectImageFormat(r io.Reader) (ImageFormat, error) {
	// Read first 512 bytes for detection
	buf := make([]byte, 512)
	n, err := r.Read(buf)
	if err != nil && err != io.EOF {
		return "", err
	}

	// Create a new reader that includes the buffered data
	// Note: In production, you'd need to wrap the reader

	// Detect by magic bytes
	if n >= 2 {
		// JPEG
		if buf[0] == 0xFF && buf[1] == 0xD8 {
			return FormatJPEG, nil
		}
		// PNG
		if n >= 8 && buf[0] == 0x89 && buf[1] == 0x50 && buf[2] == 0x4E && buf[3] == 0x47 {
			return FormatPNG, nil
		}
		// WebP
		if n >= 12 && buf[0] == 0x52 && buf[1] == 0x49 && buf[2] == 0x46 && buf[3] == 0x46 &&
			buf[8] == 0x57 && buf[9] == 0x45 && buf[10] == 0x42 && buf[11] == 0x50 {
			return FormatWebP, nil
		}
	}

	// HEIC/HEIF detection (ftyp box)
	if n >= 12 && buf[4] == 0x66 && buf[5] == 0x74 && buf[6] == 0x79 && buf[7] == 0x70 {
		majorBrand := string(buf[8:12])
		if majorBrand == "heic" || majorBrand == "heix" || majorBrand == "hevc" ||
			majorBrand == "heim" || majorBrand == "heis" || majorBrand == "mif1" {
			return FormatHEIC, nil
		}
	}

	return "", fmt.Errorf("could not detect image format")
}
