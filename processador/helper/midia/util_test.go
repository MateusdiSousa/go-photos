package helper_media_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"os/exec"
	"testing"
	"time"

	"github.com/chai2010/webp"

	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	helper_media "github.com/MateusdiSousa/go-photos/processador/helper/midia" // Ajuste a importação para o caminho correto do seu pacote
)

// --- HELPERS PARA GERAÇÃO DE IMAGENS EM MEMÓRIA ---

func createDummyImage(w, h int) image.Image {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for x := 0; x < w; x++ {
		for y := 0; y < h; y++ {
			img.Set(x, y, color.RGBA{R: 100, G: 150, B: 200, A: 255})
		}
	}
	return img
}

func createJPEGBytes(t *testing.T, w, h int) []byte {
	t.Helper()
	img := createDummyImage(w, h)
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, nil); err != nil {
		t.Fatalf("falha ao codificar JPEG dummy: %v", err)
	}
	return buf.Bytes()
}

func createPNGBytes(t *testing.T, w, h int) []byte {
	t.Helper()
	img := createDummyImage(w, h)
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("falha ao codificar PNG dummy: %v", err)
	}
	return buf.Bytes()
}

func createWebPBytes(t *testing.T, w, h int) []byte {
	t.Helper()
	img := createDummyImage(w, h)
	var buf bytes.Buffer
	err := webp.Encode(&buf, img, &webp.Options{Lossless: true})
	if err != nil {
		t.Fatalf("falha ao codificar WebP dummy: %v", err)
	}
	return buf.Bytes()
}

// --- TESTES UNITÁRIOS ---

func TestGerarHashSHA256Imagem(t *testing.T) {
	data := []byte("conteudo-de-teste-para-hash")
	reader := bytes.NewReader(data)

	hashBytes, err := helper_media.GerarHashSHA256Imagem(reader)
	if err != nil {
		t.Fatalf("esperava erro nil, obteve: %v", err)
	}

	hashString := hex.EncodeToString(hashBytes)

	// Hash SHA256 conhecido de "conteudo-de-teste-para-hash"
	expectedHasher := sha256.New()
	expectedHasher.Write(data)
	expectedHashString := hex.EncodeToString(expectedHasher.Sum(nil))

	if hashString != expectedHashString {
		t.Errorf("hash incorreto. Esperado: %s, Obteve: %s", expectedHashString, hashString)
	}
}

func TestDetectImageFormat(t *testing.T) {
	tests := []struct {
		name           string
		data           []byte
		expectedFormat helper_media.ImageFormat
		expectErr      bool
	}{
		{
			name:           "JPEG Valido",
			data:           []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10},
			expectedFormat: helper_media.FormatJPEG,
			expectErr:      false,
		},
		{
			name:           "PNG Valido",
			data:           []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
			expectedFormat: helper_media.FormatPNG,
			expectErr:      false,
		},
		{
			name:           "WebP Valido",
			data:           []byte{'R', 'I', 'F', 'F', 0x00, 0x00, 0x00, 0x00, 'W', 'E', 'B', 'P'},
			expectedFormat: helper_media.FormatWebP,
			expectErr:      false,
		},
		{
			name:           "HEIC Valido",
			data:           []byte{0x00, 0x00, 0x00, 0x20, 'f', 't', 'y', 'p', 'h', 'e', 'i', 'c'},
			expectedFormat: helper_media.FormatHEIC,
			expectErr:      false,
		},
		{
			name:      "Formato Desconhecido",
			data:      []byte("dados_aleatorios_sem_magic_bytes"),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bytes.NewReader(tt.data)
			format, err := helper_media.DetectImageFormat(reader)

			if tt.expectErr {
				if err == nil {
					t.Errorf("esperava erro, mas obteve nil")
				}
			} else {
				if err != nil {
					t.Fatalf("erro inesperado: %v", err)
				}
				if format != tt.expectedFormat {
					t.Errorf("esperado: %s, obteve: %s", tt.expectedFormat, format)
				}
			}
		})
	}
}

func TestGenerateThumbnailForJPEG(t *testing.T) {
	jpegData := createJPEGBytes(t, 800, 600)
	reader := bytes.NewReader(jpegData)

	thumbBytes, err := helper_media.GenerateThumbnailForJPEG(reader)
	if err != nil {
		t.Fatalf("erro ao gerar thumbnail JPEG: %v", err)
	}

	if len(thumbBytes) == 0 {
		t.Error("thumbnail gerada está vazia")
	}

	// Valida se o retorno é um WebP decodificável
	_, err = webp.DecodeConfig(bytes.NewReader(thumbBytes))
	if err != nil {
		t.Errorf("a thumbnail gerada nao é um WebP valido: %v", err)
	}
}

func TestGenerateThumbnailForPNG(t *testing.T) {
	pngData := createPNGBytes(t, 500, 500)
	reader := bytes.NewReader(pngData)

	thumbBytes, err := helper_media.GenerateThumbnailForPNG(reader)
	if err != nil {
		t.Fatalf("erro ao gerar thumbnail PNG: %v", err)
	}

	if len(thumbBytes) == 0 {
		t.Error("thumbnail gerada está vazia")
	}

	_, err = webp.DecodeConfig(bytes.NewReader(thumbBytes))
	if err != nil {
		t.Errorf("a thumbnail gerada nao é um WebP valido: %v", err)
	}
}

func TestGenerateThumbnailForWebp(t *testing.T) {
	webpData := createWebPBytes(t, 200, 200) // Testando imagem < 300px
	reader := bytes.NewReader(webpData)

	thumbBytes, err := helper_media.GenerateThumbnailForWebp(reader)
	if err != nil {
		t.Fatalf("erro ao gerar thumbnail WebP: %v", err)
	}

	if len(thumbBytes) == 0 {
		t.Error("thumbnail gerada está vazia")
	}
}

func TestGenerateThumbnailGeneric(t *testing.T) {
	pngData := createPNGBytes(t, 400, 400)

	t.Run("Com Reader Nil", func(t *testing.T) {
		_, err := helper_media.GenerateThumbnailGeneric(nil, helper_media.FormatPNG)
		if err == nil {
			t.Error("esperava erro ao passar reader nil")
		}
	})

	t.Run("Formato Nao Suportado", func(t *testing.T) {
		_, err := helper_media.GenerateThumbnailGeneric(bytes.NewReader(pngData), helper_media.ImageFormat("gif"))
		if err == nil {
			t.Error("esperava erro para formato nao suportado")
		}
	})

	t.Run("Sucesso com PNG", func(t *testing.T) {
		thumb, err := helper_media.GenerateThumbnailGeneric(bytes.NewReader(pngData), helper_media.FormatPNG)
		if err != nil {
			t.Fatalf("erro inesperado: %v", err)
		}
		if len(thumb) == 0 {
			t.Error("thumbnail retornou vazia")
		}
	})
}

func TestExtrairMetadadosImagem(t *testing.T) {
	t.Run("Mimetype Invalido", func(t *testing.T) {
		dummyData := bytes.NewReader([]byte("dados"))
		mediaInfo := registro.RegistroMedia{Mimetype: "application/pdf"}

		_, err := helper_media.ExtrairMetadadosImagem(dummyData, mediaInfo)
		if err == nil {
			t.Error("esperava erro para Mimetype invalido")
		}
	})

	t.Run("Processamento de WebP sem EXIF (Fallback)", func(t *testing.T) {
		webpData := createWebPBytes(t, 100, 100)
		reader := bytes.NewReader(webpData)
		mediaInfo := registro.RegistroMedia{Mimetype: "image/webp"}

		meta, err := helper_media.ExtrairMetadadosImagem(reader, mediaInfo)
		if err != nil {
			t.Fatalf("erro inesperado no WebP fallback: %v", err)
		}

		if meta.ModeloCamera != "Desconhecido" {
			t.Errorf("modelo esperado 'Desconhecido', obteve: %s", meta.ModeloCamera)
		}

		if meta.DataCriacao.IsZero() || time.Since(meta.DataCriacao) > time.Minute {
			t.Error("DataCriacao de fallback deve ser proxima ao momento atual")
		}
	})
}

func TestGerarThumbnailFromVideo(t *testing.T) {
	// Verifica se o FFmpeg está instalado no ambiente antes de rodar o teste
	_, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("FFmpeg não instalado no ambiente do teste, pulando teste de thumbnail de vídeo")
	}

	t.Run("Caminho de vídeo inexistente", func(t *testing.T) {
		_, err := helper_media.GerarThumbnailFromVideo("/caminho/invalido/video.mp4")
		if err == nil {
			t.Error("esperava erro ao tentar processar arquivo inexistente com FFmpeg")
		}
	})
}
