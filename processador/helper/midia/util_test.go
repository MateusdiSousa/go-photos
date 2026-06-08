package helper_media

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"testing"

	"github.com/MateusdiSousa/go-photos/api/domain/registro"
)

// CriarArquivoTxtBuffer gera os bytes de um TXT em memória, evitando I/O de disco pesado no teste
func CriarArquivoTxtBuffer(t *testing.T) io.ReadSeeker {
	t.Helper()
	return bytes.NewReader([]byte("Arquivo de teste"))
}

func CriarImagemJpegSimples(t *testing.T) io.ReadSeeker {
	t.Helper()
	largura, altura := 100, 100
	img := image.NewRGBA(image.Rect(0, 0, largura, altura))

	azul := color.RGBA{0, 0, 255, 255}
	for x := 0; x < largura; x++ {
		for y := 0; y < altura; y++ {
			img.Set(x, y, azul)
		}
	}

	var buf bytes.Buffer
	err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 80})
	if err != nil {
		t.Fatalf("falha fatal ao gerar imagem JPEG de teste: %s", err)
	}

	return bytes.NewReader(buf.Bytes())
}

type CaseValidarMedia struct {
	TestName          string
	fileGenerator     func() io.ReadSeeker // Mudado para função: gera um leitor zerado para cada caso
	fileInfo          registro.RegistroMedia
	resultadoEsperado error
}

func TestValidarMedia_MimeTypeInvalido(t *testing.T) {
	testCases := []CaseValidarMedia{
		{
			TestName:          "Arquivo e Mimetype invalido.",
			fileGenerator:     func() io.ReadSeeker { return CriarArquivoTxtBuffer(t) },
			fileInfo:          registro.RegistroMedia{Mimetype: "text/txt"},
			resultadoEsperado: fmt.Errorf("Media com Mimetype inválido."),
		},
		{
			TestName:          "Arquivo e Mimetype diferentes.",
			fileGenerator:     func() io.ReadSeeker { return CriarArquivoTxtBuffer(t) },
			fileInfo:          registro.RegistroMedia{Mimetype: "image/jpeg"},
			resultadoEsperado: fmt.Errorf("Não foi possível decodificar arquivo"),
		},
		{
			TestName:          "Arquivo e Mimetype diferentes (Imagem vs TXT).",
			fileGenerator:     func() io.ReadSeeker { return CriarImagemJpegSimples(t) },
			fileInfo:          registro.RegistroMedia{Mimetype: "text/txt"},
			resultadoEsperado: fmt.Errorf("Media com Mimetype inválido."),
		},
		{
			TestName:          "Caso válido.",
			fileGenerator:     func() io.ReadSeeker { return CriarImagemJpegSimples(t) },
			fileInfo:          registro.RegistroMedia{Mimetype: "image/jpeg"},
			resultadoEsperado: nil,
		},
	}

	for _, testCase := range testCases {
		// t.Run isola os sub-testes e melhora o report no terminal (ex: go test -v)
		t.Run(testCase.TestName, func(t *testing.T) {
			inputReader := testCase.fileGenerator()
			_, resultadoAtual := ValidarMedia(inputReader, testCase.fileInfo)

			// Validação robusta de erros protegida contra nils
			if (resultadoAtual == nil && testCase.resultadoEsperado != nil) ||
				(resultadoAtual != nil && testCase.resultadoEsperado == nil) ||
				(resultadoAtual != nil && testCase.resultadoEsperado != nil && resultadoAtual.Error() != testCase.resultadoEsperado.Error()) {

				t.Errorf("\nRESULTADO ESPERADO: %v\nRESULTADO ATUAL: %v", testCase.resultadoEsperado, resultadoAtual)
			}
		})
	}
}
