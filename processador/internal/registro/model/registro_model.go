package registro_model

import (
	"context"
	"log"

	"github.com/IBM/sarama"

	registro_api "github.com/MateusdiSousa/go-photos/api/domain/registro"
	model "github.com/MateusdiSousa/go-photos/processador/model"
)

func SetupRegistroModel() {
	model.RegistrarExecuta("registro-upload", func(ctx context.Context, msg *sarama.ConsumerMessage, cmd *registro_api.RegistroComando) error {
		log.Printf("COMANDO LIDO COM SUCESSO: %v", cmd)
		return nil
	})

	log.Println("Setup do Registro Model finalizado.")
}
