package registro

import (
	"time"

	"github.com/google/uuid"
)

type RegistroMedia struct {
	FileId    string    `db:"file_id" json:"file-id"`
	UserId    string    `db:"user_id" json:"user-id"`
	Filename  string    `db:"filename" json:"filename"`
	MediaType string    `db:"media_type" json:"media-type"`
	Mimetype  string    `db:"mime_type" json:"mime-type"`
	Size      int64     `db:"file_size" json:"size"`
	Bucket    string    `db:"bucket" json:"bucket"`
	CreatedAt time.Time `db:"created_at" json:"created-at"`
	// db:"-" avisa ao pgx para ignorar, já que essa coluna não existe fisicamente na tabela
	FilePath string `db:"-" json:"filepath"`
}

type RegistroComando struct {
	CmdId     string        `json:"cmd-id"`
	Cadastro  RegistroMedia `json:"cadastro"`
	TipoCmd   string        `json:"tipo-cmd"`
	Status    string        `json:"status"`
	UserId    string        `json:"user-id"`
	CreatedAt time.Time     `json:"created-at"`
}

func NewRegistroComando(cadastro RegistroMedia, userId string, tipoComando string) *RegistroComando {
	return &RegistroComando{
		Cadastro:  cadastro,
		CreatedAt: time.Now(),
		UserId:    userId,
		CmdId:     uuid.NewString(),
		TipoCmd:   tipoComando,
		Status:    "pendente",
	}
}
