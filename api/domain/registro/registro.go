package registro

import (
	"time"

	"github.com/google/uuid"
)

type RegistroMedia struct {
	FileId    string                 `db:"file_id" json:"file-id"`
	UserId    string                 `db:"user_id" json:"user-id"`
	Filename  string                 `db:"filename" json:"filename"`
	MediaType string                 `db:"media_type" json:"media-type"`
	Mimetype  string                 `db:"mime_type" json:"mime-type"`
	Size      int64                  `db:"file_size" json:"size"`
	Bucket    string                 `db:"bucket" json:"bucket"`
	Metadata  map[string]interface{} `db:"metadata" json:"metadata"`
	CreatedAt time.Time              `db:"created_at" json:"created-at"`

	// db:"-" avisa ao pgx para ignorar, já que essa coluna não existe fisicamente na tabela
	FilePath string `db:"-" json:"filepath"`
}

type RegistroEvent struct {
	EventId   string        `json:"event-id"`
	Cadastro  RegistroMedia `json:"cadastro"`
	UserId    string        `json:"user-id"`
	CreatedAt time.Time     `json:"created-at"`
}

func NewRegistroEvent(cadastro RegistroMedia, userId string) *RegistroEvent {
	return &RegistroEvent{
		Cadastro:  cadastro,
		CreatedAt: time.Now(),
		UserId:    userId,
		EventId:   uuid.NewString(),
	}

}
