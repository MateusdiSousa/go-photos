package registro

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type JSONB map[string]interface{}

func (j JSONB) Value() (driver.Value, error) {
	return json.Marshal(j)
}

func (j *JSONB) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(b, j)
}

type RegistroMedia struct {
	FileId        string    `db:"file_id" json:"file-id"`
	UserId        string    `db:"user_id" json:"user-id"`
	Filename      string    `db:"filename" json:"filename"`
	MediaType     string    `db:"media_type" json:"media-type"`
	Mimetype      string    `db:"mime_type" json:"mime-type"`
	Metadata      JSONB     `db:"metadata" json:"metadata"`
	Size          int64     `db:"file_size" json:"size"`
	Bucket        string    `db:"bucket" json:"bucket"`
	CreatedAt     time.Time `db:"created_at" json:"created-at"`
	HashSha256    string    `db:"hash_sha256" json:"hash-sha256"`
	ThumbnailPath *string   `db:"thumbnail_path" json:"thumbnail-path"`
	FilePath      *string   `db:"file_path" json:"file-path"`
}

type Evento[T any] struct {
	EventId   string    `json:"event-id"`
	EventType string    `json:"event-type"`
	UserId    string    `json:"user-id"`
	Dados     T         `json:"dados"`
	CreatedAt time.Time `json:"created-at"`
}

type Comando[T any] struct {
	CmdId     string    `json:"cmd-id"`
	Cadastro  T         `json:"cadastro"`
	TipoCmd   string    `json:"tipo-cmd"`
	Status    string    `json:"status"`
	UserId    string    `json:"user-id"`
	CreatedAt time.Time `json:"created-at"`
	Erros     []string  `json:"erros"`
}

func NewEvent[T any](dados T, userId string, tipoEvento string) *Evento[T] {
	return &Evento[T]{
		Dados:     dados,
		EventId:   uuid.NewString(),
		EventType: tipoEvento,
		CreatedAt: time.Now(),
	}
}

func NewComando[T any](cadastro T, userId string, tipoComando string) *Comando[T] {
	return &Comando[T]{
		Cadastro:  cadastro,
		CreatedAt: time.Now(),
		UserId:    userId,
		CmdId:     uuid.NewString(),
		TipoCmd:   tipoComando,
		Status:    "pendente",
	}
}
