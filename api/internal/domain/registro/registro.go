package registro

import (
	"time"

	"github.com/google/uuid"
)

type RegistroMedia struct {
	UserId    string `json:"user-id"`
	Filename  string `json:"filename"`
	MediaType string `json:"media-type"`
	Mimetype  string `json:"mime-type"`
	Metadata  string `json:"metadata"`
	Size      int64  `json:"size"`
	FilePath  string `json:"filepath"`
	FileId    string `json:"file-id"`
	Bucket    string `json:"bucket"`
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
