package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type RegistroMedia struct {
	FileID    string
	UserID    string
	Filename  string
	MediaType string
	MimeType  string
	FileSize  int64
	Bucket    string
	Metadata  []byte // JSONB mapeia bem para []byte em Go
	CreatedAt string
}

const (
	SaveRegistroMediaQ = `INSERT INTO consulta.registro_media (file_id, user_id, filename, media_type, mime_type, file_size, bucket, metadata) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
)

type ConsultaRepository struct {
	Conn              *pgx.Conn
	saveRegistroMedia *pgconn.StatementDescription
}

func NewConsultaRepository(conn *pgx.Conn) (*ConsultaRepository, error) {
	saveRegistroMediaStmt, err := conn.Prepare(context.Background(), "save_registro_media", SaveRegistroMediaQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar query 'save_registro_media': %s", err)
	}

	return &ConsultaRepository{
		Conn:              conn,
		saveRegistroMedia: saveRegistroMediaStmt,
	}, nil
}

func (r *ConsultaRepository) Close() {
	r.Conn.Close(context.Background())
}

func (r *ConsultaRepository) SaveRegistroMedia(ctx context.Context, registroMedia RegistroMedia) error {
	_, err := r.Conn.Exec(ctx, "save_registro_media",
		registroMedia.FileID,
		registroMedia.UserID,
		registroMedia.Filename,
		registroMedia.MediaType,
		registroMedia.MimeType,
		registroMedia.FileSize,
		registroMedia.Bucket,
		registroMedia.Metadata)
	if err != nil {
		return fmt.Errorf("Falha ao executar query 'save_registro_media': %s", err)
	}

	return nil
}
