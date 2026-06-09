package repository

import (
	"context"
	"fmt"

	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	SaveRegistroMediaQ = `INSERT INTO consulta.registro_media
                              (file_id, user_id, filename, media_type, mime_type, file_size, bucket, metadata, hash_sha256)
                              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                              ON CONFLICT (file_id)
                              DO UPDATE SET
                              filename = $3,
                              media_type = $4,
                              mime_type = $5,
                              file_size = $6,
                              bucket = $7,
                              metadata = $8,
                              hash_sha256 = $9;
`
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

func (r *ConsultaRepository) SaveRegistroMedia(ctx context.Context, registroMedia registro.RegistroMedia) error {

	_, err := r.Conn.Exec(ctx, "save_registro_media",
		registroMedia.FileId,
		registroMedia.UserId,
		registroMedia.Filename,
		registroMedia.MediaType,
		registroMedia.Mimetype,
		registroMedia.Size,
		registroMedia.Bucket,
		registroMedia.Metadata,
		registroMedia.HashSha256,
	)
	if err != nil {
		return fmt.Errorf("Falha ao executar query 'save_registro_media': %s", err)
	}

	return nil
}
