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
	CountRegistroByHash  = `SELECT COUNT (*) FROM consulta.registro_media WHERE hash_sha256 = $1;`
	DeleteRegistroMediaQ = `DELETE FROM consulta.registro_media WHERE file_id = $1 AND user_id = $2;`
	GetRegistroMediaQ    = `SELECT file_id, user_id, filename, media_type, mime_type, file_size, bucket, metadata, hash_sha256, created_at FROM consulta.registro_media WHERE file_id = $1;`
	ListRegistroMediaQ   = `SELECT file_id, user_id, filename, media_type, mime_type, file_size, bucket, metadata, hash_sha256, created_at FROM consulta.registro_media WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3;`
	UpdateRegistroMediaQ = `UPDATE consulta.registro_media SET filename = $1, media_type = $2, mime_type = $3, file_size = $4, metadata = $5 WHERE file_id = $6 AND user_id = $7;`
	GetRegistroByHashQ   = `SELECT file_id, user_id, filename, media_type, mime_type, file_size, bucket, metadata, hash_sha256, created_at FROM consulta.registro_media WHERE hash_sha256 = $1;`
)

// Interface para operações de banco (suporta tanto conexão quanto transação)
type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type ConsultaRepository struct {
	Conn    *pgx.Conn
	querier Querier
}

func NewConsultaRepositoryTx(tx pgx.Tx) *ConsultaRepository {
	return &ConsultaRepository{
		Conn:    nil,
		querier: tx,
	}
}

func NewConsultaRepository(conn *pgx.Conn) (*ConsultaRepository, error) {
	_, err := conn.Prepare(context.Background(), "save_registro_media", SaveRegistroMediaQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar query 'save_registro_media': %s", err)
	}

	_, err = conn.Prepare(context.Background(), "delete_registro_media", DeleteRegistroMediaQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar query 'delete_registro_media': %s", err)
	}

	_, err = conn.Prepare(context.Background(), "count_registro_by_hash", CountRegistroByHash)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar query 'count_registro_by_hash': %s", err)
	}

	_, err = conn.Prepare(context.Background(), "get_registro_media", GetRegistroMediaQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar query 'get_registro_media': %s", err)
	}

	_, err = conn.Prepare(context.Background(), "list_registro_media", ListRegistroMediaQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar query 'list_registro_media': %s", err)
	}

	_, err = conn.Prepare(context.Background(), "update_registro_media", UpdateRegistroMediaQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar query 'update_registro_media': %s", err)
	}

	_, err = conn.Prepare(context.Background(), "get_registro_by_hash", GetRegistroByHashQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar query 'get_registro_by_hash': %s", err)
	}

	return &ConsultaRepository{
		Conn:    conn,
		querier: conn,
	}, nil
}

func (r *ConsultaRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	if r.Conn == nil {
		return nil, fmt.Errorf("não é possível iniciar transição apartir de um repositório de transição ")
	}

	return r.Conn.Begin(ctx)
}

func (r *ConsultaRepository) SaveRegistroMedia(ctx context.Context, registroMedia registro.RegistroMedia) error {
	_, err := r.querier.Exec(ctx, "save_registro_media",
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

func (r *ConsultaRepository) GetRegistroMedia(ctx context.Context, fileId string) (*registro.RegistroMedia, error) {
	var registroMedia registro.RegistroMedia
	err := r.querier.QueryRow(ctx, "get_registro_media", fileId).Scan(
		&registroMedia.FileId,
		&registroMedia.UserId,
		&registroMedia.Filename,
		&registroMedia.MediaType,
		&registroMedia.Mimetype,
		&registroMedia.Size,
		&registroMedia.Bucket,
		&registroMedia.Metadata,
		&registroMedia.HashSha256,
		&registroMedia.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("Falha ao executar query 'get_registro_media': %s", err)
	}

	return &registroMedia, nil
}

func (r *ConsultaRepository) ListRegistroMedia(ctx context.Context, userId string, limit, offset int) ([]registro.RegistroMedia, error) {
	rows, err := r.querier.Query(ctx, "list_registro_media", userId, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("Falha ao executar query 'list_registro_media': %s", err)
	}
	defer rows.Close()

	var registros []registro.RegistroMedia
	for rows.Next() {
		var reg registro.RegistroMedia
		err := rows.Scan(
			&reg.FileId,
			&reg.UserId,
			&reg.Filename,
			&reg.MediaType,
			&reg.Mimetype,
			&reg.Size,
			&reg.Bucket,
			&reg.Metadata,
			&reg.HashSha256,
			&reg.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("Falha ao scanear registro: %s", err)
		}
		registros = append(registros, reg)
	}

	return registros, nil
}

func (r *ConsultaRepository) UpdateRegistroMedia(ctx context.Context, registroMedia registro.RegistroMedia) error {
	_, err := r.querier.Exec(ctx, "update_registro_media",
		registroMedia.Filename,
		registroMedia.MediaType,
		registroMedia.Mimetype,
		registroMedia.Size,
		registroMedia.Metadata,
		registroMedia.FileId,
		registroMedia.UserId,
	)
	if err != nil {
		return fmt.Errorf("Falha ao executar query 'update_registro_media': %s", err)
	}

	return nil
}

func (r *ConsultaRepository) DeleteRegistroMedia(ctx context.Context, fileId, userId string) error {
	_, err := r.querier.Exec(ctx, "delete_registro_media", fileId, userId)
	if err != nil {
		return fmt.Errorf("Falha ao executar query 'delete_registro_media': %s", err)
	}

	return nil
}

func (r *ConsultaRepository) CountRegistroByHash(ctx context.Context, hash string) (int, error) {
	var count int
	err := r.querier.QueryRow(ctx, "count_registro_by_hash", hash).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("Falha ao executar query 'count_registro_by_hash': %s", err)
	}

	return count, nil
}

func (r *ConsultaRepository) GetRegistroByHash(ctx context.Context, hash string) (*registro.RegistroMedia, error) {
	var registroMedia registro.RegistroMedia
	err := r.querier.QueryRow(ctx, "get_registro_by_hash", hash).Scan(
		&registroMedia.FileId,
		&registroMedia.UserId,
		&registroMedia.Filename,
		&registroMedia.MediaType,
		&registroMedia.Mimetype,
		&registroMedia.Size,
		&registroMedia.Bucket,
		&registroMedia.Metadata,
		&registroMedia.HashSha256,
		&registroMedia.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("Falha ao executar query 'get_registro_by_hash': %s", err)
	}

	return &registroMedia, nil
}
