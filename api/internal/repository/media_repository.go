package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	getMediaPagedQuery = `SELECT * FROM consulta.registro_media
                              WHERE user_id = $1
                              ORDER BY created_at
                              LIMIT $2
                              OFFSET $3;`
)

type MediaRepository struct {
	Conn          *pgx.Conn
	getMediaPaged *pgconn.StatementDescription
}

func NewMediaRepository(conn *pgx.Conn) (*MediaRepository, error) {
	getMediaPagedStmt, err := conn.Prepare(context.Background(), "get_media_paged", getMediaPagedQuery)
	if err != nil {
		return nil, fmt.Errorf("Falha ao prepara query 'get_media_paged': %s", err)
	}

	return &MediaRepository{
		Conn:          conn,
		getMediaPaged: getMediaPagedStmt,
	}, nil
}

func (repository *MediaRepository) GetMediaPaged(userId string, limit int, offset int) (pgx.Rows, error) {
	return repository.Conn.Query(context.Background(), "get_media_paged", userId, limit, offset)
}
