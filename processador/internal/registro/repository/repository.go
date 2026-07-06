package repository

import (
	"context"
	"fmt"

	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	"github.com/jackc/pgx/v5"
)

type IRegistroRepository interface {
	SaveRegistroMedia(context.Context, registro.RegistroMedia) error
	FindRegistroByFileIdAndUserId(ctx context.Context, FileId string, UserId string) (*registro.RegistroMedia, error)
	DeleteRegistroByFileIdAndUserId(ctx context.Context, FileId string, UserId string) error
}

const (
	SaveRegistroMediaQ = `INSERT INTO registro.registro_media (file_id, user_id, hash_sha256)
                              VALUES ($1, $2, $3)
                              ON CONFLICT (file_id) DO UPDATE SET hash_sha256 = $3;`
	FindRegistroByFileIdAndUserIdQ   = `SELECT * FROM registro.registro_media WHERE file_id = $1 AND user_id = $2;`
	DeleteRegistroByFileIdAndUserIdQ = `DELETE FROM registro.registro_media WHERE file_id = $1 AND user_id = $2;`
)

type RegistroRepository struct {
	Conn *pgx.Conn
}

func NewRegistroRepository(conn *pgx.Conn) (*RegistroRepository, error) {
	_, err := conn.Prepare(context.Background(), "save_registro_media", SaveRegistroMediaQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar a query 'save_registro_media': %s'", err)
	}

	_, err = conn.Prepare(context.Background(), "find_registro_by_file-id_and_user-id", FindRegistroByFileIdAndUserIdQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar a query 'find_registro_by_file-id_and_user-id': %s'", err)
	}

	_, err = conn.Prepare(context.Background(), "delete_registro_by_file-id_and_user-id", FindRegistroByFileIdAndUserIdQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar a query 'delete_registro_by_file-id_and_user-id': %s'", err)
	}

	return &RegistroRepository{
		Conn: conn,
	}, nil
}

func (r *RegistroRepository) SaveRegistroMedia(ctx context.Context, data registro.RegistroMedia) error {
	_, err := r.Conn.Exec(ctx, "save_registro_media",
		data.FileId, data.UserId, data.HashSha256)

	if err != nil {
		return fmt.Errorf("Falha ao atualizar tabela de registro: %s", err)
	}
	return nil
}

func (r *RegistroRepository) FindRegistroByFileIdAndUserId(ctx context.Context, fileId string, userId string) (*registro.RegistroMedia, error) {
	rows, err := r.Conn.Query(ctx, "find_registro_by_file-id_and_user-id", fileId, userId)
	if err != nil {
		return nil, fmt.Errorf("Falha ao procurar registro: %s", err)
	}
	defer rows.Close()

	registro, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[*registro.RegistroMedia])
	if err != nil {
		return nil, fmt.Errorf("Falha ao converter row para objeto: %s", err)
	}

	return *registro, nil
}

func (r *RegistroRepository) DeleteRegistroByFileIdAndUserId(ctx context.Context, fileId string, userId string) error {
	_, err := r.Conn.Exec(ctx, "delete_registro_by_file-id_and_user-id", fileId, userId)
	if err != nil {
		return fmt.Errorf("Falha ao deletar registro: %s", err)
	}

	return nil
}
