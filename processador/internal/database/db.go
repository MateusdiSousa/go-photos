package database

import (
	"context"
	"log"
	"sync"

	"github.com/jackc/pgx/v5"
)

var (
	once     sync.Once
	instance *pgx.Conn = nil
	URI                = "postgresql://postgres:example@localhost:5432/postgres"
)

func GetInstace() (*pgx.Conn, error) {
	var err error = nil

	once.Do(func() {
		log.Print("Conectando ao banco de dados postgres...")
		instance, err = pgx.Connect(context.Background(), URI)
		if err != nil {
			log.Printf("Falha ao conectar ao banco de dados: %s", err)
		}
	})
	if err != nil {
		return nil, err
	}

	return instance, nil
}
