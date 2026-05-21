package database

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
)

var instance *pgx.Conn = nil

const URI = ""

func GetInstace() *pgx.Conn {
	var err error
	if instance == nil {
		instance, err = pgx.Connect(context.Background(), URI)
		if err != nil {
			log.Fatalf("Falha ao conectar ao banco de dados: %s", err)
		}
	}
	return instance
}
