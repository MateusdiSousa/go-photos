package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/MateusdiSousa/go-photos/api/domain/registro"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	InsertEventStoreQ = `
		INSERT INTO archiver.event_store 
		(event_id, aggregate_id, event_type, version, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	GetEventsByAggregateQ = `
		SELECT id, event_id, aggregate_id, event_type, version, payload, created_at
		FROM archiver.event_store
		WHERE aggregate_id = $1
		ORDER BY created_at ASC
	`

	GetEventsByTypeQ = `
		SELECT id, event_id, aggregate_id, event_type, version, payload, created_at
		FROM archiver.event_store
		WHERE event_type = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	GetLastEventByAggregateQ = `
		SELECT id, event_id, aggregate_id, event_type, version, payload, created_at
		FROM archiver.event_store
		WHERE aggregate_id = $1
		ORDER BY created_at DESC
		LIMIT 1
	`
)

// Querier Interface para operações de banco
type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type EventStoreRepository struct {
	querier Querier
	conn    *pgx.Conn
}

type EventStore struct {
	ID          int64
	EventID     string
	AggregateID string
	EventType   string
	Version     int
	Payload     json.RawMessage
	CreatedAt   time.Time
}

func NewEventStoreRepository(conn *pgx.Conn) (*EventStoreRepository, error) {
	// Preparar queries
	_, err := conn.Prepare(context.Background(), "insert_event_store", InsertEventStoreQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar query 'insert_event_store': %s", err)
	}

	_, err = conn.Prepare(context.Background(), "get_events_by_aggregate", GetEventsByAggregateQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar query 'get_events_by_aggregate': %s", err)
	}

	_, err = conn.Prepare(context.Background(), "get_events_by_type", GetEventsByTypeQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar query 'get_events_by_type': %s", err)
	}

	_, err = conn.Prepare(context.Background(), "get_last_event_by_aggregate", GetLastEventByAggregateQ)
	if err != nil {
		return nil, fmt.Errorf("Falha ao preparar query 'get_last_event_by_aggregate': %s", err)
	}

	return &EventStoreRepository{
		querier: conn,
		conn:    conn,
	}, nil
}

func NewEventStoreRepositoryWithTx(tx pgx.Tx) *EventStoreRepository {
	return &EventStoreRepository{
		querier: tx,
		conn:    nil,
	}
}

func (r *EventStoreRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	if r.conn == nil {
		return nil, fmt.Errorf("não é possível iniciar transação a partir de um repositório com transação")
	}
	return r.conn.Begin(ctx)
}

// SaveEvent salva um evento no event store
func (r *EventStoreRepository) SaveEvent(ctx context.Context, event registro.Evento[any]) error {
	// Serializar payload para JSON
	payload, err := json.Marshal(event.Dados)
	if err != nil {
		return fmt.Errorf("falha ao serializar payload: %w", err)
	}

	_, err = r.querier.Exec(ctx, "insert_event_store",
		event.EventId,
		event.AggregateId,
		event.EventType,
		event.Version,
		payload,
		event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("falha ao inserir evento no event store: %w", err)
	}

	return nil
}

// GetEventsByAggregate retorna todos os eventos de um agregado
func (r *EventStoreRepository) GetEventsByAggregate(ctx context.Context, aggregateID string) ([]EventStore, error) {
	rows, err := r.querier.Query(ctx, "get_events_by_aggregate", aggregateID)
	if err != nil {
		return nil, fmt.Errorf("falha ao buscar eventos: %w", err)
	}
	defer rows.Close()

	var events []EventStore
	for rows.Next() {
		var e EventStore
		err := rows.Scan(
			&e.ID,
			&e.EventID,
			&e.AggregateID,
			&e.EventType,
			&e.Version,
			&e.Payload,
			&e.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("falha ao scanear evento: %w", err)
		}
		events = append(events, e)
	}

	return events, nil
}

// GetEventsByType retorna eventos de um tipo específico com paginação
func (r *EventStoreRepository) GetEventsByType(ctx context.Context, eventType string, limit, offset int) ([]EventStore, error) {
	rows, err := r.querier.Query(ctx, "get_events_by_type", eventType, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("falha ao buscar eventos: %w", err)
	}
	defer rows.Close()

	var events []EventStore
	for rows.Next() {
		var e EventStore
		err := rows.Scan(
			&e.ID,
			&e.EventID,
			&e.AggregateID,
			&e.EventType,
			&e.Version,
			&e.Payload,
			&e.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("falha ao scanear evento: %w", err)
		}
		events = append(events, e)
	}

	return events, nil
}

// GetLastEventByAggregate retorna o último evento de um agregado
func (r *EventStoreRepository) GetLastEventByAggregate(ctx context.Context, aggregateID string) (*EventStore, error) {
	var e EventStore
	err := r.querier.QueryRow(ctx, "get_last_event_by_aggregate", aggregateID).Scan(
		&e.ID,
		&e.EventID,
		&e.AggregateID,
		&e.EventType,
		&e.Version,
		&e.Payload,
		&e.CreatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("falha ao buscar último evento: %w", err)
	}

	return &e, nil
}
