CREATE SCHEMA archiver;

CREATE TABLE comando.event_store (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,           -- ID único do evento (gerado no comando)
    aggregate_id UUID NOT NULL,              -- O ID da entidade principal (file_id)
    event_type VARCHAR(100) NOT NULL,        -- 'MidiaRegistrada', 'MidiaDeletada'
    version INT NOT NULL DEFAULT 1,          -- Versão do esquema do evento (para evolução futura)
    payload JSONB NOT NULL,                  -- Dados específicos do evento
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_event_store_aggregate ON comando.event_store (aggregate_id);
CREATE INDEX idx_event_store_type ON comando.event_store (event_type);
