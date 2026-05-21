CREATE SCHEMA consulta;

CREATE TABLE consulta.registro_media (
    file_id UUID PRIMARY KEY,                   -- Identificador único do arquivo
    user_id UUID NOT NULL,                      -- Quem enviou (bom para indexar e relacionar)
    filename TEXT NOT NULL,                     -- Nome original do arquivo
    media_type VARCHAR(20) NOT NULL,            -- ex: 'image', 'video'
    mime_type VARCHAR(100) NOT NULL,            -- ex: 'image/jpeg', 'video/mp4'
    file_size BIGINT NOT NULL,                  -- Equivalente ao int64 do Go (em bytes)
    bucket VARCHAR(63) NOT NULL,                -- Nome do bucket (S3 limita a 63 caracteres)
    metadata JSONB DEFAULT '{}'::jsonb,         -- Metadados em formato JSON binário (permite indexação)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() -- Data de registro automática
);

-- Índices essenciais para performance no seu App de fotos
CREATE INDEX idx_media_user_created ON consulta.registro_media (user_id, created_at DESC);
CREATE INDEX idx_media_metadata ON connsulta.registro_media USING gin (metadata);
