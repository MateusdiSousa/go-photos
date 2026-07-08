CREATE SCHEMA registro;

CREATE TABLE registro.registro_media (
file_id TEXT PRIMARY KEY,
user_id TEXT NOT NULL,
hash_sha256 TEXT NOT NULL
);

CREATE INDEX idx_hash_file ON registro.registro_media (hash_sha256);
