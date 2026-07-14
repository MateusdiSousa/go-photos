ALTER TABLE consulta.registro_media
ADD COLUMN hash_sha256 VARCHAR(64),
ADD COLUMN thumbnail_path TEXT,
ADD COLUMN file_path TEXT;
