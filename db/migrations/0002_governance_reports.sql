-- +goose Up
ALTER TABLE event_files
    ADD COLUMN IF NOT EXISTS sha256 TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS rows BIGINT NOT NULL DEFAULT 0;

ALTER TABLE replay_jobs
    ADD COLUMN IF NOT EXISTS manifest JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'event_files_rows_nonnegative'
    ) THEN
        ALTER TABLE event_files
            ADD CONSTRAINT event_files_rows_nonnegative CHECK (rows >= 0) NOT VALID;
        ALTER TABLE event_files VALIDATE CONSTRAINT event_files_rows_nonnegative;
    END IF;
END $$;

-- +goose Down
ALTER TABLE event_files
    DROP CONSTRAINT IF EXISTS event_files_rows_nonnegative;

ALTER TABLE replay_jobs
    DROP COLUMN IF EXISTS manifest;

ALTER TABLE event_files
    DROP COLUMN IF EXISTS rows,
    DROP COLUMN IF EXISTS sha256;
