-- ============================================================================
-- Initialization Script for IMDB Project
-- Creates schema, base tables, and the message queue table.
--
-- STUDENTS: You must implement the queue functions (enqueue, dequeue,
-- ack, nack) yourselves as PostgreSQL PL/pgSQL functions.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS imdb;
SET search_path TO imdb, public;

-- ============================================================================
-- Base Tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS imdb.title_basics (
    tconst TEXT PRIMARY KEY,
    title_type TEXT,
    primary_title TEXT,
    original_title TEXT,
    is_adult BOOLEAN,
    start_year INTEGER,
    end_year INTEGER,
    runtime_minutes INTEGER,
    genres TEXT[],
    metadata JSONB DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS imdb.title_ratings (
    tconst TEXT PRIMARY KEY REFERENCES imdb.title_basics(tconst),
    average_rating NUMERIC(3,1),
    num_votes INTEGER,
    metadata JSONB DEFAULT '{}'
);


CREATE TABLE IF NOT EXISTS imdb.name_basics (
    nconst TEXT PRIMARY KEY,
    primary_name TEXT,
    birth_year INTEGER,
    death_year INTEGER,
    primary_profession TEXT[],
    known_for_titles TEXT[],
    metadata JSONB DEFAULT '{}'
);


CREATE TABLE IF NOT EXISTS imdb.title_crew (
    tconst TEXT PRIMARY KEY REFERENCES imdb.title_basics(tconst),
    directors TEXT[],
    writers TEXT[],
    metadata JSONB DEFAULT '{}'
);


CREATE TABLE IF NOT EXISTS imdb.title_principals (
    id SERIAL PRIMARY KEY,
    tconst TEXT REFERENCES imdb.title_basics(tconst),
    ordering INTEGER,
    nconst TEXT REFERENCES imdb.name_basics(nconst),
    category TEXT,
    job TEXT,
    characters TEXT,
    metadata JSONB DEFAULT '{}'
);


CREATE TABLE IF NOT EXISTS imdb.title_akas (
    id SERIAL PRIMARY KEY,
    title_id TEXT REFERENCES imdb.title_basics(tconst),
    ordering INTEGER,
    title TEXT,
    region TEXT,
    language TEXT,
    types TEXT[],
    attributes TEXT[],
    is_original_title BOOLEAN,
    metadata JSONB DEFAULT '{}'
);


CREATE TABLE IF NOT EXISTS imdb.title_episode (
    tconst TEXT PRIMARY KEY REFERENCES imdb.title_basics(tconst),
    parent_tconst TEXT REFERENCES imdb.title_basics(tconst),
    season_number INTEGER,
    episode_number INTEGER,
    metadata JSONB DEFAULT '{}'
);


CREATE TABLE IF NOT EXISTS imdb.event_log (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT,
    payload JSONB,
    source_file TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- Message Queue Table (students must implement the functions)
-- ============================================================================

CREATE TABLE IF NOT EXISTS imdb.message_queue (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL DEFAULT 'default',
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'done', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_message_queue_status
    ON imdb.message_queue(queue_name, status, id)
    WHERE status = 'pending';


-- ============================================================================
-- HINT: Implement these functions:
--
-- imdb.enqueue(queue_name TEXT, payload JSONB) RETURNS BIGINT
--   INSERT INTO message_queue ... RETURNING id
--
-- imdb.dequeue(queue_name TEXT, batch_size INT, visibility_timeout INTERVAL)
--   RETURNS TABLE(msg_id BIGINT, payload JSONB)
--   Use WITH ... FOR UPDATE SKIP LOCKED then UPDATE status = 'processing'
--
-- imdb.ack(msg_id BIGINT) RETURNS VOID
--   UPDATE message_queue SET status = 'done'
--
-- imdb.nack(msg_id BIGINT, reason TEXT) RETURNS VOID
--   UPDATE message_queue SET status = 'failed'
--
-- imdb.queue_metrics(queue_name TEXT DEFAULT NULL)
--   RETURNS TABLE(queue_name TEXT, pending BIGINT, processing BIGINT, ...)
-- ============================================================================

ALTER TABLE imdb.message_queue
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 5,
    ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS processing_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

-- queue integrity 
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'message_queue_attempt_count_chk'
          AND conrelid = 'imdb.message_queue'::regclass
    ) THEN
        ALTER TABLE imdb.message_queue
            ADD CONSTRAINT message_queue_attempt_count_chk
            CHECK (attempt_count >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'message_queue_max_attempts_chk'
          AND conrelid = 'imdb.message_queue'::regclass
    ) THEN
        ALTER TABLE imdb.message_queue
            ADD CONSTRAINT message_queue_max_attempts_chk
            CHECK (max_attempts > 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'message_queue_attempt_limit_chk'
          AND conrelid = 'imdb.message_queue'::regclass
    ) THEN
        ALTER TABLE imdb.message_queue
            ADD CONSTRAINT message_queue_attempt_limit_chk
            CHECK (attempt_count <= max_attempts);
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'title_principals_natural_uk'
          AND conrelid = 'imdb.title_principals'::regclass
    ) THEN
        ALTER TABLE imdb.title_principals
            ADD CONSTRAINT title_principals_natural_uk
            UNIQUE (tconst, ordering);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'title_akas_natural_uk'
          AND conrelid = 'imdb.title_akas'::regclass
    ) THEN
        ALTER TABLE imdb.title_akas
            ADD CONSTRAINT title_akas_natural_uk
            UNIQUE (title_id, ordering);
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS imdb.producer_progress (
    source_file TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    source_size_bytes BIGINT NOT NULL,
    source_mtime_ns BIGINT NOT NULL,
    last_line BIGINT NOT NULL DEFAULT 1,
    rows_enqueued BIGINT NOT NULL DEFAULT 0,
    malformed_rows BIGINT NOT NULL DEFAULT 0,
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- Indexes (minimal - students should add more)
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_title_basics_type
    ON imdb.title_basics(title_type);

CREATE INDEX IF NOT EXISTS idx_title_basics_year
    ON imdb.title_basics(start_year);

CREATE INDEX IF NOT EXISTS idx_title_ratings_votes
    ON imdb.title_ratings(num_votes DESC);

CREATE INDEX IF NOT EXISTS idx_title_ratings_rating
    ON imdb.title_ratings(average_rating DESC);

SELECT 'IMDB schema created successfully' AS status;
