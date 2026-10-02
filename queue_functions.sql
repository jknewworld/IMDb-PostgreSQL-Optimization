SET search_path TO imdb, public;


CREATE INDEX IF NOT EXISTS idx_message_queue_pending_ready
    ON imdb.message_queue(queue_name, available_at, id)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_message_queue_processing_timeout
    ON imdb.message_queue(queue_name, processing_started_at, id)
    WHERE status = 'processing';

CREATE INDEX IF NOT EXISTS idx_message_queue_status_metrics
    ON imdb.message_queue(queue_name, status);

-- Enqueue
CREATE OR REPLACE FUNCTION imdb.enqueue(
    p_queue_name TEXT,
    p_payload JSONB
) RETURNS BIGINT AS $$
DECLARE
    v_message_id BIGINT;
BEGIN
    IF p_queue_name IS NULL OR BTRIM(p_queue_name) = '' THEN
        RAISE EXCEPTION 'queue name must not be null or empty'
            USING ERRCODE = '22023';
    END IF;

    IF p_payload IS NULL THEN
        RAISE EXCEPTION 'payload must not be null'
            USING ERRCODE = '22004';
    END IF;

    INSERT INTO imdb.message_queue (
        queue_name,
        payload,
        status,
        attempt_count,
        max_attempts,
        available_at,
        created_at,
        updated_at
    )
    VALUES (
        p_queue_name,
        p_payload,
        'pending',
        0,
        5,
        NOW(),
        NOW(),
        NOW()
    )
    RETURNING id INTO v_message_id;

    RETURN v_message_id;
END;
$$ LANGUAGE plpgsql;


-- Dequeue
CREATE OR REPLACE FUNCTION imdb.dequeue(
    p_queue_name TEXT,
    p_batch_size INTEGER DEFAULT 10,
    p_visibility_timeout INTERVAL DEFAULT INTERVAL '30 seconds'
) RETURNS TABLE(msg_id BIGINT, payload JSONB) AS $$
BEGIN
    IF p_queue_name IS NULL OR BTRIM(p_queue_name) = '' THEN
        RAISE EXCEPTION 'queue name must not be null or empty'
            USING ERRCODE = '22023';
    END IF;

    IF p_batch_size IS NULL OR p_batch_size < 1 OR p_batch_size > 10000 THEN
        RAISE EXCEPTION 'batch size must be between 1 and 10000'
            USING ERRCODE = '22023';
    END IF;

    IF p_visibility_timeout IS NULL OR p_visibility_timeout <= INTERVAL '0 seconds' THEN
        RAISE EXCEPTION 'visibility timeout must be greater than zero'
            USING ERRCODE = '22023';
    END IF;


    WITH exhausted AS (
        SELECT mq.id
        FROM imdb.message_queue AS mq
        WHERE mq.queue_name = p_queue_name
          AND mq.status = 'processing'
          AND mq.processing_started_at <= NOW() - p_visibility_timeout
          AND mq.attempt_count >= mq.max_attempts
        ORDER BY mq.id
        FOR UPDATE SKIP LOCKED
    )
    UPDATE imdb.message_queue AS mq
    SET status = 'failed',
        failed_at = NOW(),
        processing_started_at = NULL,
        last_error = COALESCE(
            NULLIF(mq.last_error, ''),
            'visibility timeout expired after maximum attempts'
        ),
        updated_at = NOW()
    FROM exhausted
    WHERE mq.id = exhausted.id;

    RETURN QUERY
    WITH selected AS (
        SELECT mq.id
        FROM imdb.message_queue AS mq
        WHERE mq.queue_name = p_queue_name
          AND mq.attempt_count < mq.max_attempts
          AND (
                (mq.status = 'pending' AND mq.available_at <= NOW())
                OR
                (
                    mq.status = 'processing'
                    AND mq.processing_started_at <= NOW() - p_visibility_timeout
                )
              )
        ORDER BY mq.id
        LIMIT p_batch_size
        FOR UPDATE SKIP LOCKED
    )
    UPDATE imdb.message_queue AS mq
    SET status = 'processing',
        attempt_count = mq.attempt_count + 1,
        processing_started_at = NOW(),
        completed_at = NULL,
        failed_at = NULL,
        last_error = CASE
            WHEN mq.status = 'processing'
                THEN COALESCE(NULLIF(mq.last_error, ''), 'visibility timeout expired; message reclaimed')
            ELSE mq.last_error
        END,
        updated_at = NOW()
    FROM selected
    WHERE mq.id = selected.id
    RETURNING mq.id, mq.payload;
END;
$$ LANGUAGE plpgsql;

-- ACK
CREATE OR REPLACE FUNCTION imdb.ack(
    p_msg_id BIGINT
) RETURNS VOID AS $$
BEGIN
    IF p_msg_id IS NULL THEN
        RAISE EXCEPTION 'message id must not be null'
            USING ERRCODE = '22004';
    END IF;

    UPDATE imdb.message_queue
    SET status = 'done',
        completed_at = COALESCE(completed_at, NOW()),
        processing_started_at = NULL,
        failed_at = NULL,
        updated_at = NOW()
    WHERE id = p_msg_id
      AND status IN ('processing', 'done');
END;
$$ LANGUAGE plpgsql;

-- NACK
CREATE OR REPLACE FUNCTION imdb.nack(
    p_msg_id BIGINT,
    p_reason TEXT DEFAULT ''
) RETURNS VOID AS $$
BEGIN
    IF p_msg_id IS NULL THEN
        RAISE EXCEPTION 'message id must not be null'
            USING ERRCODE = '22004';
    END IF;

    UPDATE imdb.message_queue AS mq
    SET status = CASE
            WHEN mq.status IN ('done', 'failed') THEN mq.status
            WHEN COALESCE(p_reason, '') LIKE '[permanent]%'
                 OR mq.attempt_count >= mq.max_attempts
                THEN 'failed'
            ELSE 'pending'
        END,
        available_at = CASE
            WHEN mq.status IN ('done', 'failed') THEN mq.available_at
            WHEN COALESCE(p_reason, '') LIKE '[permanent]%'
                 OR mq.attempt_count >= mq.max_attempts
                THEN mq.available_at
            ELSE NOW() + make_interval(
                secs => LEAST(
                    300,
                    CAST(power(2, GREATEST(mq.attempt_count - 1, 0)) AS INTEGER)
                )
            )
        END,
        processing_started_at = CASE
            WHEN mq.status IN ('done', 'failed') THEN mq.processing_started_at
            ELSE NULL
        END,
        failed_at = CASE
            WHEN mq.status = 'failed' THEN COALESCE(mq.failed_at, NOW())
            WHEN mq.status = 'done' THEN mq.failed_at
            WHEN COALESCE(p_reason, '') LIKE '[permanent]%'
                 OR mq.attempt_count >= mq.max_attempts
                THEN NOW()
            ELSE NULL
        END,
        last_error = CASE
            WHEN mq.status IN ('done', 'failed') THEN mq.last_error
            ELSE LEFT(COALESCE(p_reason, ''), 4000)
        END,
        updated_at = CASE
            WHEN mq.status IN ('done', 'failed') THEN mq.updated_at
            ELSE NOW()
        END
    WHERE mq.id = p_msg_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION imdb.queue_metrics(
    p_queue_name TEXT DEFAULT NULL
) RETURNS TABLE(
    queue_name TEXT,
    pending BIGINT,
    processing BIGINT,
    done BIGINT,
    failed BIGINT,
    total BIGINT,
    oldest_pending_at TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT mq.queue_name,
           COUNT(*) FILTER (WHERE mq.status = 'pending') AS pending,
           COUNT(*) FILTER (WHERE mq.status = 'processing') AS processing,
           COUNT(*) FILTER (WHERE mq.status = 'done') AS done,
           COUNT(*) FILTER (WHERE mq.status = 'failed') AS failed,
           COUNT(*) AS total,
           MIN(mq.created_at) FILTER (WHERE mq.status = 'pending') AS oldest_pending_at
    FROM imdb.message_queue AS mq
    WHERE p_queue_name IS NULL OR mq.queue_name = p_queue_name
    GROUP BY mq.queue_name
    ORDER BY mq.queue_name;
END;
$$ LANGUAGE plpgsql STABLE;


CREATE OR REPLACE FUNCTION imdb.purge_done(
    p_queue_name TEXT,
    p_older_than INTERVAL DEFAULT INTERVAL '2 minutes',
    p_limit INTEGER DEFAULT 5000
) RETURNS BIGINT AS $$
DECLARE
    v_deleted BIGINT;
BEGIN
    IF p_queue_name IS NULL OR BTRIM(p_queue_name) = '' THEN
        RAISE EXCEPTION 'queue name must not be null or empty'
            USING ERRCODE = '22023';
    END IF;

    IF p_older_than IS NULL OR p_older_than < INTERVAL '0 seconds' THEN
        RAISE EXCEPTION 'older-than interval must be non-negative'
            USING ERRCODE = '22023';
    END IF;

    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 100000 THEN
        RAISE EXCEPTION 'purge limit must be between 1 and 100000'
            USING ERRCODE = '22023';
    END IF;

    WITH victims AS (
        SELECT mq.id
        FROM imdb.message_queue AS mq
        WHERE mq.queue_name = p_queue_name
          AND mq.status = 'done'
          AND mq.completed_at <= NOW() - p_older_than
        ORDER BY mq.id
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM imdb.message_queue AS mq
    USING victims
    WHERE mq.id = victims.id;

    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION imdb.purge_done(TEXT, INTERVAL, INTEGER) IS
    'Delete a bounded batch of old ACKed messages so the durable queue remains disk-bounded during full IMDb ingestion.';

SELECT 'IMDB queue functions created successfully' AS status;
