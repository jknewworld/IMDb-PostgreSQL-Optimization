
\set ON_ERROR_STOP on
SET search_path TO imdb, public;

TRUNCATE TABLE imdb.message_queue RESTART IDENTITY;

-- Test 1: enqueue one message
DO $$
DECLARE
    v_id BIGINT;
BEGIN
    v_id := imdb.enqueue('test_single', '{"table":"title_basics","data":{"tconst":"tt9000001"}}'::JSONB);
    IF v_id IS NULL THEN
        RAISE EXCEPTION 'Test 1 failed: enqueue returned null';
    END IF;
    RAISE NOTICE 'Test 1 passed';
END;
$$;

-- Test 2: dequeue one message
DO $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM imdb.dequeue('test_single', 1, INTERVAL '1 minute');
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'Test 2 failed: expected 1 row, got %', v_count;
    END IF;
    RAISE NOTICE 'Test 2 passed';
END;
$$;

-- Test 3: ACK
DO $$
DECLARE
    v_id BIGINT;
    v_status TEXT;
BEGIN
    SELECT id INTO v_id
    FROM imdb.message_queue
    WHERE queue_name = 'test_single'
    ORDER BY id
    LIMIT 1;
    PERFORM imdb.ack(v_id);
    SELECT status INTO v_status FROM imdb.message_queue WHERE id = v_id;
    IF v_status <> 'done' THEN
        RAISE EXCEPTION 'Test 3 failed: expected done, got %', v_status;
    END IF;
    RAISE NOTICE 'Test 3 passed';
END;
$$;

-- Test 4: NACK creates a retry when attempts remain
DO $$
DECLARE
    v_id BIGINT;
    v_status TEXT;
BEGIN
    v_id := imdb.enqueue('test_nack', '{"value":1}'::JSONB);
    PERFORM 1 FROM imdb.dequeue('test_nack', 1, INTERVAL '1 minute');
    PERFORM imdb.nack(v_id, 'temporary test failure');
    SELECT status INTO v_status FROM imdb.message_queue WHERE id = v_id;
    IF v_status <> 'pending' THEN
        RAISE EXCEPTION 'Test 4 failed: expected pending, got %', v_status;
    END IF;
    RAISE NOTICE 'Test 4 passed';
END;
$$;

-- Test 5: batch dequeue
DO $$
DECLARE
    v_count INTEGER;
BEGIN
    PERFORM imdb.enqueue('test_batch', jsonb_build_object('value', 1));
    PERFORM imdb.enqueue('test_batch', jsonb_build_object('value', 2));
    PERFORM imdb.enqueue('test_batch', jsonb_build_object('value', 3));
    SELECT COUNT(*) INTO v_count
    FROM imdb.dequeue('test_batch', 3, INTERVAL '1 minute');
    IF v_count <> 3 THEN
        RAISE EXCEPTION 'Test 5 failed: expected 3 rows, got %', v_count;
    END IF;
    RAISE NOTICE 'Test 5 passed';
END;
$$;

-- Test 6: empty queue behavior
DO $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM imdb.dequeue('queue_does_not_exist', 10, INTERVAL '1 minute');
    IF v_count <> 0 THEN
        RAISE EXCEPTION 'Test 6 failed: expected 0 rows, got %', v_count;
    END IF;
    RAISE NOTICE 'Test 6 passed';
END;
$$;

-- Test 7: multiple queue names are isolated
DO $$
DECLARE
    v_a INTEGER;
    v_b INTEGER;
BEGIN
    PERFORM imdb.enqueue('queue_a', '{"queue":"a"}'::JSONB);
    PERFORM imdb.enqueue('queue_b', '{"queue":"b"}'::JSONB);
    SELECT COUNT(*) INTO v_a FROM imdb.dequeue('queue_a', 10, INTERVAL '1 minute');
    SELECT COUNT(*) INTO v_b FROM imdb.dequeue('queue_b', 10, INTERVAL '1 minute');
    IF v_a <> 1 OR v_b <> 1 THEN
        RAISE EXCEPTION 'Test 7 failed: queue_a %, queue_b %', v_a, v_b;
    END IF;
    RAISE NOTICE 'Test 7 passed';
END;
$$;

-- Test 8 is a two-session concurrency test. Exact commands are documented in README.md under "Concurrent queue test".

-- Test 9: visibility timeout expiration
DO $$
DECLARE
    v_id BIGINT;
    v_count INTEGER;
BEGIN
    v_id := imdb.enqueue('test_visibility', '{"value":9}'::JSONB);
    PERFORM 1 FROM imdb.dequeue('test_visibility', 1, INTERVAL '30 seconds');
    UPDATE imdb.message_queue
    SET processing_started_at = NOW() - INTERVAL '2 minutes'
    WHERE id = v_id;
    SELECT COUNT(*) INTO v_count
    FROM imdb.dequeue('test_visibility', 1, INTERVAL '30 seconds');
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'Test 9 failed: expired message was not reclaimed';
    END IF;
    RAISE NOTICE 'Test 9 passed';
END;
$$;

-- Test 10: retry limit and final failed status
DO $$
DECLARE
    v_id BIGINT;
    v_status TEXT;
BEGIN
    v_id := imdb.enqueue('test_retry', '{"value":10}'::JSONB);
    UPDATE imdb.message_queue SET max_attempts = 2 WHERE id = v_id;

    PERFORM 1 FROM imdb.dequeue('test_retry', 1, INTERVAL '1 minute');
    PERFORM imdb.nack(v_id, 'first failure');
    UPDATE imdb.message_queue SET available_at = NOW() WHERE id = v_id;

    PERFORM 1 FROM imdb.dequeue('test_retry', 1, INTERVAL '1 minute');
    PERFORM imdb.nack(v_id, 'second failure');

    SELECT status INTO v_status FROM imdb.message_queue WHERE id = v_id;
    IF v_status <> 'failed' THEN
        RAISE EXCEPTION 'Test 10 failed: expected failed, got %', v_status;
    END IF;
    RAISE NOTICE 'Test 10 passed';
END;
$$;

-- Test 11: duplicate payloads are valid queue messages
DO $$
DECLARE
    v_count INTEGER;
BEGIN
    PERFORM imdb.enqueue('test_duplicate', '{"same":true}'::JSONB);
    PERFORM imdb.enqueue('test_duplicate', '{"same":true}'::JSONB);
    SELECT COUNT(*) INTO v_count
    FROM imdb.message_queue
    WHERE queue_name = 'test_duplicate';
    IF v_count <> 2 THEN
        RAISE EXCEPTION 'Test 11 failed: expected 2 queue rows, got %', v_count;
    END IF;
    RAISE NOTICE 'Test 11 passed';
END;
$$;

-- Test 12: malformed application payload is permanently NACKed by convention
DO $$
DECLARE
    v_id BIGINT;
    v_status TEXT;
BEGIN
    v_id := imdb.enqueue('test_malformed', '{"unexpected":true}'::JSONB);
    PERFORM 1 FROM imdb.dequeue('test_malformed', 1, INTERVAL '1 minute');
    PERFORM imdb.nack(v_id, '[permanent] payload must contain table and data');
    SELECT status INTO v_status FROM imdb.message_queue WHERE id = v_id;
    IF v_status <> 'failed' THEN
        RAISE EXCEPTION 'Test 12 failed: expected failed, got %', v_status;
    END IF;
    RAISE NOTICE 'Test 12 passed';
END;
$$;

SELECT * FROM imdb.queue_metrics();
SELECT 'All single-session queue tests passed' AS result;
