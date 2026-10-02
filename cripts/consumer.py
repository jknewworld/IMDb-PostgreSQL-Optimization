"""
IMDB Queue Consumer
Reads messages from the PostgreSQL queue and inserts them into the database.

STUDENTS: Fill in the TODO sections below.
"""

import argparse
import json
import time
import psycopg2
from psycopg2.extras import Json, execute_values
from common import database_config, normalize_message_data

SOURCE_DB_CONFIG = database_config()
TARGET_DB_CONFIG = database_config()

QUEUE_NAME = "imdb_ingest"
BATCH_SIZE = 100
POLL_INTERVAL = 1
MAX_EMPTY_POLLS = 10
VISIBILITY_TIMEOUT = "30 seconds"

# TODO: Define INSERT templates for each table.
TABLE_INSERT_TEMPLATES = {
    "title_basics": """
        INSERT INTO imdb.title_basics (
            tconst, title_type, primary_title, original_title, is_adult,
            start_year, end_year, runtime_minutes, genres, metadata
        ) VALUES %s
        ON CONFLICT (tconst) DO NOTHING
    """,
    "title_ratings": """
        INSERT INTO imdb.title_ratings (
            tconst, average_rating, num_votes, metadata
        ) VALUES %s
        ON CONFLICT (tconst) DO NOTHING
    """,
    "name_basics": """
        INSERT INTO imdb.name_basics (
            nconst, primary_name, birth_year, death_year,
            primary_profession, known_for_titles, metadata
        ) VALUES %s
        ON CONFLICT (nconst) DO NOTHING
    """,
    "title_crew": """
        INSERT INTO imdb.title_crew (
            tconst, directors, writers, metadata
        ) VALUES %s
        ON CONFLICT (tconst) DO NOTHING
    """,
    "title_principals": """
        INSERT INTO imdb.title_principals (
            tconst, ordering, nconst, category, job, characters, metadata
        ) VALUES %s
        ON CONFLICT (tconst, ordering) DO NOTHING
    """,
    "title_akas": """
        INSERT INTO imdb.title_akas (
            title_id, ordering, title, region, language,
            types, attributes, is_original_title, metadata
        ) VALUES %s
        ON CONFLICT (title_id, ordering) DO NOTHING
    """,
    "title_episode": """
        INSERT INTO imdb.title_episode (
            tconst, parent_tconst, season_number, episode_number, metadata
        ) VALUES %s
        ON CONFLICT (tconst) DO NOTHING
    """,
}

# TODO: Convert a flat data dict into a tuple matching the table schema.
#       Handle nulls, type conversions, and metadata JSON.
def build_row(table_name, data):
    """Convert a flat data dictionary to the tuple required by the table."""
    row = list(normalize_message_data(table_name, data))

    row[-1] = Json(row[-1])
    return tuple(row)


def log_event(conn, event_type, payload, source_file=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO imdb.event_log (event_type, payload, source_file)
            VALUES (%s, %s, %s)
            """,
            (event_type, Json(payload), source_file),
        )


def process_messages(
    queue_name=QUEUE_NAME,
    batch_size=BATCH_SIZE,
    max_empty_polls=MAX_EMPTY_POLLS,
    poll_interval=POLL_INTERVAL,
    visibility_timeout=VISIBILITY_TIMEOUT,
    purge_done=True,
):
    print("=" * 60)
    print("IMDB Queue Consumer")
    print("=" * 60)

    conn = psycopg2.connect(**SOURCE_DB_CONFIG)
    conn.set_session(autocommit=False)
    cur = conn.cursor()

    total_processed = 0
    total_nacked = 0
    total_invalid = 0
    empty_polls = 0
    batch_counter = 0

    try:
        while True:
            # TODO: Dequeue messages using imdb.dequeue()
            # messages = src_cur.callproc("imdb.dequeue", (...))
            cur.execute(
                "SELECT * FROM imdb.dequeue(%s, %s, %s::INTERVAL)",
                (queue_name, batch_size, visibility_timeout),
            )
            messages = cur.fetchall()

            if not messages:
                conn.commit()
                empty_polls += 1

                if max_empty_polls and empty_polls >= max_empty_polls:
                    print(f"\nQueue empty after {max_empty_polls} polls. Exiting.")
                    break

                print(
                    f"  No messages "
                    f"(poll {empty_polls}/{max_empty_polls if max_empty_polls else '∞'})..."
                )
                time.sleep(poll_interval)
                continue

            empty_polls = 0
            batch_data = {}

            # TODO: Parse JSON, build row, group by table
            for msg_id, payload in messages:
                try:
                    if isinstance(payload, str):
                        payload = json.loads(payload)

                    if not isinstance(payload, dict):
                        raise ValueError("payload must be a JSON object")

                    table_name = payload.get("table")
                    data = payload.get("data")

                    if table_name not in TABLE_INSERT_TEMPLATES:
                        raise ValueError(f"unsupported table: {table_name!r}")
                    if not isinstance(data, dict):
                        raise ValueError("payload.data must be a JSON object")

                    row = build_row(table_name, data)

                    if table_name not in batch_data:
                        batch_data[table_name] = {"rows": [], "ids": []}

                    batch_data[table_name]["rows"].append(row)
                    batch_data[table_name]["ids"].append(msg_id)

                except (ValueError, TypeError, json.JSONDecodeError) as error:
                    cur.execute(
                        "SELECT imdb.nack(%s, %s)",
                        (msg_id, f"[permanent] validation error: {error}"[:4000]),
                    )
                    total_invalid += 1
                    print(f"  [ERROR] Invalid message {msg_id}: {error}")

            for table_name, data in batch_data.items():
                if not data["rows"]:
                    continue

                cur.execute("SAVEPOINT table_insert")

                try:
                    execute_values(
                        cur,
                        TABLE_INSERT_TEMPLATES[table_name],
                        data["rows"],
                        page_size=len(data["rows"]),
                    )

                    for msg_id in data["ids"]:
                        cur.execute("SELECT imdb.ack(%s)", (msg_id,))

                    cur.execute("RELEASE SAVEPOINT table_insert")
                    total_processed += len(data["ids"])
                    print(
                        f"  Processed {len(data['ids'])} messages "
                        f"for imdb.{table_name}"
                    )

                except psycopg2.Error as error:
                    cur.execute("ROLLBACK TO SAVEPOINT table_insert")
                    cur.execute("RELEASE SAVEPOINT table_insert")

                    for msg_id in data["ids"]:
                        cur.execute(
                            "SELECT imdb.nack(%s, %s)",
                            (msg_id, f"database error: {error}"[:4000]),
                        )
                    total_nacked += len(data["ids"])
                    print(f"  [ERROR] Failed to insert into {table_name}: {error}")

            log_event(
                conn,
                "consumer_batch_complete",
                {
                    "claimed": len(messages),
                    "total_processed": total_processed,
                    "total_nacked": total_nacked,
                    "total_invalid": total_invalid,
                },
            )

            conn.commit()
            batch_counter += 1

            if purge_done and batch_counter % 20 == 0:
                cur.execute(
                    "SELECT imdb.purge_done(%s, %s::INTERVAL, %s)",
                    (queue_name, "2 minutes", 5000),
                )
                deleted = cur.fetchone()[0]
                conn.commit()
                if deleted:
                    print(f"  Purged {deleted} old done messages from queue")

    except KeyboardInterrupt:
        conn.rollback()
        print("\n\nConsumer stopped by user.")

    except psycopg2.Error as error:
        conn.rollback()
        print(f"\n[ERROR] Consumer transaction failed: {error}")

    finally:
        cur.close()
        conn.close()

    print(
        f"\nConsumer finished: {total_processed} successful messages, "
        f"{total_nacked} NACKed messages, {total_invalid} invalid messages"
    )


def parse_args():
    parser = argparse.ArgumentParser(description="IMDB Queue Consumer")
    parser.add_argument("--queue-name", default=QUEUE_NAME)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--max-empty-polls", type=int, default=MAX_EMPTY_POLLS)
    parser.add_argument("--poll-interval", type=float, default=POLL_INTERVAL)
    parser.add_argument("--visibility-timeout", default=VISIBILITY_TIMEOUT)
    parser.add_argument("--no-purge-done", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    process_messages(
        queue_name=args.queue_name,
        batch_size=args.batch_size,
        max_empty_polls=args.max_empty_polls,
        poll_interval=args.poll_interval,
        visibility_timeout=args.visibility_timeout,
        purge_done=not args.no_purge_done,
    )
