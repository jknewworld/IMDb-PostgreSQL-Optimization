"""
IMDB TSV Producer
Reads TSV files and sends records as JSON messages to the PostgreSQL queue.

STUDENTS: Fill in the TODO sections below.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import logging
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, TextIO

import psycopg2
from psycopg2.extras import Json, execute_values

from common import (
    DATASET_BY_FILENAME,
    DATASET_SPECS,
    configure_logging,
    database_config,
    env_bool,
    env_float,
    env_int,
    normalize_tsv_row,
)


LOGGER = logging.getLogger("imdb_project.producer")


@contextmanager
def open_dataset(path: Path) -> Iterator[TextIO]:
    if path.name.endswith(".gz"):
        with gzip.open(path, mode="rt", encoding="utf-8", newline="") as stream:
            yield stream
    else:
        with path.open(mode="r", encoding="utf-8", newline="") as stream:
            yield stream


def send_batch(cursor, queue_name: str, messages: list[dict]) -> None:
    """Enqueue many *individual* record messages in one SQL round trip."""
    if not messages:
        return
    values = [(queue_name, Json(message)) for message in messages]
    execute_values(
        cursor,
        """
        SELECT imdb.enqueue(v.queue_name::TEXT, v.payload::JSONB)
        FROM (VALUES %s) AS v(queue_name, payload)
        """,
        values,
        page_size=len(values),
    )


def locate_files(data_dir: Path, requested: list[str] | None) -> list[Path]:
    paths: list[Path] = []
    requested_set = set(requested or [])

    for spec in DATASET_SPECS:
        if requested_set and spec.filename not in requested_set and spec.table not in requested_set:
            continue
        compressed = data_dir / spec.filename
        plain = data_dir / spec.filename.removesuffix(".gz")
        if compressed.exists():
            paths.append(compressed)
        elif plain.exists():
            paths.append(plain)
        else:
            LOGGER.warning("Dataset file is missing: %s", spec.filename)

    known = {
        item
        for spec in DATASET_SPECS
        for item in (spec.filename, spec.filename.removesuffix(".gz"), spec.table)
    }
    unknown = requested_set - known
    if unknown:
        raise ValueError(f"unknown file/table selectors: {', '.join(sorted(unknown))}")
    return paths


def _file_identity(filepath: Path) -> tuple[int, int]:
    stat = filepath.stat()
    return stat.st_size, stat.st_mtime_ns


def load_progress(connection, filepath: Path, table_name: str, restart_progress: bool) -> dict:
    size_bytes, mtime_ns = _file_identity(filepath)
    with connection.cursor() as cursor:
        if restart_progress:
            cursor.execute(
                "DELETE FROM imdb.producer_progress WHERE source_file = %s",
                (filepath.name,),
            )
            connection.commit()

        cursor.execute(
            """
            SELECT source_size_bytes, source_mtime_ns, last_line,
                   rows_enqueued, malformed_rows, completed
            FROM imdb.producer_progress
            WHERE source_file = %s
            """,
            (filepath.name,),
        )
        row = cursor.fetchone()

    if row is None:
        return {
            "size": size_bytes,
            "mtime_ns": mtime_ns,
            "last_line": 1,
            "rows_enqueued": 0,
            "malformed_rows": 0,
            "completed": False,
        }

    old_size, old_mtime, last_line, rows_enqueued, malformed_rows, completed = row
    if old_size != size_bytes or old_mtime != mtime_ns:
        raise ValueError(
            f"{filepath.name} changed since the previous producer run. "
            "Use --restart-progress after verifying the new file."
        )

    return {
        "size": size_bytes,
        "mtime_ns": mtime_ns,
        "last_line": int(last_line),
        "rows_enqueued": int(rows_enqueued),
        "malformed_rows": int(malformed_rows),
        "completed": bool(completed),
    }


def save_progress(
    cursor,
    *,
    filepath: Path,
    table_name: str,
    source_size_bytes: int,
    source_mtime_ns: int,
    last_line: int,
    rows_enqueued: int,
    malformed_rows: int,
    completed: bool,
) -> None:
    cursor.execute(
        """
        INSERT INTO imdb.producer_progress (
            source_file, table_name, source_size_bytes, source_mtime_ns,
            last_line, rows_enqueued, malformed_rows, completed, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (source_file) DO UPDATE
        SET table_name = EXCLUDED.table_name,
            source_size_bytes = EXCLUDED.source_size_bytes,
            source_mtime_ns = EXCLUDED.source_mtime_ns,
            last_line = EXCLUDED.last_line,
            rows_enqueued = EXCLUDED.rows_enqueued,
            malformed_rows = EXCLUDED.malformed_rows,
            completed = EXCLUDED.completed,
            updated_at = NOW()
        """,
        (
            filepath.name,
            table_name,
            source_size_bytes,
            source_mtime_ns,
            last_line,
            rows_enqueued,
            malformed_rows,
            completed,
        ),
    )


def queue_inflight(connection, queue_name: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM imdb.message_queue
            WHERE queue_name = %s
              AND status IN ('pending', 'processing')
            """,
            (queue_name,),
        )
        return int(cursor.fetchone()[0])


def wait_for_queue_capacity(
    connection,
    *,
    queue_name: str,
    high_water: int,
    low_water: int,
    poll_interval: float,
) -> None:
    """Bound queue growth so a fast Producer cannot fill the disk."""
    if high_water <= 0:
        return

    inflight = queue_inflight(connection, queue_name)
    if inflight < high_water:
        connection.rollback()  
        return

    LOGGER.warning(
        "Queue backpressure: %d in-flight messages >= high-water %d; waiting for Consumer",
        inflight,
        high_water,
    )
    while inflight > low_water:
        connection.rollback()
        time.sleep(poll_interval)
        inflight = queue_inflight(connection, queue_name)

    connection.rollback()
    LOGGER.info("Queue backlog dropped to %d; Producer resumed", inflight)


def process_file(
    connection,
    filepath: Path,
    *,
    queue_name: str,
    batch_size: int,
    commit_every_batches: int,
    progress_every: int,
    include_metadata: bool,
    max_rows: int,
    malformed_log: Path,
    high_water: int,
    low_water: int,
    backpressure_poll_interval: float,
    restart_progress: bool,
) -> tuple[int, int]:
    spec = DATASET_BY_FILENAME.get(filepath.name)
    if spec is None:
        raise ValueError(f"unsupported dataset file: {filepath.name}")

    progress = load_progress(connection, filepath, spec.table, restart_progress)
    if progress["completed"]:
        LOGGER.info(
            "Skipping %s: checkpoint says it was already fully enqueued (%d rows)",
            filepath.name,
            progress["rows_enqueued"],
        )
        return 0, 0

    run_enqueued = 0
    run_malformed = 0
    total_enqueued = progress["rows_enqueued"]
    total_malformed = progress["malformed_rows"]
    last_committed_line = progress["last_line"]
    last_processed_line = last_committed_line
    batches_since_commit = 0
    batch: list[dict] = []
    reached_eof = False

    malformed_log.parent.mkdir(parents=True, exist_ok=True)
    LOGGER.info(
        "Processing %s -> imdb.%s (resume after source line %d)",
        filepath.name,
        spec.table,
        last_committed_line,
    )

    with open_dataset(filepath) as stream, malformed_log.open("a", encoding="utf-8") as bad_stream:
        reader = csv.DictReader(stream, delimiter="\t", quoting=csv.QUOTE_NONE)
        expected_headers = {field.source for field in spec.fields}
        actual_headers = set(reader.fieldnames or [])
        missing_headers = expected_headers - actual_headers
        if missing_headers:
            raise ValueError(
                f"{filepath.name} is missing headers: {', '.join(sorted(missing_headers))}"
            )

        with connection.cursor() as cursor:
            for line_number, raw_row in enumerate(reader, start=2):
                if line_number <= last_committed_line:
                    continue
                if max_rows and run_enqueued >= max_rows:
                    break

                last_processed_line = line_number
                try:
                    metadata = (
                        {"source_file": filepath.name, "source_line": line_number}
                        if include_metadata
                        else {}
                    )
                    record = normalize_tsv_row(raw_row, spec, metadata=metadata)
                    batch.append({"table": spec.table, "data": record})
                    run_enqueued += 1
                    total_enqueued += 1
                except (TypeError, ValueError) as exc:
                    run_malformed += 1
                    total_malformed += 1
                    bad_stream.write(
                        f"{filepath.name}\t{line_number}\t{type(exc).__name__}: {exc}\t{raw_row!r}\n"
                    )
                    continue

                if len(batch) >= batch_size:
                    send_batch(cursor, queue_name, batch)
                    batch.clear()
                    batches_since_commit += 1

                    if batches_since_commit >= commit_every_batches:
                        save_progress(
                            cursor,
                            filepath=filepath,
                            table_name=spec.table,
                            source_size_bytes=progress["size"],
                            source_mtime_ns=progress["mtime_ns"],
                            last_line=last_processed_line,
                            rows_enqueued=total_enqueued,
                            malformed_rows=total_malformed,
                            completed=False,
                        )
                        connection.commit()
                        last_committed_line = last_processed_line
                        batches_since_commit = 0
                        wait_for_queue_capacity(
                            connection,
                            queue_name=queue_name,
                            high_water=high_water,
                            low_water=low_water,
                            poll_interval=backpressure_poll_interval,
                        )

                    if progress_every and run_enqueued % progress_every < batch_size:
                        LOGGER.info(
                            "Enqueued %d new records from %s (%d total for this source)",
                            run_enqueued,
                            filepath.name,
                            total_enqueued,
                        )
            else:
                reached_eof = True

            if batch:
                send_batch(cursor, queue_name, batch)
                batch.clear()

            save_progress(
                cursor,
                filepath=filepath,
                table_name=spec.table,
                source_size_bytes=progress["size"],
                source_mtime_ns=progress["mtime_ns"],
                last_line=last_processed_line,
                rows_enqueued=total_enqueued,
                malformed_rows=total_malformed,
                completed=reached_eof,
            )
            cursor.execute(
                """
                INSERT INTO imdb.event_log(event_type, payload, source_file)
                VALUES (
                    'producer_file_checkpoint',
                    jsonb_build_object(
                        'table', %s,
                        'run_enqueued', %s,
                        'run_malformed', %s,
                        'total_enqueued', %s,
                        'completed', %s
                    ),
                    %s
                )
                """,
                (
                    spec.table,
                    run_enqueued,
                    run_malformed,
                    total_enqueued,
                    reached_eof,
                    filepath.name,
                ),
            )
            connection.commit()

    LOGGER.info(
        "Completed run for %s: %d enqueued, %d malformed%s",
        filepath.name,
        run_enqueued,
        run_malformed,
        " (EOF reached)" if reached_eof else " (checkpoint saved; file not complete)",
    )
    return run_enqueued, run_malformed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(os.getenv("IMDB_DATA_DIR", "./data")),
    )
    parser.add_argument(
        "--queue-name",
        default=os.getenv("IMDB_QUEUE_NAME", "imdb_ingest"),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=env_int("IMDB_PRODUCER_BATCH_SIZE", 500, 1),
        help="Number of individual record messages sent in one SQL round trip.",
    )
    parser.add_argument(
        "--commit-every-batches",
        type=int,
        default=env_int("IMDB_PRODUCER_COMMIT_EVERY_BATCHES", 10, 1),
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=env_int("IMDB_PROGRESS_EVERY", 10000, 0),
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=env_int("IMDB_MAX_ROWS_PER_FILE", 0, 0),
        help="Limit new accepted records per file for testing; zero means unlimited.",
    )
    parser.add_argument(
        "--files",
        nargs="*",
        help="Dataset filenames or target table names. Default: all in dependency order.",
    )
    parser.add_argument(
        "--include-metadata",
        action="store_true",
        default=env_bool("IMDB_INCLUDE_SOURCE_METADATA", False),
    )
    parser.add_argument(
        "--malformed-log",
        type=Path,
        default=Path(os.getenv("IMDB_MALFORMED_LOG", "./logs/malformed_rows.log")),
    )
    parser.add_argument(
        "--high-water",
        type=int,
        default=env_int("IMDB_PRODUCER_HIGH_WATER", 20000, 0),
        help="Pause when pending+processing messages reach this value; zero disables backpressure.",
    )
    parser.add_argument(
        "--low-water",
        type=int,
        default=env_int("IMDB_PRODUCER_LOW_WATER", 10000, 0),
        help="Resume after backlog falls to this value.",
    )
    parser.add_argument(
        "--backpressure-poll-interval",
        type=float,
        default=env_float("IMDB_BACKPRESSURE_POLL_INTERVAL", 0.5, 0.05),
    )
    parser.add_argument(
        "--restart-progress",
        action="store_true",
        help="Forget the saved checkpoint for each selected source before reading it.",
    )
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level, Path("./logs/producer.log"))

    if args.batch_size < 1 or args.batch_size > 10000:
        raise ValueError("batch size must be between 1 and 10000")
    if args.high_water and args.low_water >= args.high_water:
        raise ValueError("low-water must be smaller than high-water")

    args.data_dir.mkdir(parents=True, exist_ok=True)
    files = locate_files(args.data_dir, args.files)
    if not files:
        LOGGER.error("No supported IMDb files found in %s", args.data_dir)
        LOGGER.error("Run: python scripts/download_imdb.py --data-dir %s", args.data_dir)
        return 1

    config = database_config()
    if not config["password"]:
        LOGGER.warning("POSTGRES_PASSWORD is empty; this only works with passwordless local access")

    total_enqueued = 0
    total_malformed = 0
    started = time.monotonic()

    try:
        with psycopg2.connect(**config) as connection:
            connection.autocommit = False
            for filepath in files:
                enqueued, malformed = process_file(
                    connection,
                    filepath,
                    queue_name=args.queue_name,
                    batch_size=args.batch_size,
                    commit_every_batches=args.commit_every_batches,
                    progress_every=args.progress_every,
                    include_metadata=args.include_metadata,
                    max_rows=args.max_rows,
                    malformed_log=args.malformed_log,
                    high_water=args.high_water,
                    low_water=args.low_water,
                    backpressure_poll_interval=args.backpressure_poll_interval,
                    restart_progress=args.restart_progress,
                )
                total_enqueued += enqueued
                total_malformed += malformed
    except psycopg2.Error as exc:
        LOGGER.exception("Database operation failed: %s", exc)
        return 2
    except (OSError, ValueError, csv.Error) as exc:
        LOGGER.exception("Producer failed: %s", exc)
        return 3

    LOGGER.info(
        "Producer finished: %d new messages, %d malformed rows, %.2f seconds",
        total_enqueued,
        total_malformed,
        time.monotonic() - started,
    )
    LOGGER.info(
        "Restart safety: enqueue and producer checkpoint are committed together; final inserts are also idempotent via real UNIQUE/PK constraints."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
