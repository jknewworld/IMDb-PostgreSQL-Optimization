from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env", override=False)

LOGGER = logging.getLogger("imdb_project")
TCONST_PATTERN = re.compile(r"^tt\d+$")
NCONST_PATTERN = re.compile(r"^nm\d+$")


def configure_logging(level: str = "INFO", log_file: Path | None = None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=handlers,
        force=True,
    )


def env_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    value = default if raw is None or raw == "" else int(raw)
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def env_float(name: str, default: float, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    value = default if raw is None or raw == "" else float(raw)
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def database_config() -> dict[str, Any]:
    return {
        "dbname": os.getenv("POSTGRES_DB", "imdb"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": env_int("POSTGRES_PORT", 5432, 1),
        "connect_timeout": env_int("POSTGRES_CONNECT_TIMEOUT", 10, 1),
        "application_name": os.getenv("POSTGRES_APPLICATION_NAME", "imdb_project"),
    }


def nullify(value: Any) -> Any:
    return None if value == r"\N" else value


def parse_text(value: Any) -> str | None:
    value = nullify(value)
    if value is None:
        return None
    return str(value)


def parse_int(value: Any) -> int | None:
    value = nullify(value)
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("boolean is not a valid integer")
    return int(value)


def parse_rating(value: Any) -> Decimal | None:
    value = nullify(value)
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"invalid rating: {value!r}") from exc
    if not Decimal("0.0") <= result <= Decimal("10.0"):
        raise ValueError(f"rating outside 0..10: {result}")
    return result


def parse_bool(value: Any) -> bool | None:
    value = nullify(value)
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "t", "yes"}:
        return True
    if normalized in {"0", "false", "f", "no"}:
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


def parse_array(value: Any) -> list[str] | None:
    value = nullify(value)
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item) for item in value if item is not None and str(item) != ""]
    if isinstance(value, tuple):
        return [str(item) for item in value if item is not None and str(item) != ""]
    text = str(value)
    if text == "":
        return []
    return [part for part in text.split(",") if part != ""]


def parse_json_text(value: Any) -> str | None:
    value = nullify(value)
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        parsed = value
    else:
        parsed = json.loads(str(value))
    return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))


def parse_metadata(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("metadata must be a JSON object")


def validate_tconst(value: Any) -> str:
    result = parse_text(value)
    if result is None or not TCONST_PATTERN.fullmatch(result):
        raise ValueError(f"invalid tconst: {value!r}")
    return result


def validate_nconst(value: Any) -> str:
    result = parse_text(value)
    if result is None or not NCONST_PATTERN.fullmatch(result):
        raise ValueError(f"invalid nconst: {value!r}")
    return result


def optional_nconst(value: Any) -> str | None:
    value = nullify(value)
    if value is None or value == "":
        return None
    return validate_nconst(value)


Converter = Callable[[Any], Any]


@dataclass(frozen=True)
class FieldSpec:
    source: str
    target: str
    converter: Converter
    required: bool = False


@dataclass(frozen=True)
class DatasetSpec:
    filename: str
    table: str
    fields: tuple[FieldSpec, ...]


DATASET_SPECS: tuple[DatasetSpec, ...] = (
    DatasetSpec(
        "title.basics.tsv.gz",
        "title_basics",
        (
            FieldSpec("tconst", "tconst", validate_tconst, True),
            FieldSpec("titleType", "title_type", parse_text),
            FieldSpec("primaryTitle", "primary_title", parse_text),
            FieldSpec("originalTitle", "original_title", parse_text),
            FieldSpec("isAdult", "is_adult", parse_bool),
            FieldSpec("startYear", "start_year", parse_int),
            FieldSpec("endYear", "end_year", parse_int),
            FieldSpec("runtimeMinutes", "runtime_minutes", parse_int),
            FieldSpec("genres", "genres", parse_array),
        ),
    ),
    DatasetSpec(
        "name.basics.tsv.gz",
        "name_basics",
        (
            FieldSpec("nconst", "nconst", validate_nconst, True),
            FieldSpec("primaryName", "primary_name", parse_text),
            FieldSpec("birthYear", "birth_year", parse_int),
            FieldSpec("deathYear", "death_year", parse_int),
            FieldSpec("primaryProfession", "primary_profession", parse_array),
            FieldSpec("knownForTitles", "known_for_titles", parse_array),
        ),
    ),
    DatasetSpec(
        "title.ratings.tsv.gz",
        "title_ratings",
        (
            FieldSpec("tconst", "tconst", validate_tconst, True),
            FieldSpec("averageRating", "average_rating", parse_rating),
            FieldSpec("numVotes", "num_votes", parse_int),
        ),
    ),
    DatasetSpec(
        "title.crew.tsv.gz",
        "title_crew",
        (
            FieldSpec("tconst", "tconst", validate_tconst, True),
            FieldSpec("directors", "directors", parse_array),
            FieldSpec("writers", "writers", parse_array),
        ),
    ),
    DatasetSpec(
        "title.episode.tsv.gz",
        "title_episode",
        (
            FieldSpec("tconst", "tconst", validate_tconst, True),
            FieldSpec("parentTconst", "parent_tconst", validate_tconst, True),
            FieldSpec("seasonNumber", "season_number", parse_int),
            FieldSpec("episodeNumber", "episode_number", parse_int),
        ),
    ),
    DatasetSpec(
        "title.akas.tsv.gz",
        "title_akas",
        (
            FieldSpec("titleId", "title_id", validate_tconst, True),
            FieldSpec("ordering", "ordering", parse_int, True),
            FieldSpec("title", "title", parse_text),
            FieldSpec("region", "region", parse_text),
            FieldSpec("language", "language", parse_text),
            FieldSpec("types", "types", parse_array),
            FieldSpec("attributes", "attributes", parse_array),
            FieldSpec("isOriginalTitle", "is_original_title", parse_bool),
        ),
    ),
    DatasetSpec(
        "title.principals.tsv.gz",
        "title_principals",
        (
            FieldSpec("tconst", "tconst", validate_tconst, True),
            FieldSpec("ordering", "ordering", parse_int, True),
            FieldSpec("nconst", "nconst", optional_nconst),
            FieldSpec("category", "category", parse_text),
            FieldSpec("job", "job", parse_text),
            FieldSpec("characters", "characters", parse_json_text),
        ),
    ),
)

DATASET_BY_FILENAME: dict[str, DatasetSpec] = {}
for _spec in DATASET_SPECS:
    DATASET_BY_FILENAME[_spec.filename] = _spec
    DATASET_BY_FILENAME[_spec.filename.removesuffix(".gz")] = _spec


TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "title_basics": (
        "tconst",
        "title_type",
        "primary_title",
        "original_title",
        "is_adult",
        "start_year",
        "end_year",
        "runtime_minutes",
        "genres",
        "metadata",
    ),
    "title_ratings": ("tconst", "average_rating", "num_votes", "metadata"),
    "name_basics": (
        "nconst",
        "primary_name",
        "birth_year",
        "death_year",
        "primary_profession",
        "known_for_titles",
        "metadata",
    ),
    "title_crew": ("tconst", "directors", "writers", "metadata"),
    "title_principals": (
        "tconst",
        "ordering",
        "nconst",
        "category",
        "job",
        "characters",
        "metadata",
    ),
    "title_akas": (
        "title_id",
        "ordering",
        "title",
        "region",
        "language",
        "types",
        "attributes",
        "is_original_title",
        "metadata",
    ),
    "title_episode": (
        "tconst",
        "parent_tconst",
        "season_number",
        "episode_number",
        "metadata",
    ),
}

TABLE_CONVERTERS: dict[str, Mapping[str, Converter]] = {
    "title_basics": {
        "tconst": validate_tconst,
        "title_type": parse_text,
        "primary_title": parse_text,
        "original_title": parse_text,
        "is_adult": parse_bool,
        "start_year": parse_int,
        "end_year": parse_int,
        "runtime_minutes": parse_int,
        "genres": parse_array,
        "metadata": parse_metadata,
    },
    "title_ratings": {
        "tconst": validate_tconst,
        "average_rating": parse_rating,
        "num_votes": parse_int,
        "metadata": parse_metadata,
    },
    "name_basics": {
        "nconst": validate_nconst,
        "primary_name": parse_text,
        "birth_year": parse_int,
        "death_year": parse_int,
        "primary_profession": parse_array,
        "known_for_titles": parse_array,
        "metadata": parse_metadata,
    },
    "title_crew": {
        "tconst": validate_tconst,
        "directors": parse_array,
        "writers": parse_array,
        "metadata": parse_metadata,
    },
    "title_principals": {
        "tconst": validate_tconst,
        "ordering": parse_int,
        "nconst": optional_nconst,
        "category": parse_text,
        "job": parse_text,
        "characters": parse_json_text,
        "metadata": parse_metadata,
    },
    "title_akas": {
        "title_id": validate_tconst,
        "ordering": parse_int,
        "title": parse_text,
        "region": parse_text,
        "language": parse_text,
        "types": parse_array,
        "attributes": parse_array,
        "is_original_title": parse_bool,
        "metadata": parse_metadata,
    },
    "title_episode": {
        "tconst": validate_tconst,
        "parent_tconst": validate_tconst,
        "season_number": parse_int,
        "episode_number": parse_int,
        "metadata": parse_metadata,
    },
}

REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "title_basics": ("tconst",),
    "title_ratings": ("tconst",),
    "name_basics": ("nconst",),
    "title_crew": ("tconst",),
    "title_principals": ("tconst", "ordering"),
    "title_akas": ("title_id", "ordering"),
    "title_episode": ("tconst", "parent_tconst"),
}


def normalize_tsv_row(
    row: Mapping[str, Any],
    spec: DatasetSpec,
    *,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in spec.fields:
        if field.source not in row:
            raise ValueError(f"missing source column {field.source!r}")
        converted = field.converter(row[field.source])
        if field.required and converted is None:
            raise ValueError(f"required field {field.source!r} is null")
        result[field.target] = converted
    result["metadata"] = metadata or {}
    return result


def normalize_message_data(table_name: str, data: Any) -> tuple[Any, ...]:
    if table_name not in TABLE_COLUMNS:
        raise ValueError(f"unsupported target table: {table_name!r}")
    if not isinstance(data, dict):
        raise ValueError("message data must be a JSON object")

    converters = TABLE_CONVERTERS[table_name]
    missing = [name for name in REQUIRED_COLUMNS[table_name] if data.get(name) is None]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")

    row: list[Any] = []
    for column in TABLE_COLUMNS[table_name]:
        raw = data.get(column, {} if column == "metadata" else None)
        row.append(converters[column](raw))
    return tuple(row)
