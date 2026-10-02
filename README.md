# IMDb PostgreSQL Queue & Query Optimization

A PostgreSQL database engineering project built around the public IMDb datasets.  
The project combines a **PL/pgSQL message queue**, a **Python producer-consumer ingestion pipeline**, **JSONB**, database indexing, and **EXPLAIN ANALYZE**-based query optimization.

## Highlights

- PostgreSQL 16 environment managed with Docker Compose
- IMDb TSV ingestion through a database-backed message queue
- Queue operations implemented in PL/pgSQL:
  - `enqueue`
  - `dequeue`
  - `ack`
  - `nack`
- Concurrent-safe message claiming using `FOR UPDATE SKIP LOCKED`
- Visibility timeout, retry tracking, exponential backoff, queue metrics, and cleanup of completed messages
- Python producer with:
  - batched enqueue operations
  - restart-safe progress checkpoints
  - malformed-row handling
  - configurable queue backpressure
- Python consumer with:
  - batched inserts
  - `ON CONFLICT DO NOTHING`
  - validation and permanent/transient failure handling
  - savepoints and ACK/NACK processing
- PostgreSQL arrays and JSONB fields
- B-tree, partial, covering, and GIN indexes
- Eight analytical IMDb query scenarios
- Query-plan analysis using `EXPLAIN ANALYZE`

## Project Structure

```text
.
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── Report.pdf
├── scripts/
│   ├── common.py
│   ├── producer.py
│   └── consumer.py
└── sql/
    ├── init.sql
    ├── queue_functions.sql
    ├── queue_tests.sql
    ├── queries.sql
    ├── scenarios.sql
    ├── indexes.sql
    └── drop_analytical_indexes.sql
```

## Database Design

The PostgreSQL schema stores the main IMDb datasets, including:

- `title_basics`
- `title_ratings`
- `name_basics`
- `title_crew`
- `title_principals`
- `title_akas`
- `title_episode`

Several columns are represented using PostgreSQL arrays, while additional flexible metadata is stored in `JSONB`.

## PostgreSQL Message Queue

The ingestion pipeline uses PostgreSQL itself as a message queue.

Each queue message contains a JSONB payload describing the destination table and normalized IMDb record.

A simplified message looks like:

```json
{
  "table": "title_basics",
  "data": {
    "tconst": "tt1234567",
    "title_type": "movie",
    "primary_title": "Example Movie"
  }
}
```

Messages move through the following states:

```text
pending -> processing -> done
                      \-> failed
```

`dequeue` uses `FOR UPDATE SKIP LOCKED` so multiple consumers can safely claim different messages without blocking one another.

The queue implementation also supports:

- visibility timeouts
- retry counts
- maximum-attempt limits
- exponential retry backoff
- queue metrics
- purging completed messages

## Producer

`scripts/producer.py` reads IMDb `.tsv` or `.tsv.gz` files, validates and normalizes each row, and enqueues records in batches.

It also includes:

- progress checkpoints for restart safety
- source-file identity checks
- malformed-row logging
- high-water / low-water backpressure
- configurable batching

## Consumer

`scripts/consumer.py` dequeues messages in batches and inserts them into their corresponding PostgreSQL tables.

The consumer uses batched `execute_values` inserts and idempotent constraints such as:

```sql
ON CONFLICT DO NOTHING
```

Successfully processed messages are ACKed; invalid or failed messages are NACKed.

## Analytical Queries

The project includes eight IMDb analysis scenarios:

1. Average movie rating by genre
2. Top movies by number of votes
3. Directors with the most movies
4. Yearly movie-rating trends
5. Actors appearing across the most genres
6. TV series with the most seasons
7. Most common genre pairs
8. Longest movies by genre

The scenarios are defined in:

```text
sql/scenarios.sql
```

## Indexing and Optimization

`sql/indexes.sql` contains workload-oriented indexes, including:

- partial B-tree indexes
- covering indexes with `INCLUDE`
- GIN indexes for arrays
- GIN indexing for JSONB

Query behavior can be inspected before and after index creation using:

```sql
EXPLAIN ANALYZE
```

To remove the analytical indexes:

```text
sql/drop_analytical_indexes.sql
```

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/IMDb-PostgreSQL-Optimization.git
cd IMDb-PostgreSQL-Optimization
```

### 2. Create the environment file

Linux/macOS:

```bash
cp .env.example .env
```

Windows:

```powershell
Copy-Item .env.example .env
```

Then edit `.env` and set a local PostgreSQL password.

> `.env` is intentionally excluded from version control.

### 3. Create the Docker volume

The Compose file uses an external persistent volume:

```bash
docker volume create imdb-postgres-data
```

### 4. Start PostgreSQL

```bash
docker compose up -d
```

### 5. Install Python dependencies

```bash
python -m venv .venv
```

Activate the environment and run:

```bash
pip install -r requirements.txt
```

### 6. Download IMDb datasets

Download the required non-commercial IMDb TSV datasets from:

https://datasets.imdbws.com/

Place the `.tsv.gz` files inside:

```text
data/
```

The `data/` directory is excluded from Git because the datasets are large and can be downloaded from IMDb directly.

## Running the Pipeline

Start the consumer in one terminal:

```bash
python scripts/consumer.py
```

Run the producer in another terminal:

```bash
python scripts/producer.py
```

For a small test run:

```bash
python scripts/producer.py --max-rows 1000
```

You can also select individual datasets:

```bash
python scripts/producer.py --files title_basics name_basics
```

## Queue Tests

Queue behavior can be tested with:

```text
sql/queue_tests.sql
```

## Query Optimization

Run the analytical scenarios:

```bash
psql -d imdb -f sql/scenarios.sql
```

Create the workload-specific indexes:

```bash
psql -d imdb -f sql/indexes.sql
```

Then rerun the scenarios and compare the `EXPLAIN ANALYZE` plans.

## Report

The complete academic report is available in:

[Report.pdf](Report.pdf)

## Technologies

- PostgreSQL 16
- PL/pgSQL
- Python
- psycopg2
- Docker / Docker Compose
- JSONB
- SQL query optimization
- IMDb datasets

## Authors

- Yasaman Kavianpour
- Sobhan Aram
- AmirMehdi Vaziri
