# Partitioning Strategy Benchmarking

Benchmarks read performance of expiry-based queries across four database strategies:

| # | Strategy | Engine | Approach |
|---|----------|--------|----------|
| 1 | `postgres_partition` | PostgreSQL 16 | Native `PARTITION BY RANGE` on `expiry` |
| 2 | `postgres_bucket` | PostgreSQL 16 | Explicit `bucket` column with composite index |
| 3 | `mysql_partition` | MySQL 8 | Native `PARTITION BY RANGE (UNIX_TIMESTAMP(expiry))` |
| 4 | `mysql_bucket` | MySQL 8 | Explicit `bucket` column with composite index |

## Table Schema

```
id      BIGINT     -- monotonically increasing
expiry  TIMESTAMP  -- when this row expires
```

## Query Under Test

> Fetch all IDs where expiry is due in the next 5 minutes, using cursor-based streaming.

```sql
SELECT id FROM <table> WHERE expiry >= :now AND expiry < :now + interval '5 minutes'
```

## Scale

- **30 billion rows** total (configurable for local testing)
- **~1,000 rows per second** of expiry
- Query fetches **~300,000 rows** per execution (5 min × 1000 rows/s)

## Quick Start

```bash
# 1. Start databases
docker compose up -d

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run benchmark (smaller scale for local testing)
python benchmark.py run --total-rows 10000000 --rows-per-second 1000

# 4. Run only specific strategy
python benchmark.py run --strategy postgres_partition --total-rows 1000000

# 5. Separate load and query phases
python benchmark.py load --total-rows 10000000
python benchmark.py query --rounds 10
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `run` | Full lifecycle: setup → load → benchmark → teardown |
| `load` | Setup schemas and load data only |
| `query` | Run benchmark queries (data must be loaded) |
| `teardown` | Drop all benchmark tables |

## Key Options

| Flag | Default | Description |
|------|---------|-------------|
| `--total-rows` | 10,000,000 | Total rows to generate |
| `--rows-per-second` | 1,000 | Rows per second of expiry |
| `--fetch-window` | 300 | Query window in seconds (5 min) |
| `--cursor-batch` | 10,000 | Rows per cursor fetch |
| `--partition-interval` | 1 | Partition width in hours |
| `--bucket-interval` | 300 | Bucket width in seconds |
| `--rounds` | 5 | Measured benchmark rounds |
| `--strategy` | all | Run a single strategy by name |

## Output

Results are printed as a comparison table and saved as JSON under `results/`.

```
BENCHMARK RESULTS
+--------------------+----------+--------------+----------+----------+----------+-------------------+--------+
| Strategy           | Avg Rows | Avg Time (s) | Min (s)  | P50 (s)  | Max (s)  | Avg 1st Batch (s) | Rounds |
+--------------------+----------+--------------+----------+----------+----------+-------------------+--------+
| postgres_partition | 300,000  | 0.1234       | 0.1100   | 0.1230   | 0.1400   | 0.0150            | 5      |
| postgres_bucket    | 300,000  | 0.0987       | 0.0900   | 0.0980   | 0.1100   | 0.0120            | 5      |
| mysql_partition    | 300,000  | 0.2345       | 0.2200   | 0.2340   | 0.2500   | 0.0200            | 5      |
| mysql_bucket       | 300,000  | 0.1876       | 0.1800   | 0.1870   | 0.1950   | 0.0180            | 5      |
+--------------------+----------+--------------+----------+----------+----------+-------------------+--------+
```
