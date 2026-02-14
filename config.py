"""Benchmark configuration."""

from dataclasses import dataclass, field


@dataclass
class PostgresConfig:
    host: str = "localhost"
    port: int = 5432
    user: str = "bench"
    password: str = "bench"
    database: str = "bench"


@dataclass
class MysqlConfig:
    host: str = "localhost"
    port: int = 3306
    user: str = "bench"
    password: str = "bench"
    database: str = "bench"


@dataclass
class BenchmarkConfig:
    # Data scale
    total_rows: int = 30_000_000_000  # 30 billion
    rows_per_second: int = 1000       # ~1000 rows per second of expiry

    # Query parameters
    fetch_window_seconds: int = 300   # 5 minutes
    cursor_batch_size: int = 10_000   # rows per cursor fetch

    # Partitioning
    partition_interval_hours: int = 1  # 1-hour partitions

    # Bucket column
    bucket_interval_seconds: int = 300  # 5-minute buckets

    # Benchmark run
    warmup_rounds: int = 1
    benchmark_rounds: int = 5

    # Subset mode — for realistic local testing without 30B rows.
    # Set total_rows to a smaller value (e.g. 10_000_000) and
    # adjust rows_per_second proportionally.
    # The schema and queries remain identical to full-scale.

    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    mysql: MysqlConfig = field(default_factory=MysqlConfig)
