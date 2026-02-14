"""PostgreSQL with native range partitioning on expiry."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import tuple_row

from config import BenchmarkConfig
from strategies.base import QueryResult, Strategy

TABLE = "expiry_partition"


class PgPartitionStrategy(Strategy):

    @property
    def name(self) -> str:
        return "postgres_partition"

    @property
    def engine(self) -> str:
        return "postgres"

    # ------------------------------------------------------------------

    def connect(self, cfg: BenchmarkConfig) -> Any:
        return psycopg.connect(
            host=cfg.postgres.host,
            port=cfg.postgres.port,
            user=cfg.postgres.user,
            password=cfg.postgres.password,
            dbname=cfg.postgres.database,
            row_factory=tuple_row,
        )

    def close(self, conn: Any) -> None:
        conn.close()

    # ------------------------------------------------------------------

    def setup_schema(self, conn: Any, cfg: BenchmarkConfig) -> None:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    id       BIGINT    NOT NULL,
                    expiry   TIMESTAMP NOT NULL
                ) PARTITION BY RANGE (expiry);
            """)
            conn.commit()

    def create_partition(
        self, conn: Any, start: datetime, end: datetime,
    ) -> str:
        """Create a single partition covering [start, end). Returns partition name."""
        name = f"{TABLE}_{start.strftime('%Y%m%d_%H%M')}"
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {name}
                    PARTITION OF {TABLE}
                    FOR VALUES FROM (%s) TO (%s);
            """, (start, end))
            conn.commit()
        return name

    def create_partition_index(self, conn: Any, partition_name: str) -> None:
        idx = f"idx_{partition_name}_expiry"
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS {idx}
                    ON {partition_name} (expiry);
            """)
            conn.commit()

    def teardown_schema(self, conn: Any, cfg: BenchmarkConfig) -> None:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {TABLE} CASCADE;")
            conn.commit()

    # ------------------------------------------------------------------

    def insert_batch(
        self,
        conn: Any,
        rows: list[tuple[int, float]],
        cfg: BenchmarkConfig,
    ) -> None:
        converted = [
            (r[0], datetime.fromtimestamp(r[1], tz=timezone.utc))
            for r in rows
        ]
        with conn.cursor() as cur:
            with cur.copy(f"COPY {TABLE} (id, expiry) FROM STDIN") as copy:
                for row in converted:
                    copy.write_row(row)
            conn.commit()

    # ------------------------------------------------------------------

    def fetch_expiring_ids(
        self,
        conn: Any,
        cfg: BenchmarkConfig,
        now_epoch: float,
    ) -> QueryResult:
        now_ts = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
        end_ts = datetime.fromtimestamp(
            now_epoch + cfg.fetch_window_seconds, tz=timezone.utc,
        )

        qr = QueryResult()
        t0 = time.perf_counter()

        # Use a server-side named cursor for true cursor-based fetching.
        with conn.cursor(name="fetch_expiring") as cur:
            cur.itersize = cfg.cursor_batch_size
            cur.execute(
                f"SELECT id FROM {TABLE} WHERE expiry >= %s AND expiry < %s",
                (now_ts, end_ts),
            )
            first_batch = True
            while True:
                batch = cur.fetchmany(cfg.cursor_batch_size)
                if not batch:
                    break
                qr.total_rows_fetched += len(batch)
                qr.fetch_batches += 1
                if first_batch:
                    qr.first_batch_seconds = time.perf_counter() - t0
                    first_batch = False

        qr.elapsed_seconds = time.perf_counter() - t0
        return qr
