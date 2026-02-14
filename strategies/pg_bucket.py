"""PostgreSQL with an explicit bucket column (no partitioning)."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import tuple_row

from config import BenchmarkConfig
from strategies.base import QueryResult, Strategy

TABLE = "expiry_bucket"


def _epoch_to_bucket(epoch: float, interval: int) -> int:
    """Map an epoch timestamp to a bucket id."""
    return int(epoch) // interval


class PgBucketStrategy(Strategy):

    @property
    def name(self) -> str:
        return "postgres_bucket"

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
                    expiry   TIMESTAMP NOT NULL,
                    bucket   BIGINT    NOT NULL
                );
            """)
            # Composite index: bucket first for equality, then expiry for range.
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{TABLE}_bucket_expiry
                    ON {TABLE} (bucket, expiry);
            """)
            conn.commit()

    def teardown_schema(self, conn: Any, cfg: BenchmarkConfig) -> None:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {TABLE};")
            conn.commit()

    # ------------------------------------------------------------------

    def insert_batch(
        self,
        conn: Any,
        rows: list[tuple[int, float]],
        cfg: BenchmarkConfig,
    ) -> None:
        interval = cfg.bucket_interval_seconds
        converted = [
            (
                r[0],
                datetime.fromtimestamp(r[1], tz=timezone.utc),
                _epoch_to_bucket(r[1], interval),
            )
            for r in rows
        ]
        with conn.cursor() as cur:
            with cur.copy(f"COPY {TABLE} (id, expiry, bucket) FROM STDIN") as copy:
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
        interval = cfg.bucket_interval_seconds
        now_ts = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
        end_ts = datetime.fromtimestamp(
            now_epoch + cfg.fetch_window_seconds, tz=timezone.utc,
        )

        # Determine which buckets overlap with [now, now + window).
        bucket_start = _epoch_to_bucket(now_epoch, interval)
        bucket_end = _epoch_to_bucket(now_epoch + cfg.fetch_window_seconds, interval)
        buckets = list(range(bucket_start, bucket_end + 1))

        qr = QueryResult()
        t0 = time.perf_counter()

        with conn.cursor(name="fetch_expiring_bucket") as cur:
            cur.itersize = cfg.cursor_batch_size
            cur.execute(
                f"""SELECT id FROM {TABLE}
                    WHERE bucket = ANY(%s)
                      AND expiry >= %s
                      AND expiry < %s""",
                (buckets, now_ts, end_ts),
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
