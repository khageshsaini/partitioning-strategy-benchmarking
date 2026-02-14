"""MySQL with an explicit bucket column (no partitioning)."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import mysql.connector

from config import BenchmarkConfig
from strategies.base import QueryResult, Strategy

TABLE = "expiry_bucket"


def _epoch_to_bucket(epoch: float, interval: int) -> int:
    return int(epoch) // interval


class MysqlBucketStrategy(Strategy):

    @property
    def name(self) -> str:
        return "mysql_bucket"

    @property
    def engine(self) -> str:
        return "mysql"

    # ------------------------------------------------------------------

    def connect(self, cfg: BenchmarkConfig) -> Any:
        return mysql.connector.connect(
            host=cfg.mysql.host,
            port=cfg.mysql.port,
            user=cfg.mysql.user,
            password=cfg.mysql.password,
            database=cfg.mysql.database,
        )

    def close(self, conn: Any) -> None:
        conn.close()

    # ------------------------------------------------------------------

    def setup_schema(self, conn: Any, cfg: BenchmarkConfig) -> None:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id       BIGINT   NOT NULL,
                expiry   DATETIME NOT NULL,
                bucket   BIGINT   NOT NULL,
                INDEX idx_bucket_expiry (bucket, expiry)
            ) ENGINE=InnoDB;
        """)
        conn.commit()
        cur.close()

    def teardown_schema(self, conn: Any, cfg: BenchmarkConfig) -> None:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {TABLE};")
        conn.commit()
        cur.close()

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
        cur = conn.cursor()
        cur.executemany(
            f"INSERT INTO {TABLE} (id, expiry, bucket) VALUES (%s, %s, %s)",
            converted,
        )
        conn.commit()
        cur.close()

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

        bucket_start = _epoch_to_bucket(now_epoch, interval)
        bucket_end = _epoch_to_bucket(now_epoch + cfg.fetch_window_seconds, interval)
        buckets = list(range(bucket_start, bucket_end + 1))

        placeholders = ", ".join(["%s"] * len(buckets))

        qr = QueryResult()
        t0 = time.perf_counter()

        cur = conn.cursor(buffered=False)
        cur.execute(
            f"""SELECT id FROM {TABLE}
                WHERE bucket IN ({placeholders})
                  AND expiry >= %s
                  AND expiry < %s""",
            (*buckets, now_ts, end_ts),
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

        cur.close()
        qr.elapsed_seconds = time.perf_counter() - t0
        return qr
