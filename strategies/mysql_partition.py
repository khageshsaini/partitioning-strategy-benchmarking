"""MySQL with native range partitioning on expiry."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import mysql.connector

from config import BenchmarkConfig
from strategies.base import QueryResult, Strategy

TABLE = "expiry_partition"


class MysqlPartitionStrategy(Strategy):

    @property
    def name(self) -> str:
        return "mysql_partition"

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
        # MySQL requires the partition key in the primary key.
        # We use a composite key (id, expiry) and partition by RANGE on
        # UNIX_TIMESTAMP(expiry) bucketed into hourly ranges.
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id       BIGINT      NOT NULL,
                expiry   DATETIME    NOT NULL,
                PRIMARY KEY (id, expiry),
                INDEX idx_expiry (expiry)
            ) ENGINE=InnoDB
            PARTITION BY RANGE (UNIX_TIMESTAMP(expiry)) (
                PARTITION p_default VALUES LESS THAN MAXVALUE
            );
        """)
        conn.commit()
        cur.close()

    def reorganize_partitions(
        self,
        conn: Any,
        partitions: list[tuple[str, int]],
    ) -> None:
        """Reorganize p_default into concrete hourly partitions.

        partitions: list of (partition_name, upper_bound_epoch)
        """
        if not partitions:
            return
        parts = ", ".join(
            f"PARTITION {name} VALUES LESS THAN ({bound})"
            for name, bound in partitions
        )
        # Keep the catch-all at the end.
        parts += ", PARTITION p_default VALUES LESS THAN MAXVALUE"
        cur = conn.cursor()
        cur.execute(f"""
            ALTER TABLE {TABLE}
            REORGANIZE PARTITION p_default INTO ({parts});
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
        converted = [
            (r[0], datetime.fromtimestamp(r[1], tz=timezone.utc))
            for r in rows
        ]
        cur = conn.cursor()
        cur.executemany(
            f"INSERT INTO {TABLE} (id, expiry) VALUES (%s, %s)",
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
        now_ts = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
        end_ts = datetime.fromtimestamp(
            now_epoch + cfg.fetch_window_seconds, tz=timezone.utc,
        )

        qr = QueryResult()
        t0 = time.perf_counter()

        # MySQL doesn't have server-side named cursors like PostgreSQL.
        # We use buffered=False for streaming row-by-row from the server
        # and manually batch the reads.
        cur = conn.cursor(buffered=False)
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

        cur.close()
        qr.elapsed_seconds = time.perf_counter() - t0
        return qr
