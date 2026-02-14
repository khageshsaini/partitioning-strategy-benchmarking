"""Generate and load test data into each strategy's table.

Data model:
  - 30 billion rows total (configurable via BenchmarkConfig.total_rows)
  - ~1000 rows per second of expiry
  - IDs are monotonically increasing bigints
  - Expiry values are spread uniformly: for each second in the time range
    there are exactly `rows_per_second` rows with that expiry.

The generator works in *streaming batches* so memory stays bounded even at
full scale.  It also creates the necessary partitions ahead of data loading.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from config import BenchmarkConfig
from strategies.base import Strategy
from strategies.pg_partition import PgPartitionStrategy
from strategies.mysql_partition import MysqlPartitionStrategy


# How many rows to buffer before flushing to the database.
INSERT_BATCH_SIZE = 50_000


def _total_seconds(cfg: BenchmarkConfig) -> int:
    """Total number of distinct expiry seconds to cover all rows."""
    return cfg.total_rows // cfg.rows_per_second


def compute_time_range(cfg: BenchmarkConfig) -> tuple[float, float]:
    """Return (start_epoch, end_epoch) for the generated data.

    The range is anchored so that *now* falls roughly in the middle.
    """
    total_sec = _total_seconds(cfg)
    now = time.time()
    start = now - total_sec // 2
    end = start + total_sec
    return start, end


def _create_pg_partitions(
    strategy: PgPartitionStrategy,
    conn: Any,
    cfg: BenchmarkConfig,
    start_epoch: float,
    end_epoch: float,
) -> None:
    """Pre-create hourly partitions for the full data range."""
    interval_s = cfg.partition_interval_hours * 3600
    t = start_epoch
    count = 0
    while t < end_epoch:
        p_start = datetime.fromtimestamp(t, tz=timezone.utc)
        p_end = datetime.fromtimestamp(t + interval_s, tz=timezone.utc)
        name = strategy.create_partition(conn, p_start, p_end)
        strategy.create_partition_index(conn, name)
        t += interval_s
        count += 1
        if count % 500 == 0:
            print(f"  [pg_partition] created {count} partitions …")
    print(f"  [pg_partition] {count} partitions ready.")


def _create_mysql_partitions(
    strategy: MysqlPartitionStrategy,
    conn: Any,
    cfg: BenchmarkConfig,
    start_epoch: float,
    end_epoch: float,
) -> None:
    """Reorganize MySQL default partition into hourly partitions."""
    interval_s = cfg.partition_interval_hours * 3600
    parts: list[tuple[str, int]] = []
    t = start_epoch
    idx = 0
    while t < end_epoch:
        upper = int(t + interval_s)
        ts = datetime.fromtimestamp(t, tz=timezone.utc)
        name = f"p_{ts.strftime('%Y%m%d_%H%M')}"
        parts.append((name, upper))
        t += interval_s
        idx += 1
        if idx % 500 == 0:
            print(f"  [mysql_partition] queued {idx} partitions …")

    print(f"  [mysql_partition] reorganizing into {len(parts)} partitions …")
    strategy.reorganize_partitions(conn, parts)
    print(f"  [mysql_partition] partitions ready.")


def prepare_partitions(
    strategy: Strategy,
    conn: Any,
    cfg: BenchmarkConfig,
    start_epoch: float,
    end_epoch: float,
) -> None:
    """Create partitions if the strategy requires them."""
    if isinstance(strategy, PgPartitionStrategy):
        _create_pg_partitions(strategy, conn, cfg, start_epoch, end_epoch)
    elif isinstance(strategy, MysqlPartitionStrategy):
        _create_mysql_partitions(strategy, conn, cfg, start_epoch, end_epoch)


def generate_rows(
    cfg: BenchmarkConfig,
    start_epoch: float,
    end_epoch: float,
):
    """Yield (id, expiry_epoch) tuples in chronological order.

    For each second in [start_epoch, end_epoch) we emit
    `rows_per_second` rows.
    """
    row_id = 1
    current = int(start_epoch)
    end = int(end_epoch)
    while current < end:
        for _ in range(cfg.rows_per_second):
            yield (row_id, float(current))
            row_id += 1
        current += 1


def load_data(
    strategy: Strategy,
    conn: Any,
    cfg: BenchmarkConfig,
    start_epoch: float,
    end_epoch: float,
    progress_every: int = 5_000_000,
) -> int:
    """Stream-insert generated data into the strategy's table.

    Returns total rows inserted.
    """
    batch: list[tuple[int, float]] = []
    total = 0
    t0 = time.time()

    for row in generate_rows(cfg, start_epoch, end_epoch):
        batch.append(row)
        if len(batch) >= INSERT_BATCH_SIZE:
            strategy.insert_batch(conn, batch, cfg)
            total += len(batch)
            batch.clear()
            if total % progress_every == 0:
                elapsed = time.time() - t0
                rate = total / elapsed if elapsed > 0 else 0
                print(
                    f"  [{strategy.name}] {total:>14,} rows  "
                    f"({rate:,.0f} rows/s)",
                )

    # Flush remainder.
    if batch:
        strategy.insert_batch(conn, batch, cfg)
        total += len(batch)

    elapsed = time.time() - t0
    print(
        f"  [{strategy.name}] DONE — {total:,} rows in {elapsed:.1f}s "
        f"({total / elapsed:,.0f} rows/s)",
    )
    return total
