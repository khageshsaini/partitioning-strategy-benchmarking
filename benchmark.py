#!/usr/bin/env python3
"""Main entry-point for the partitioning strategy benchmark.

Usage examples
--------------
  # Full lifecycle: setup schemas, load data, run benchmark, teardown
  python benchmark.py run --total-rows 10000000 --rows-per-second 1000

  # Only run the query benchmark (assumes data is already loaded)
  python benchmark.py query --rounds 10

  # Load data only (useful to separate load phase from query phase)
  python benchmark.py load --total-rows 10000000

  # Teardown all tables
  python benchmark.py teardown

  # Run a specific strategy
  python benchmark.py run --strategy postgres_partition --total-rows 1000000
"""

from __future__ import annotations

import sys
import time

import click

from config import BenchmarkConfig
from data_generator import compute_time_range, load_data, prepare_partitions
from reporter import print_results, save_results
from strategies import ALL_STRATEGIES
from strategies.base import BenchmarkResult, Strategy


def _resolve_strategies(name: str | None) -> list[Strategy]:
    if name is None:
        return list(ALL_STRATEGIES)
    for s in ALL_STRATEGIES:
        if s.name == name:
            return [s]
    available = ", ".join(s.name for s in ALL_STRATEGIES)
    click.echo(f"Unknown strategy '{name}'. Available: {available}", err=True)
    sys.exit(1)


def _build_config(
    total_rows: int,
    rows_per_second: int,
    fetch_window: int,
    cursor_batch: int,
    partition_interval: int,
    bucket_interval: int,
    warmup: int,
    rounds: int,
    pg_host: str,
    pg_port: int,
    mysql_host: str,
    mysql_port: int,
) -> BenchmarkConfig:
    cfg = BenchmarkConfig(
        total_rows=total_rows,
        rows_per_second=rows_per_second,
        fetch_window_seconds=fetch_window,
        cursor_batch_size=cursor_batch,
        partition_interval_hours=partition_interval,
        bucket_interval_seconds=bucket_interval,
        warmup_rounds=warmup,
        benchmark_rounds=rounds,
    )
    cfg.postgres.host = pg_host
    cfg.postgres.port = pg_port
    cfg.mysql.host = mysql_host
    cfg.mysql.port = mysql_port
    return cfg


# ---- Common CLI options ------------------------------------------------

_common_options = [
    click.option("--total-rows", default=10_000_000, show_default=True,
                 help="Total rows to generate."),
    click.option("--rows-per-second", default=1000, show_default=True,
                 help="Rows per second of expiry."),
    click.option("--fetch-window", default=300, show_default=True,
                 help="Fetch window in seconds (default 5 min)."),
    click.option("--cursor-batch", default=10_000, show_default=True,
                 help="Rows per cursor fetch batch."),
    click.option("--partition-interval", default=1, show_default=True,
                 help="Partition interval in hours."),
    click.option("--bucket-interval", default=300, show_default=True,
                 help="Bucket interval in seconds."),
    click.option("--warmup", default=1, show_default=True,
                 help="Warmup query rounds."),
    click.option("--rounds", default=5, show_default=True,
                 help="Measured query rounds."),
    click.option("--pg-host", default="localhost", show_default=True),
    click.option("--pg-port", default=5432, show_default=True),
    click.option("--mysql-host", default="localhost", show_default=True),
    click.option("--mysql-port", default=3306, show_default=True),
    click.option("--strategy", default=None,
                 help="Run only this strategy (e.g. postgres_partition)."),
]


def add_options(options):
    def wrapper(func):
        for opt in reversed(options):
            func = opt(func)
        return func
    return wrapper


# ---- CLI ---------------------------------------------------------------

@click.group()
def cli():
    """Benchmark read performance of expiry-based queries."""


@cli.command()
@add_options(_common_options)
def run(strategy, **kwargs):
    """Full lifecycle: setup -> load -> benchmark -> teardown."""
    cfg = _build_config(**{k: v for k, v in kwargs.items() if k != "strategy"})
    strategies = _resolve_strategies(strategy)
    start_epoch, end_epoch = compute_time_range(cfg)
    now_epoch = (start_epoch + end_epoch) / 2  # "now" is midpoint

    results: list[BenchmarkResult] = []

    for strat in strategies:
        click.echo(f"\n{'='*60}")
        click.echo(f"Strategy: {strat.name}")
        click.echo(f"{'='*60}")

        conn = strat.connect(cfg)
        try:
            # Setup
            click.echo("  Setting up schema …")
            strat.teardown_schema(conn, cfg)
            strat.setup_schema(conn, cfg)

            # Partitions
            click.echo("  Creating partitions …")
            prepare_partitions(strat, conn, cfg, start_epoch, end_epoch)

            # Load
            click.echo("  Loading data …")
            load_data(strat, conn, cfg, start_epoch, end_epoch)
        finally:
            strat.close(conn)

        # Benchmark
        click.echo("  Running benchmark …")
        br = strat.run_benchmark(cfg, now_epoch)
        results.append(br)
        click.echo(
            f"  => avg {br.avg_elapsed:.4f}s  |  "
            f"first-batch {br.avg_first_batch:.4f}s  |  "
            f"rows {br.avg_rows:,.0f}"
        )

        # Teardown
        conn = strat.connect(cfg)
        try:
            strat.teardown_schema(conn, cfg)
        finally:
            strat.close(conn)

    print_results(results)
    save_results(results)


@cli.command()
@add_options(_common_options)
def load(strategy, **kwargs):
    """Set up schemas and load data (no benchmark query)."""
    cfg = _build_config(**{k: v for k, v in kwargs.items() if k != "strategy"})
    strategies = _resolve_strategies(strategy)
    start_epoch, end_epoch = compute_time_range(cfg)

    for strat in strategies:
        click.echo(f"\n--- {strat.name} ---")
        conn = strat.connect(cfg)
        try:
            strat.teardown_schema(conn, cfg)
            strat.setup_schema(conn, cfg)
            prepare_partitions(strat, conn, cfg, start_epoch, end_epoch)
            load_data(strat, conn, cfg, start_epoch, end_epoch)
        finally:
            strat.close(conn)

    click.echo("\nData loading complete.")


@cli.command()
@add_options(_common_options)
def query(strategy, **kwargs):
    """Run benchmark queries only (data must already be loaded)."""
    cfg = _build_config(**{k: v for k, v in kwargs.items() if k != "strategy"})
    strategies = _resolve_strategies(strategy)
    start_epoch, end_epoch = compute_time_range(cfg)
    now_epoch = (start_epoch + end_epoch) / 2

    results: list[BenchmarkResult] = []
    for strat in strategies:
        click.echo(f"\n--- {strat.name} ---")
        br = strat.run_benchmark(cfg, now_epoch)
        results.append(br)

    print_results(results)
    save_results(results)


@cli.command()
@add_options(_common_options)
def teardown(strategy, **kwargs):
    """Drop all benchmark tables."""
    cfg = _build_config(**{k: v for k, v in kwargs.items() if k != "strategy"})
    strategies = _resolve_strategies(strategy)

    for strat in strategies:
        click.echo(f"  Tearing down {strat.name} …")
        conn = strat.connect(cfg)
        try:
            strat.teardown_schema(conn, cfg)
        finally:
            strat.close(conn)

    click.echo("Teardown complete.")


if __name__ == "__main__":
    cli()
