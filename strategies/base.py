"""Base strategy interface."""

from __future__ import annotations

import abc
import time
from dataclasses import dataclass, field
from typing import Any

from config import BenchmarkConfig


@dataclass
class QueryResult:
    """Result of a single benchmark query execution."""
    total_rows_fetched: int = 0
    fetch_batches: int = 0
    elapsed_seconds: float = 0.0
    first_batch_seconds: float = 0.0


@dataclass
class BenchmarkResult:
    """Aggregated benchmark results for a strategy."""
    strategy_name: str = ""
    warmup_results: list[QueryResult] = field(default_factory=list)
    results: list[QueryResult] = field(default_factory=list)

    @property
    def avg_elapsed(self) -> float:
        if not self.results:
            return 0.0
        return sum(r.elapsed_seconds for r in self.results) / len(self.results)

    @property
    def avg_first_batch(self) -> float:
        if not self.results:
            return 0.0
        return sum(r.first_batch_seconds for r in self.results) / len(self.results)

    @property
    def avg_rows(self) -> float:
        if not self.results:
            return 0.0
        return sum(r.total_rows_fetched for r in self.results) / len(self.results)

    @property
    def min_elapsed(self) -> float:
        if not self.results:
            return 0.0
        return min(r.elapsed_seconds for r in self.results)

    @property
    def max_elapsed(self) -> float:
        if not self.results:
            return 0.0
        return max(r.elapsed_seconds for r in self.results)

    @property
    def p50_elapsed(self) -> float:
        if not self.results:
            return 0.0
        s = sorted(r.elapsed_seconds for r in self.results)
        mid = len(s) // 2
        return s[mid]


class Strategy(abc.ABC):
    """Abstract base for a benchmarking strategy."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Human-readable strategy name."""

    @property
    @abc.abstractmethod
    def engine(self) -> str:
        """'postgres' or 'mysql'."""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def connect(self, cfg: BenchmarkConfig) -> Any:
        """Return a DB connection."""

    @abc.abstractmethod
    def close(self, conn: Any) -> None:
        """Close the connection."""

    @abc.abstractmethod
    def setup_schema(self, conn: Any, cfg: BenchmarkConfig) -> None:
        """Create the table(s) / partitions. Idempotent."""

    @abc.abstractmethod
    def teardown_schema(self, conn: Any, cfg: BenchmarkConfig) -> None:
        """Drop everything. Idempotent."""

    @abc.abstractmethod
    def insert_batch(
        self,
        conn: Any,
        rows: list[tuple[int, float]],
        cfg: BenchmarkConfig,
    ) -> None:
        """Insert a batch of (id, expiry_epoch) rows."""

    @abc.abstractmethod
    def fetch_expiring_ids(
        self,
        conn: Any,
        cfg: BenchmarkConfig,
        now_epoch: float,
    ) -> QueryResult:
        """Fetch all ids with expiry in [now, now + window) using a cursor.

        Returns a QueryResult with timing and row-count information.
        """

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def run_benchmark(
        self,
        cfg: BenchmarkConfig,
        now_epoch: float,
    ) -> BenchmarkResult:
        """Run the full benchmark (warmup + measured rounds)."""
        conn = self.connect(cfg)
        result = BenchmarkResult(strategy_name=self.name)
        try:
            # Warmup
            for _ in range(cfg.warmup_rounds):
                qr = self.fetch_expiring_ids(conn, cfg, now_epoch)
                result.warmup_results.append(qr)

            # Measured rounds
            for _ in range(cfg.benchmark_rounds):
                qr = self.fetch_expiring_ids(conn, cfg, now_epoch)
                result.results.append(qr)
        finally:
            self.close(conn)
        return result
