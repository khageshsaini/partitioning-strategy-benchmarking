"""Collect and display benchmark results."""

from __future__ import annotations

import json
import os
from datetime import datetime

from tabulate import tabulate

from strategies.base import BenchmarkResult


def print_results(results: list[BenchmarkResult]) -> None:
    """Pretty-print a comparison table to stdout."""
    headers = [
        "Strategy",
        "Avg Rows",
        "Avg Time (s)",
        "Min (s)",
        "P50 (s)",
        "Max (s)",
        "Avg 1st Batch (s)",
        "Rounds",
    ]
    rows = []
    for r in results:
        rows.append([
            r.strategy_name,
            f"{r.avg_rows:,.0f}",
            f"{r.avg_elapsed:.4f}",
            f"{r.min_elapsed:.4f}",
            f"{r.p50_elapsed:.4f}",
            f"{r.max_elapsed:.4f}",
            f"{r.avg_first_batch:.4f}",
            len(r.results),
        ])

    print("\n" + "=" * 80)
    print("BENCHMARK RESULTS")
    print("=" * 80)
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    print()


def save_results(
    results: list[BenchmarkResult],
    output_dir: str = "results",
) -> str:
    """Save raw results as JSON. Returns the file path."""
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"benchmark_{ts}.json")

    data = []
    for r in results:
        data.append({
            "strategy": r.strategy_name,
            "warmup": [
                {
                    "rows": w.total_rows_fetched,
                    "batches": w.fetch_batches,
                    "elapsed_s": w.elapsed_seconds,
                    "first_batch_s": w.first_batch_seconds,
                }
                for w in r.warmup_results
            ],
            "rounds": [
                {
                    "rows": rr.total_rows_fetched,
                    "batches": rr.fetch_batches,
                    "elapsed_s": rr.elapsed_seconds,
                    "first_batch_s": rr.first_batch_seconds,
                }
                for rr in r.results
            ],
            "summary": {
                "avg_elapsed_s": r.avg_elapsed,
                "min_elapsed_s": r.min_elapsed,
                "p50_elapsed_s": r.p50_elapsed,
                "max_elapsed_s": r.max_elapsed,
                "avg_first_batch_s": r.avg_first_batch,
                "avg_rows": r.avg_rows,
            },
        })

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Results saved to {path}")
    return path
