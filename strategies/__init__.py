"""Database partitioning strategies for benchmarking."""

from strategies.base import Strategy
from strategies.pg_partition import PgPartitionStrategy
from strategies.pg_bucket import PgBucketStrategy
from strategies.mysql_partition import MysqlPartitionStrategy
from strategies.mysql_bucket import MysqlBucketStrategy

ALL_STRATEGIES: list[Strategy] = [
    PgPartitionStrategy(),
    PgBucketStrategy(),
    MysqlPartitionStrategy(),
    MysqlBucketStrategy(),
]

__all__ = [
    "Strategy",
    "PgPartitionStrategy",
    "PgBucketStrategy",
    "MysqlPartitionStrategy",
    "MysqlBucketStrategy",
    "ALL_STRATEGIES",
]
