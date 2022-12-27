"""Dummy data factory.
"""
from typing import Any, List, Mapping, Optional, Tuple
from pyspark.sql.types import Row, StructType

import diffit.schema.dummy


# pylint: disable=too-few-public-methods
class Data:
    """Dummy data.

    """
    def __init__(self, row_count: int = 2, skew: bool = False):
        self.row_count: int = row_count
        self.skew: bool = skew

    def rows(self) -> List[Tuple[Optional[Any], ...]]:
        """Create a dynamic set of DataFrame rows.

        Row count based on :attr:`row_count`.

        """
        col = 'dummy_col02_val'
        return [
            (i, f'{col}{i+1 if self.skew else i:0>10}') for i in range(1, self.row_count+1)
        ]

    def args(self) -> Mapping[Tuple[None, StructType], StructType]:
        """Return a construct.

        """
        return (map(lambda x: Row(*x), self.rows()),
                diffit.schema.dummy.schema())
