"""Spark SQL DataFrame schema for Analytics job ad metadata edits.

Advertiser activity job post edit metadata.

"""
from dataclasses import dataclass
from typing import Optional, Text

from pyspark.sql import Column
from pyspark.sql.types import IntegerType, LongType, StructField, StructType
import pyspark.sql.functions as F


@dataclass(frozen=True)
class RangeFilter:
    """Filtering attributes for the row report processor."""

    column: Text
    lower: Optional[int] = None
    upper: Optional[int] = None
    force_range: bool = False

    @property
    def thresholds_are_valid(self) -> bool:
        """Check if the threshold rules are workable."""

        return self.lower is not None or self.upper is not None

    @staticmethod
    def is_supported_range_condition_types(column: StructField) -> bool:
        """Validate if `column` type supports range calculations.

        Returns:
            `True` if the type is supported. `False` otherwise.

        """
        return isinstance(column.dataType, (IntegerType, LongType))

    def range_filter_clause(self, schema: StructType) -> Optional[Column]:
        """Set up range search clause to filter.

        Checks if the provided column is supported as a range condition in the clause.

        """
        condition: Optional[Column] = None

        if self.force_range or RangeFilter.is_supported_range_condition_types(
            schema[self.column]
        ):
            if self.lower is not None and self.upper is None:
                condition = F.col(self.column) >= int(self.lower)
            elif self.lower is None and self.upper is not None:
                condition = F.col(self.column) <= int(self.upper)
            elif self.lower is not None and self.upper is not None:
                condition = (F.col(self.column) >= int(self.lower)) & (
                    F.col(self.column) <= int(self.upper)
                )

        return condition
