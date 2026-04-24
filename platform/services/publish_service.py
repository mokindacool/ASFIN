from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine

from ASFINT.Utility.BQ_Helpers import clean_name

from ..core.database import engine as default_engine


@dataclass(slots=True)
class PublishResult:
    table_name: str
    row_count: int
    schema_snapshot: dict[str, str]


class WarehousePublisher(ABC):
    @abstractmethod
    def publish(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        version_number: int,
    ) -> PublishResult:
        """
        Persist a cleaned DataFrame to the warehouse and return publish metadata.
        """


class PostgreSQLWarehousePublisher(WarehousePublisher):
    def __init__(self, engine: Engine | None = None, schema: str | None = None):
        self.engine = engine or default_engine
        self.schema = schema

    def publish(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        version_number: int,
    ) -> PublishResult:
        sanitized_table = clean_name(f"{dataset_name}_v{version_number}")
        sanitized_df = self._sanitize_dataframe(df)

        sanitized_df.to_sql(
            name=sanitized_table,
            con=self.engine,
            schema=self.schema,
            if_exists="replace",
            index=False,
            method="multi",
        )

        return PublishResult(
            table_name=sanitized_table,
            row_count=int(len(sanitized_df)),
            schema_snapshot={column: str(dtype) for column, dtype in sanitized_df.dtypes.items()},
        )

    def _sanitize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        sanitized = df.copy()
        sanitized.columns = self._sanitize_columns(list(df.columns))
        return sanitized

    def _sanitize_columns(self, columns: list[Any]) -> list[str]:
        seen: dict[str, int] = {}
        sanitized_columns: list[str] = []

        for column in columns:
            base = clean_name(str(column))
            count = seen.get(base, 0)
            seen[base] = count + 1
            sanitized_columns.append(base if count == 0 else clean_name(f"{base}_{count}"))

        return sanitized_columns
