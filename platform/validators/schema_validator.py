from __future__ import annotations

from typing import Any

import pandas as pd

from .base import BaseValidator, ValidationResult


class SchemaValidator(BaseValidator):
    check_name = "schema"

    def run(self, df: pd.DataFrame, config: dict[str, Any] | None = None) -> ValidationResult:
        config = config or {}
        schema_def = self._extract_schema_def(config)

        if not schema_def:
            return self._result(
                status="skipped",
                severity="warning",
                message="No schema_def provided; schema validation skipped.",
            )

        expected_columns = [col["name"] for col in schema_def]
        missing_columns = [name for name in expected_columns if name not in df.columns]
        if missing_columns:
            return self._result(
                status="fail",
                severity="error",
                details={"missing_columns": missing_columns},
                message=f"Missing required columns: {missing_columns}",
            )

        type_errors: list[dict[str, Any]] = []
        for column in schema_def:
            name = column["name"]
            dtype = str(column.get("dtype", "")).strip().lower()
            if not dtype:
                continue

            invalid_count, sample_values = self._count_uncoercible(df[name], dtype)
            if invalid_count > 0:
                type_errors.append(
                    {
                        "column": name,
                        "dtype": dtype,
                        "invalid_count": invalid_count,
                        "sample_values": sample_values,
                    }
                )

        if type_errors:
            return self._result(
                status="fail",
                severity="error",
                details={"type_errors": type_errors},
                message="One or more columns could not be coerced to the declared dtype.",
            )

        return self._result(
            status="pass",
            severity="info",
            details={"validated_columns": expected_columns},
            message="Schema validation passed.",
        )

    def _extract_schema_def(self, config: dict[str, Any]) -> list[dict[str, Any]]:
        if "schema_def" in config and config["schema_def"] is not None:
            return [self._normalize_column_def(col) for col in config["schema_def"]]

        dataset = config.get("dataset")
        if dataset is not None and getattr(dataset, "schema_def", None) is not None:
            return [self._normalize_column_def(col) for col in dataset.schema_def]

        return []

    def _normalize_column_def(self, column: Any) -> dict[str, Any]:
        if hasattr(column, "model_dump"):
            return column.model_dump()
        if isinstance(column, dict):
            return column
        raise TypeError(f"Unsupported schema_def entry type: {type(column)}")

    def _count_uncoercible(self, series: pd.Series, dtype: str) -> tuple[int, list[Any]]:
        non_null = series.dropna()
        if non_null.empty:
            return 0, []

        if dtype in {"int", "integer"}:
            coerced = pd.to_numeric(non_null, errors="coerce")
            invalid_mask = coerced.isna() | (coerced % 1 != 0)
        elif dtype in {"float", "double", "number", "numeric"}:
            coerced = pd.to_numeric(non_null, errors="coerce")
            invalid_mask = coerced.isna()
        elif dtype in {"bool", "boolean"}:
            lowered = non_null.astype(str).str.strip().str.lower()
            valid_values = {"true", "false", "1", "0", "yes", "no", "y", "n", "t", "f"}
            invalid_mask = ~lowered.isin(valid_values)
        elif dtype in {"datetime", "timestamp", "date"}:
            coerced = pd.to_datetime(non_null, errors="coerce")
            invalid_mask = coerced.isna()
        elif dtype in {"str", "string", "text", "object"}:
            invalid_mask = pd.Series(False, index=non_null.index)
        else:
            # Unknown declared dtypes are treated as pass-through for now.
            invalid_mask = pd.Series(False, index=non_null.index)

        invalid_values = non_null[invalid_mask]
        return int(invalid_values.shape[0]), invalid_values.head(5).tolist()
