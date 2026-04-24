from __future__ import annotations

from typing import Any

import pandas as pd

from .base import BaseValidator, ValidationResult


class JoinValidator(BaseValidator):
    check_name = "join"
    join_key = "Org Name"

    def run(self, df: pd.DataFrame, config: dict[str, Any] | None = None) -> ValidationResult:
        config = config or {}
        if not self._is_reconcile_config(config):
            return self._result(
                status="skipped",
                severity="info",
                message="Join validation only applies to RECONCILE datasets.",
            )

        duplicate_count, duplicate_samples = self._find_duplicate_keys(df)
        row_explosion = self._detect_row_explosion(df, config)

        details: dict[str, Any] = {
            "join_key": self.join_key,
            "row_count_output": int(len(df)),
        }
        if duplicate_count:
            details["duplicate_key_count"] = duplicate_count
            details["duplicate_key_samples"] = duplicate_samples
        if row_explosion is not None:
            details.update(row_explosion)

        failed = duplicate_count > 0 or bool(row_explosion and row_explosion["row_explosion_detected"])
        if failed:
            return self._result(
                status="fail",
                severity="error",
                details=details,
                message="Join validation failed due to duplicate keys or row explosion.",
            )

        return self._result(
            status="pass",
            severity="info",
            details=details,
            message="Join validation passed.",
        )

    def _is_reconcile_config(self, config: dict[str, Any]) -> bool:
        process_type = str(config.get("process_type") or "").upper()
        dataset = config.get("dataset")
        if not process_type and dataset is not None:
            process_type = str(getattr(dataset, "process_type", "")).upper()
        return process_type == "RECONCILE"

    def _find_duplicate_keys(self, df: pd.DataFrame) -> tuple[int, list[str]]:
        if self.join_key not in df.columns:
            return 0, []

        non_null = df[self.join_key].dropna()
        duplicate_values = non_null[non_null.duplicated(keep=False)]
        unique_duplicates = pd.unique(duplicate_values.astype(str))
        return int(len(unique_duplicates)), unique_duplicates[:5].tolist()

    def _detect_row_explosion(
        self,
        df: pd.DataFrame,
        config: dict[str, Any],
    ) -> dict[str, Any] | None:
        input_row_count = config.get("input_row_count")
        if input_row_count in (None, 0):
            return None

        multiplier = float(config.get("max_row_multiplier", 1.5))
        output_row_count = int(len(df))
        threshold = int(input_row_count * multiplier)
        return {
            "row_count_input": int(input_row_count),
            "max_row_multiplier": multiplier,
            "max_allowed_rows": threshold,
            "row_explosion_detected": output_row_count > threshold,
        }
