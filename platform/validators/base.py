from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(slots=True)
class ValidationResult:
    check_name: str
    status: str
    severity: str
    details: dict[str, Any] = field(default_factory=dict)
    message: str | None = None


class BaseValidator(ABC):
    check_name: str

    @abstractmethod
    def run(self, df: pd.DataFrame, config: dict[str, Any] | None = None) -> ValidationResult:
        """
        Execute a validation check and return a structured ValidationResult.
        """

    def _result(
        self,
        *,
        status: str,
        severity: str = "error",
        details: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ValidationResult:
        return ValidationResult(
            check_name=self.check_name,
            status=status,
            severity=severity,
            details=details or {},
            message=message,
        )
