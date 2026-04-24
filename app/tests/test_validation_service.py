"""
Tests for ValidationService (Week 4):

- Tests create temporary raw CSV files, insert matching Dataset and Ingestion rows into the test database, 
  and then call ValidationService.run().
- The service loads the raw file, runs SchemaValidator and ShapeValidator, writes validation_results rows, 
  and updates the ingestion status.

Run 'pytest app/tests/test_validation_service.py' in terminal from repo root after installing requirements
- 'pip install requirements.txt' and 'pip install requirements.api.txt'
"""

import pandas as pd

from app.core.models import Dataset, Ingestion, ValidationResult
from app.services.validation_service import ValidationService


def test_validation_service_passes_valid_file(db, tmp_path):
    """
    Checks that a valid file passes schema and shape validation and the ingestion status becomes "validated".
    """

    raw_file = tmp_path / "valid.csv"

    pd.DataFrame({
        "name": ["A", "B", "C"],
        "amount": [10, 20, 30],
    }).to_csv(raw_file, index=False)

    dataset = Dataset(
        name="test_valid_dataset",
        process_type="FR",
        description="Test dataset",
        schema_def=[
            {"name": "name", "dtype": "str", "required": True},
            {"name": "amount", "dtype": "int", "required": True},
        ],
        validation_cfg={
            "min_rows": 1,
            "max_null_fraction": 0.5,
        },
    )
    db.add(dataset)
    db.commit()
    db.refresh(dataset)

    ingestion = Ingestion(
        dataset_id=dataset.id,
        status="pending",
        original_filename="valid.csv",
        file_ext=".csv",
        raw_path=str(raw_file),
        file_size_bytes=raw_file.stat().st_size,
        file_sha256="test-valid-hash",
    )
    db.add(ingestion)
    db.commit()
    db.refresh(ingestion)

    results = ValidationService(db).run(ingestion.id)

    db.refresh(ingestion)

    assert ingestion.status == "validated"
    assert len(results) == 2

    saved_results = (
        db.query(ValidationResult)
        .filter(ValidationResult.ingestion_id == ingestion.id)
        .all()
    )

    assert len(saved_results) == 2
    assert {r.check_name for r in saved_results} == {"schema", "shape"}


def test_validation_service_fails_bad_shape(db, tmp_path):
    """
    Checks that a shape failure (file has fewer rows than the dataset's min_rows validation setting), 
    causes ShapeValidator to fail and ingestion status becomes "validation_failed".
    """
    raw_file = tmp_path / "bad_shape.csv"

    pd.DataFrame({
        "name": ["A"],
        "amount": [10],
    }).to_csv(raw_file, index=False)

    dataset = Dataset(
        name="test_bad_shape_dataset",
        process_type="FR",
        description="Test dataset",
        schema_def=[
            {"name": "name", "dtype": "str", "required": True},
            {"name": "amount", "dtype": "int", "required": True},
        ],
        validation_cfg={
            "min_rows": 5,
            "max_null_fraction": 0.5,
        },
    )
    db.add(dataset)
    db.commit()
    db.refresh(dataset)

    ingestion = Ingestion(
        dataset_id=dataset.id,
        status="pending",
        original_filename="bad_shape.csv",
        file_ext=".csv",
        raw_path=str(raw_file),
        file_size_bytes=raw_file.stat().st_size,
        file_sha256="test-bad-shape-hash",
    )
    db.add(ingestion)
    db.commit()
    db.refresh(ingestion)

    results = ValidationService(db).run(ingestion.id)

    db.refresh(ingestion)

    assert ingestion.status == "validation_failed"
    assert any(r.check_name == "shape" and r.status == "fail" for r in results)