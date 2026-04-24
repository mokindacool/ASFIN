# Alex Weekly Code Overview

This document tracks Alex's assigned work based on what is actually present in the repo today. It focuses on Alex's code, explains what each part does, and points out where the current implementation lives.

## Current Repo Reality

- Alex's code exists in two places:
  - `platform/` contains most of the core service, validator, and schema work.
  - `app/` contains the currently wired FastAPI application used by Docker.
- That means this overview separates:
  - Alex's implemented modules in `platform/`
  - Alex's live API-facing preview work in `app/`
- Some `platform/` tests and references exist, but not all `platform/` files they expect are still present.

## Week 1

### Goal

Set up the local runtime foundation: Docker, API container, DB container, and the initial long-term project structure.

### Alex code that exists

- `docker-compose.yml`
- `Dockerfile.api`
- `platform/`

### What is accurate today

#### `docker-compose.yml`

- Starts:
  - `api`
  - `db`
- Passes:
  - `DATABASE_URL`
  - `DATA_ROOT`
- Creates a shared Docker volume for persisted data under `/data`.

#### `Dockerfile.api`

- Builds the API container.
- Installs dependencies.
- Copies the repo into the container.
- Starts Uvicorn with:
  - `app.api.main:app`

#### `platform/` scaffold

- The `platform/` tree exists and contains:
  - `core/`
  - `services/`
  - `validators/`
  - `api/`
  - `tests/`
- It is useful as the main location for Alex's backend modules.
- It is not the currently booted FastAPI app.

### Important correction

- The live app started by Docker is `app.api.main`, not `platform.api.main`.
- So Week 1 is accurate as infrastructure/setup work, but not as a fully live `platform/` app entrypoint.

### Commands that work

```powershell
docker compose up --build
docker compose up --build -d
docker compose down
docker compose ps
curl http://localhost:8000/healthz
curl http://localhost:8000/readyz
```

## Week 2

### Goal

Provide shared configuration, DB wiring, and schema contracts for the dataset/ingestion platform layer.

### Alex code that exists

- `platform/core/config.py`
- `platform/core/database.py`
- `platform/core/schemas.py`

### What each file does

#### `platform/core/config.py`

- Defines settings using `BaseSettings`.
- Reads environment variables and `.env`.
- Normalizes common debug/dev/prod-like values into a boolean `debug`.

#### `platform/core/database.py`

- Creates the SQLAlchemy engine from the configured DB URL.
- Defines the SQLAlchemy `Base`.
- Creates the session factory.
- Exposes `get_db()` for dependency injection.

#### `platform/core/schemas.py`

- Defines shared Pydantic models.
- Includes:
  - health and message responses
  - pagination params
  - dataset create/update/response models
  - column definitions for `schema_def`

### How it fits together

- `config.py` provides runtime settings.
- `database.py` uses those settings to build the DB layer.
- `schemas.py` defines the request/response contracts that services and routes can share.

### Commands that work

```powershell
Get-Content platform\core\config.py
Get-Content platform\core\database.py
Get-Content platform\core\schemas.py
```

## Week 3

### Goal

Handle ingestion creation and raw file persistence.

### Alex code that exists

- `platform/services/storage.py`
- `platform/services/ingestion_service.py`

### What each file does

#### `platform/services/storage.py`

- Centralizes file path construction.
- Defines canonical paths for:
  - raw files
  - clean outputs
  - staging files
- Writes uploaded raw bytes into the raw zone.

Canonical patterns:

- `/data/raw/{dataset_id}/{ingestion_id}/original.{ext}`
- `/data/clean/{dataset_id}/{ingestion_id}/`
- `/data/staging/{ingestion_id}/`

#### `platform/services/ingestion_service.py`

- Validates that the dataset exists and is active.
- Reads upload bytes.
- Computes SHA-256 for duplicate detection.
- Rejects duplicates with `409`.
- Creates an ingestion row with `status="pending"`.
- Writes the file into the raw zone using `StorageService`.

### How it fits together

- `IngestionService` depends on Week 2 DB/session setup.
- It uses `StorageService` for canonical storage paths.
- Its output becomes the input for later validation, ETL, and preview steps.

### Important correction

- `platform/tests/test_ingestion_service.py` exists, but it currently fails because it expects `platform/core/models.py`, which is not present in the repo now.

### Files to inspect

```powershell
Get-Content platform\services\storage.py
Get-Content platform\services\ingestion_service.py
```

## Week 4

### Goal

Create the validator foundation and schema validation logic.

### Alex code that exists

- `platform/validators/base.py`
- `platform/validators/schema_validator.py`

### What each file does

#### `platform/validators/base.py`

- Defines `ValidationResult`.
- Defines the abstract validator interface.
- Provides a helper for consistent result construction.

#### `platform/validators/schema_validator.py`

- Validates `schema_def` against a DataFrame.
- Checks:
  - required columns exist
  - values can be coerced to the declared dtype
- Returns structured:
  - `pass`
  - `fail`
  - `skipped`

### How it fits together

- This is the shared contract for validation logic.
- `SchemaValidator` uses the dataset schema shape introduced in Week 2.
- The same validator pattern is reused later by `JoinValidator`.

### Commands that work

```powershell
Get-Content platform\validators\base.py
Get-Content platform\validators\schema_validator.py
.\.venv\Scripts\python.exe -m pytest platform\tests\test_validators.py -q
```

## Week 5

### Goal

Run the core ETL bridge from raw upload to clean Parquet output.

### Alex code that exists

- `platform/services/elt_service.py`

### What the file does

- Loads the ingestion and dataset records.
- Finds the uploaded raw file.
- Creates a staging directory.
- Renames/copies the staged file into the filename shape ASFINT expects.
- Calls:
  - `ASFINT.Config.Config.get_pFuncs(process_type, "pull")`
  - `ASFINT.Transform.Processor.ASUCProcessor`
- Writes clean output DataFrames to Parquet in the clean zone.
- Updates ingestion status to:
  - `processing`
  - `clean_ready`
  - or `failed`
- Cleans up the staging directory.

### How it fits together

- Depends on Week 3 raw file storage.
- Bridges the new platform layer into the legacy ASFINT pull/process pipeline.
- Produces the clean Parquet outputs used by later preview/publish logic.

### Commands that work

```powershell
Get-Content platform\services\elt_service.py
.\.venv\Scripts\python.exe -m pytest platform\tests\test_etl_service.py -q
```

## Week 6

### Goal

Publish cleaned DataFrames into warehouse-style SQL tables with sanitized names.

### Alex code that exists

- `platform/services/publish_service.py`

### What the file does

- Defines a warehouse publishing interface.
- Implements `PostgreSQLWarehousePublisher`.
- Uses `ASFINT.Utility.BQ_Helpers.clean_name()` to sanitize:
  - table names
  - column names
- Writes the DataFrame to a table like:
  - `{dataset_name}_v{version_number}`
- Returns publish metadata:
  - table name
  - row count
  - schema snapshot

### How it fits together

- Consumes clean DataFrames produced after ETL.
- Uses the DB engine from the shared core layer.
- Represents Alex's warehouse write step, even though the fully wired publish flow in `app/` is not completed.

### Commands that work

```powershell
Get-Content platform\services\publish_service.py
.\.venv\Scripts\python.exe -m pytest platform\tests\test_publish_service.py -q
Get-Content ASFINT\Utility\BQ_Helpers.py
```

## Week 7

### Goal

Add ingestion preview support in the live API tree.

### Alex code that exists

- `app/services/ingestion_service.py`
- `app/api/routers/ingestions.py`
- `app/tests/test_ingestion_service.py`

### Why this week is in `app/`

- The live Docker-started API is under `app/`.
- The preview endpoint is implemented there, not in `platform/`.

### What each file does

#### `app/services/ingestion_service.py`

- Adds preview support on top of ingestion logic.
- Supports:
  - `zone=raw`
  - `zone=clean`
- For raw files:
  - reads CSV into row objects
  - reads TXT into line objects
- For clean files:
  - reads the first Parquet file from the clean output directory
  - returns row objects

#### `app/api/routers/ingestions.py`

- Exposes:
  - `POST /api/v1/datasets/{dataset_id}/ingestions`
  - `GET /api/v1/ingestions/{ingestion_id}`
  - `GET /api/v1/datasets/{dataset_id}/ingestions`
  - `GET /api/v1/ingestions/{ingestion_id}/preview`

#### `app/tests/test_ingestion_service.py`

- Covers:
  - ingestion creation
  - duplicate rejection
  - missing dataset handling
  - status polling
  - pagination
  - preview for raw CSV
  - preview for raw TXT
  - preview for clean Parquet

### How it fits together

- Reuses the ingestion/raw-file lifecycle from the broader pipeline.
- Reads clean outputs produced by the ETL flow.
- Exposes a user-facing inspection step through the live API.

### Commands that work

```powershell
Get-Content app\services\ingestion_service.py
Get-Content app\api\routers\ingestions.py
.\.venv\Scripts\python.exe -m pytest app\tests\test_ingestion_service.py -q
curl "http://localhost:8000/api/v1/ingestions/1/preview?rows=5&zone=raw"
curl "http://localhost:8000/api/v1/ingestions/1/preview?rows=5&zone=clean"
```

## Week 8

### Goal

Add RECONCILE-specific join validation protection.

### Alex code that exists

- `platform/validators/join_validator.py`
- `platform/validators/__init__.py`
- `platform/tests/test_validators.py`

### What each file does

#### `platform/validators/join_validator.py`

- Defines `JoinValidator`.
- Only applies to `RECONCILE`.
- Checks:
  - duplicate `Org Name` join keys
  - suspicious row explosion relative to input row count

#### `platform/validators/__init__.py`

- Re-exports validator classes for cleaner imports.

#### `platform/tests/test_validators.py`

- Tests:
  - schema validator pass/fail/skip behavior
  - join validator skip behavior
  - duplicate key failure
  - row explosion failure
  - stable join pass case

### How it fits together

- Extends the validator framework from Week 4.
- Adds a guardrail specifically for reconciliation-style joins.

### Commands that work

```powershell
Get-Content platform\validators\join_validator.py
Get-Content platform\tests\test_validators.py
.\.venv\Scripts\python.exe -m pytest platform\tests\test_validators.py -q
```

## Best Review Order

If you want to read Alex's implemented work in the clearest order:

1. `platform/core/config.py`
2. `platform/core/database.py`
3. `platform/core/schemas.py`
4. `platform/services/storage.py`
5. `platform/services/ingestion_service.py`
6. `platform/validators/base.py`
7. `platform/validators/schema_validator.py`
8. `platform/services/elt_service.py`
9. `platform/services/publish_service.py`
10. `app/services/ingestion_service.py`
11. `app/api/routers/ingestions.py`
12. `platform/validators/join_validator.py`

## Reliable Command List

These are the Alex-related commands I checked against the repo:

```powershell
.\.venv\Scripts\python.exe -m pytest app\tests\test_ingestion_service.py -q
.\.venv\Scripts\python.exe -m pytest platform\tests\test_validators.py -q
.\.venv\Scripts\python.exe -m pytest platform\tests\test_etl_service.py -q
.\.venv\Scripts\python.exe -m pytest platform\tests\test_publish_service.py -q
docker compose up --build
curl http://localhost:8000/healthz
curl http://localhost:8000/readyz
```

## Known Gaps

- `platform/core/models.py` is not present, even though some `platform` tests still expect it.
- `platform/tests/test_ingestion_service.py` is therefore not currently runnable as-is.
- `platform/tests/test_api_datasets.py` is not present.
- The live `app/` tree has empty files such as:
  - `app/services/publish_service.py`
  - `app/services/validation_service.py`
- So Alex's core pieces exist, but the repo is still split between implemented `platform/` modules and the live `app/` API.

## Short Summary

Alex's implemented code covers the platform backbone:

- runtime and Docker setup
- config and DB wiring
- schema contracts
- raw storage and ingestion creation
- validator foundation and schema validation
- ETL staging and Parquet output
- warehouse publishing utilities
- preview support in the live API
- RECONCILE join validation

The biggest repo-level caveat is that the code is split between `platform/` and `app/`, so this overview now describes Alex's work based on what actually exists rather than what the original week plan intended.
