import os
from pathlib import Path

from fastapi import UploadFile

DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data"))


def _raw_dir(dataset_id: int) -> Path:
    path = DATA_ROOT / "raw" / str(dataset_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


async def save_upload(dataset_id: int, file: UploadFile) -> tuple[Path, int]:
    """Write an uploaded file to /data/raw/{dataset_id}/{filename}.

    Returns (stored_path, file_size_bytes).
    Streams in 64 KB chunks to avoid loading the whole file into memory.
    """
    dest = _raw_dir(dataset_id) / file.filename
    size = 0
    with open(dest, "wb") as f:
        while chunk := await file.read(65536):
            f.write(chunk)
            size += len(chunk)
    return dest, size


def resolve_path(stored_path: str) -> Path:
    return Path(stored_path)
