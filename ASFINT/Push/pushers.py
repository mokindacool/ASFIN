import re
from pathlib import Path

_SAFE_CHARS = re.compile(r'[\\/:"*?<>|]+')  # Windows + Unix unsafe

def _safe_filename(name: str) -> str:
    return _SAFE_CHARS.sub("-", str(name)).strip()

def push_csv(df, filename, filepath):
    """Write a CSV to disk; filename can be logical (we’ll sanitize)."""
    # ensure folder exists
    path = Path(filepath)
    path.mkdir(parents=True, exist_ok=True)

    # sanitize filename (no extensions in name expected here)
    safe = _safe_filename(filename)
    out = path / f"{safe}.csv"

    df.to_csv(out, index=False)