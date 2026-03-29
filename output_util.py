"""Latest Excel report discovery under output/."""

from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = _ROOT / "output"


def get_latest_xlsx_info() -> dict[str, bool | str]:
    if not OUTPUT_DIR.is_dir():
        return {"available": False}
    files = list(OUTPUT_DIR.glob("*.xlsx"))
    if not files:
        return {"available": False}
    latest = max(files, key=lambda p: p.stat().st_mtime)
    return {"available": True, "filename": latest.name}
