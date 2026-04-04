"""List Vertex AI Model Garden Gemini models for GOOGLE_CLOUD_PROJECT (uses ADC / .env).

Run from repo root:
  python scripts/list_vertex_models.py
  python scripts/list_vertex_models.py --filter flash
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))

    from dotenv import load_dotenv

    load_dotenv(root / ".env")

    parser = argparse.ArgumentParser(description="List Vertex publisher models (Model Garden).")
    parser.add_argument(
        "--filter",
        default=os.getenv("JOBHUNTER_VERTEX_MODEL_LIST_FILTER", "gemini"),
        help='Substring filter (default: "gemini"). Empty string = no name filter.',
    )
    parser.add_argument(
        "--location",
        default=(os.getenv("GOOGLE_CLOUD_LOCATION") or "us-central1").strip(),
        help="GCP region (default: GOOGLE_CLOUD_LOCATION or us-central1).",
    )
    args = parser.parse_args()

    project = (os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or "").strip()
    if not project:
        print("Set GOOGLE_CLOUD_PROJECT (or GCP_PROJECT) in .env", file=sys.stderr)
        return 1

    import vertexai
    from vertexai.model_garden import list_models

    vertexai.init(project=project, location=args.location)
    filt = (args.filter or "").strip() or None
    try:
        raw = list_models(model_filter=filt) if filt else list_models()
    except Exception as e:
        print(f"list_models failed: {e}", file=sys.stderr)
        return 1

    print(f"project={project}  location={args.location}  filter={filt!r}  count={len(raw)}\n")
    print("Model Garden id → use in JOBHUNTER_VERTEX_MODEL (short id for LangChain):\n")
    for line in sorted(raw):
        short = line.split("@", 1)[0]
        if "/" in short:
            short = short.split("/", 1)[-1]
        print(f"  {line}")
        print(f"    → JOBHUNTER_VERTEX_MODEL={short}")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
