"""Validate LLM API keys against provider HTTP APIs (no extra dependencies)."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)


def validate_gemini_api_key(api_key: str) -> tuple[bool, str]:
    """
    Call Google Generative Language API list-models; 200 means the key is accepted.
    See https://ai.google.dev/api/rest/v1beta/models/list
    """
    key = (api_key or "").strip()
    if not key:
        return False, "API key is empty."

    q = urllib.parse.urlencode({"key": key})
    url = f"https://generativelanguage.googleapis.com/v1beta/models?{q}"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"User-Agent": "JobHunter-AI/1"},
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            if resp.status != 200:
                return False, f"Unexpected HTTP {resp.status} from Google."
            raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw)
            models = data.get("models") if isinstance(data, dict) else None
            n = len(models) if isinstance(models, list) else 0
            return True, f"Valid. Google returned {n} model entries."
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")[:600]
        except Exception:
            err_body = str(e)
        if e.code == 400 and "API_KEY_INVALID" in err_body:
            return False, "Invalid API key (Google: API_KEY_INVALID)."
        if e.code == 403:
            return False, "Key rejected (403). Check API is enabled for Generative Language."
        logger.warning("Gemini validate HTTPError %s: %s", e.code, err_body)
        return False, f"Google API HTTP {e.code}: {err_body}"
    except urllib.error.URLError as e:
        return False, f"Network error talking to Google: {e.reason!s}"
    except Exception as e:
        logger.exception("Gemini validate failed")
        return False, str(e)
