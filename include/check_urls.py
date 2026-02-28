"""Quick script to sanity-check gov_files.json URLs.

We do:
- HEAD to confirm existence
- GET (Range) for first bytes to confirm it's likely the expected file (ZIP/CSV)
"""

from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.request

ssl_ctx = ssl.create_default_context()


def _short(s: str | None, max_len: int = 80) -> str:
    if not s:
        return ""
    return s if len(s) <= max_len else s[: max_len - 1] + "…"


def _sniff(name: str, content_type: str | None, sample: bytes) -> str:
    lowered = sample.lstrip().lower()
    if lowered.startswith(b"<!doctype html") or lowered.startswith(b"<html"):
        return "looks like HTML (not data)"

    if name.lower().endswith(".zip") or (content_type or "").lower().startswith("application/zip"):
        return "zip signature OK" if sample.startswith(b"PK") else "not a ZIP signature"

    if name.lower().endswith(".csv") or "csv" in (content_type or "").lower():
        # CSV is plain text; just assert it isn't binary and isn't HTML.
        try:
            sample.decode("utf-8")
            return "text sample OK"
        except UnicodeDecodeError:
            return "not UTF-8 text"

    return "unclassified"


with open("include/gov_files.json", "r", encoding="utf-8") as f:
    data = json.load(f)

for entry in data["gov_files"]:
    url = entry["file_location"]
    name = entry["file_name"]

    head_status: int | str = "?"
    content_type = None
    content_length = None
    final_url = url

    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=20, context=ssl_ctx) as r:
            head_status = r.status
            final_url = r.geturl()
            content_type = r.headers.get("Content-Type")
            content_length = r.headers.get("Content-Length")
    except urllib.error.HTTPError as e:
        head_status = e.code
    except Exception as e:
        print(f"ERR  - {name}\n  {url}\n  {e}")
        continue

    # Fetch only a tiny range to confirm content isn't an error HTML page.
    sample = b""
    get_status: int | str = "?"
    try:
        get_req = urllib.request.Request(url, method="GET", headers={"Range": "bytes=0-2047"})
        with urllib.request.urlopen(get_req, timeout=20, context=ssl_ctx) as r:
            get_status = r.status
            final_url = r.geturl()
            sample = r.read(2048) or b""
            # Prefer GET headers if present (some servers omit on HEAD).
            content_type = r.headers.get("Content-Type") or content_type
            content_length = r.headers.get("Content-Length") or content_length
    except urllib.error.HTTPError as e:
        get_status = e.code
    except Exception as e:
        print(f"ERR  - {name}\n  {url}\n  GET failed: {e}")
        continue

    sniff = _sniff(name, content_type, sample)
    ct = _short(content_type)
    cl = content_length or ""
    redirected = f" (-> {final_url})" if final_url and final_url != url else ""
    print(f"{head_status}/{get_status} - {name}{redirected} | {ct} {cl} | {sniff}")
