from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request


def fetch_json(url: str) -> tuple[bool, str]:
    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            code = int(getattr(resp, "status", 0) or 0)
            data = resp.read().decode("utf-8", errors="replace")
            if code < 200 or code >= 300:
                return False, f"HTTP {code}: {data[:240]}"
            return True, data
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = str(e)
        return False, f"HTTPError {e.code}: {body[:240]}"
    except Exception as e:
        return False, f"Error: {e}"


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scripts/render_post_deploy_check.py <base_url>")
        print("Example: python scripts/render_post_deploy_check.py https://ai-game-studio-mvp.onrender.com")
        return 2

    base = sys.argv[1].strip().rstrip("/")
    checks = [
        ("health", f"{base}/healthz"),
        ("ready", f"{base}/readyz"),
        ("state", f"{base}/api/state"),
        ("completion", f"{base}/api/completion"),
        ("learning", f"{base}/api/learning/status"),
    ]

    failed = 0
    for name, url in checks:
        ok, payload = fetch_json(url)
        if not ok:
            failed += 1
            print(f"[FAIL] {name}: {url}")
            print(f"       {payload}")
            continue
        preview = payload[:180].replace("\n", " ")
        print(f"[ OK ] {name}: {url}")
        print(f"       {preview}")

    if failed:
        print(f"\nResult: FAILED ({failed}/{len(checks)} checks failed)")
        return 1
    print(f"\nResult: PASS ({len(checks)}/{len(checks)} checks passed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
