#!/usr/bin/env python3
"""20 concurrent short real requests against the live kernel."""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

KERNEL = os.environ.get("KIN_KERNEL_URL", "http://127.0.0.1:18081")
OUT = Path(os.environ.get("KIN_TRACE_DIR", "/workspace/artifacts/spcx-long-trace"))
N = int(os.environ.get("KIN_CONC", "20"))


def one(i: int):
    session = f"hello-c20-{i:02d}"
    body = json.dumps(
        {
            "model": "claude-sonnet-5",
            "max_tokens": 32,
            "stream": True,
            "messages": [{"role": "user", "content": f"Reply with exactly: hello-{i:02d}"}],
        }
    ).encode()
    req = urllib.request.Request(
        f"{KERNEL}/v1/messages",
        data=body,
        method="POST",
        headers={
            "content-type": "application/json",
            "accept": "text/event-stream",
            "x-kin-session-id": session,
            "anthropic-version": "2023-06-01",
        },
    )
    t0 = time.perf_counter()
    status = None
    types = []
    text = []
    err = None
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            status = resp.status
            event = "message"
            buf = []
            while True:
                raw = resp.readline()
                if not raw:
                    break
                s = raw.decode("utf-8", "replace").rstrip("\r\n")
                if s.startswith("event:"):
                    event = s[6:].strip() or "message"
                    continue
                if s.startswith("data:"):
                    buf.append(s[5:].lstrip())
                    continue
                if s == "":
                    data = "\n".join(buf)
                    buf = []
                    if data and data != "[DONE]":
                        try:
                            obj = json.loads(data)
                        except json.JSONDecodeError:
                            obj = {}
                        types.append(obj.get("type") or event)
                        delta = obj.get("delta") or {}
                        if delta.get("type") == "text_delta":
                            text.append(delta.get("text") or "")
                    event = "message"
    except Exception as exc:
        err = str(exc)[:240]
        if isinstance(exc, urllib.error.HTTPError):
            status = exc.code
    return {
        "i": i,
        "session": session,
        "status": status,
        "elapsed": round(time.perf_counter() - t0, 3),
        "text": "".join(text),
        "n_events": len(types),
        "has_message_start": "message_start" in types,
        "has_text": bool(text),
        "error": err,
    }


def health():
    try:
        with urllib.request.urlopen(f"{KERNEL}/healthz", timeout=3) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        return {"error": str(exc)}


def main():
    before = health()
    t0 = time.perf_counter()
    rows = []
    with ThreadPoolExecutor(max_workers=N) as pool:
        futs = [pool.submit(one, i) for i in range(N)]
        for fut in as_completed(futs):
            rows.append(fut.result())
            print(
                f"done {rows[-1]['i']:02d} status={rows[-1]['status']} "
                f"elapsed={rows[-1]['elapsed']} text={rows[-1]['text'][:40]!r}",
                flush=True,
            )
    rows.sort(key=lambda r: r["i"])
    after = health()
    report = {
        "n": N,
        "elapsed": round(time.perf_counter() - t0, 3),
        "ok": sum(1 for r in rows if r["status"] == 200 and r["has_text"]),
        "streamed": sum(1 for r in rows if r["has_message_start"]),
        "unique_texts": len({r["text"] for r in rows if r["text"]}),
        "before": before.get("memory"),
        "after": after.get("memory"),
        "rows": rows,
    }
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "conc20-hello.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print("SUMMARY", json.dumps({k: report[k] for k in ("n", "elapsed", "ok", "streamed", "unique_texts")}))


if __name__ == "__main__":
    main()
