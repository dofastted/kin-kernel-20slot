#!/usr/bin/env python3
"""Harvest completed turn 3 from stdout, then record turn 4."""
from __future__ import annotations

import json
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, "/workspace/service/scripts")
import record_long_session as rec

OUT = rec.OUT
SESSION = rec.SESSION
TURNS = rec.TURNS
STDOUT = rec.STDOUT_LOG


def load_ndjson(path: Path):
    rows = []
    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows


def long_assistant_texts():
    texts = []
    frames = []
    if not STDOUT.exists():
        return texts, frames
    for line in STDOUT.read_text().splitlines():
        if not line.strip():
            continue
        try:
            frame = rec.redact(json.loads(line))
        except json.JSONDecodeError:
            continue
        frames.append(frame)
        if frame.get("type") != "assistant":
            continue
        for block in (frame.get("message") or {}).get("content") or []:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text" and len(block.get("text") or "") > 200:
                if str(block.get("text")).startswith("20 kin-slot"):
                    continue
                texts.append(block["text"])
    return texts, frames


def main():
    request_log = load_ndjson(OUT / "request.ndjson")
    response_frames = load_ndjson(OUT / "response.ndjson")
    sse_all = load_ndjson(OUT / "sse.ndjson")
    timing = json.loads((OUT / "timing.json").read_text()) if (OUT / "timing.json").exists() else []
    meta = json.loads((OUT / "metadata.json").read_text()) if (OUT / "metadata.json").exists() else {}
    transcript = (OUT / "transcript.md").read_text() if (OUT / "transcript.md").exists() else ""

    texts, frames = long_assistant_texts()
    print("long_texts", [len(t) for t in texts], flush=True)
    # Heuristic: first long research text is overview, last is related (turn 3).
    turn1_text = texts[0] if texts else ""
    turn3_text = texts[-1] if texts else ""
    turn2_text = texts[-2] if len(texts) >= 2 else ""
    if "关联公司" in turn3_text:
        pass
    elif len(texts) >= 3:
        turn3_text = texts[-1]

    if "## Turn 3" not in transcript and turn3_text:
        transcript += (
            f"\n## Turn 3: related\n\n**User**\n\n{TURNS[2]['content']}\n\n"
            f"**Assistant**\n\n{turn3_text}\n"
        )
    if not any(t.get("index") == 3 for t in meta.get("turns") or []):
        meta.setdefault("turns", []).append(
            {
                "index": 3,
                "title": "related",
                "status": 200,
                "elapsed": None,
                "chars": len(turn3_text),
                "harvested_stdout": True,
            }
        )
    rec.persist(request_log, response_frames, sse_all, timing, meta, [transcript])

    history = [
        {"role": "user", "content": TURNS[0]["content"]},
        {"role": "assistant", "content": turn1_text or "(turn1)"},
        {"role": "user", "content": TURNS[1]["content"]},
        {"role": "assistant", "content": turn2_text or "(turn2)"},
        {"role": "user", "content": TURNS[2]["content"]},
        {"role": "assistant", "content": turn3_text or "(turn3)"},
    ]

    t0 = time.perf_counter()
    sampler = rec.MemorySampler(OUT / "memory-turn4.csv", t0)
    sampler.start()
    stdout_pos = rec.stdout_offset()
    turn = TURNS[3]
    payload = rec.base_request(turn, history)
    request_log.append(
        {
            "t": round(time.perf_counter() - t0, 3),
            "turn": 4,
            "title": turn["title"],
            "request": rec.redact(payload),
        }
    )
    rec.persist(request_log, response_frames, sse_all, timing, meta, [transcript])
    print("TURN 4/4 report start", flush=True)
    result = rec.sse_request(payload, SESSION)
    new_frames, _ = rec.read_stdout_since(stdout_pos)
    response_frames.extend(
        {"t": round(time.perf_counter() - t0, 3), "turn": 4, "frame": fr} for fr in new_frames
    )
    sse_all.append({"turn": 4, "title": turn["title"], **{k: result[k] for k in result if k != "text"}})
    parents = []
    tools = []
    for fr in new_frames:
        pid = fr.get("parent_tool_use_id")
        if pid:
            parents.append(pid)
        if fr.get("type") == "assistant":
            for block in (fr.get("message") or {}).get("content") or []:
                if isinstance(block, dict) and block.get("type") == "tool_use":
                    tools.append(
                        {
                            "turn": 4,
                            "name": block.get("name"),
                            "id": block.get("id"),
                            "parent": pid,
                        }
                    )
                    if str(block.get("name") or "").lower() in {"websearch", "web_search"}:
                        meta["web_search"] = meta.get("web_search", 0) + 1
    meta.setdefault("parents", []).extend(sorted(set(parents)))
    meta.setdefault("tool_use", []).extend(tools)
    types = Counter(ev.get("type") for ev in result["events"])
    timing.append(
        {
            "turn": 4,
            "title": "report",
            "elapsed": result["elapsed"],
            "status": result["status"],
            "n_sse": len(result["events"]),
            "n_stdout": len(new_frames),
            "ttft": result["events"][0]["t"] if result["events"] else None,
            "timed_out": result.get("timed_out"),
            "sse_types": dict(types),
            "event_times": [
                {"t": ev["t"], "type": ev.get("type"), "event": ev.get("event")}
                for ev in result["events"]
            ],
        }
    )
    meta.setdefault("turns", []).append(
        {
            "index": 4,
            "title": "report",
            "status": result["status"],
            "elapsed": result["elapsed"],
            "chars": len(result["text"]),
            "n_parents": len(set(parents)),
            "tool_names": [t["name"] for t in tools],
            "timed_out": result.get("timed_out"),
        }
    )
    transcript += (
        f"\n## Turn 4: report\n\n**User**\n\n{turn['content']}\n\n"
        f"**Assistant**\n\n{result['text']}\n"
    )
    print(
        f"TURN 4 done status={result['status']} elapsed={result['elapsed']} "
        f"chars={len(result['text'])} stdout={len(new_frames)} sse={len(result['events'])}",
        flush=True,
    )
    sampler.stop.set()
    sampler.join(timeout=2)
    meta["elapsed_total"] = round(time.perf_counter() - t0, 3)
    rec.persist(request_log, response_frames, sse_all, timing, meta, [transcript])
    # Canonical Claude stdout dump (redacted) for replay.
    raw = "\n".join(json.dumps(fr, ensure_ascii=False) for fr in frames + new_frames) + "\n"
    (OUT / "response.raw.ndjson").write_text(raw)
    print("DONE", json.dumps({"turns": meta.get("turns"), "web_search": meta.get("web_search")}, ensure_ascii=False)[:2000], flush=True)


if __name__ == "__main__":
    main()
