#!/usr/bin/env python3
"""Record one multi-turn Kin session: HTTP + Claude stdout + memory. Redacts secrets."""
from __future__ import annotations

import csv
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

KERNEL = os.environ.get("KIN_KERNEL_URL", "http://127.0.0.1:18081")
STDOUT_LOG = Path(os.environ.get("KIN_STDOUT_LOG", "/tmp/kin-live/claude.multiplex.stdout.log"))
OUT = Path(os.environ.get("KIN_TRACE_DIR", "/workspace/artifacts/spcx-long-trace"))
SESSION = os.environ.get("KIN_SESSION_ID", "spcx-long-01")
KERNEL_PID = int(os.environ.get("KIN_KERNEL_PID", "0") or "0")
CLAUDE_PID = int(os.environ.get("KIN_CLAUDE_PID", "0") or "0")
TURN_DEADLINE = int(os.environ.get("KIN_TURN_DEADLINE", "240"))
SOCKET_TIMEOUT = int(os.environ.get("KIN_SOCKET_TIMEOUT", "30"))

SECRET_RE = re.compile(
    r"(sk-ant-[A-Za-z0-9_-]+|Bearer\s+\S+|oat01-[A-Za-z0-9_-]+|ort01-[A-Za-z0-9_-]+|"
    r"sid01-[A-Za-z0-9_-]+)",
    re.I,
)
HEADER_DROP = {"authorization", "cookie", "set-cookie", "x-api-key", "proxy-authorization"}

TURNS = [
    {
        "title": "overview",
        "content": (
            "搜索并介绍 SPCX 这只美股 ETF：全称、发行人、投资目标、费用率、AUM、"
            "前十大持仓、近一年及今年以来表现。只用公开信息，注明来源日期。"
        ),
        "search": True,
        "max_tokens": 1536,
    },
    {
        "title": "outlook",
        "content": (
            "基于刚才的检索，分析 SPCX 前景与风险：SPAC 市场 2025-2026 周期、"
            "利率与 IPO/De-SPAC 窗口、对持仓公司和投资者的影响。列出未来 12 个月"
            "关键催化剂与主要下行风险。保持中性研究口径，不要给出买卖指令。"
        ),
        "search": False,
        "max_tokens": 2048,
    },
    {
        "title": "related",
        "content": (
            "继续搜索 SPCX 的关联公司与可比产品：最大持仓对应的 SPAC/运营公司、"
            "发行商（Tuttle Capital / Exchange Traded Concepts）、可比 SPAC ETF"
            "（例如 SPAK 或其他），以及主要 SPAC 发起人。说明每家与 SPCX 的关联逻辑。"
        ),
        "search": True,
        "max_tokens": 2048,
    },
    {
        "title": "report",
        "content": (
            "把以上三轮整理成一份机构风格研报，结构固定为：\n"
            "1. 标的与结构  2. 持仓与表现  3. 行业与宏观  4. 关联公司图谱\n"
            "5. 12 个月情景（乐观/基准/悲观）  6. 风险  7. 结论（中性观察，非投资建议）。\n"
            "文末列出引用过的来源。"
        ),
        "search": False,
        "max_tokens": 3072,
    },
]


def redact(obj):
    if isinstance(obj, str):
        return SECRET_RE.sub("[REDACTED]", obj)
    if isinstance(obj, dict):
        return {
            k: redact(v)
            for k, v in obj.items()
            if str(k).lower() not in HEADER_DROP
        }
    if isinstance(obj, list):
        return [redact(v) for v in obj]
    return obj


def discover_pids():
    global KERNEL_PID, CLAUDE_PID
    if KERNEL_PID and CLAUDE_PID:
        return
    try:
        hz = health()
        mem = hz.get("memory") or {}
        # pids not in health; scan /proc
    except Exception:
        mem = {}
    for d in Path("/proc").iterdir():
        if not d.name.isdigit():
            continue
        try:
            cmd = (d / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", "replace")
        except Exception:
            continue
        if KERNEL_PID == 0 and "kin-kernel" in cmd:
            KERNEL_PID = int(d.name)
        if CLAUDE_PID == 0 and "/claude" in cmd and "--output-format" in cmd:
            CLAUDE_PID = int(d.name)


def rss_pss(pid: int):
    rss = pss = None
    if not pid:
        return rss, pss
    try:
        parts = Path(f"/proc/{pid}/statm").read_text().split()
        rss = int(parts[1]) * 4096
    except Exception:
        pass
    try:
        for line in Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines():
            if line.startswith("Pss:"):
                pss = int(line.split()[1]) * 1024
                break
    except Exception:
        pass
    return rss, pss


def cgroup_current():
    try:
        return int(Path("/sys/fs/cgroup/memory.current").read_text().strip())
    except Exception:
        return None


def health():
    try:
        with urllib.request.urlopen(f"{KERNEL}/healthz", timeout=2) as resp:
            return json.loads(resp.read())
    except Exception:
        return {}


class MemorySampler(threading.Thread):
    def __init__(self, path: Path, t0: float):
        super().__init__(daemon=True)
        self.path = path
        self.t0 = t0
        self.stop = threading.Event()
        self.rows = []

    def run(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(
                [
                    "t_rel",
                    "cgroup_bytes",
                    "kernel_rss",
                    "kernel_pss",
                    "claude_rss",
                    "claude_pss",
                    "health_observed",
                    "health_claude_rss",
                    "health_kernel_rss",
                    "health_service_rss",
                    "health_pending",
                    "health_admission",
                ]
            )
            while not self.stop.is_set():
                discover_pids()
                hz = health().get("memory") or {}
                kr, kp = rss_pss(KERNEL_PID)
                cr, cp = rss_pss(CLAUDE_PID)
                row = [
                    f"{time.perf_counter() - self.t0:.3f}",
                    cgroup_current() or "",
                    kr or "",
                    kp or "",
                    cr or "",
                    cp or "",
                    hz.get("observed_bytes", ""),
                    hz.get("claude_rss_bytes", ""),
                    hz.get("kernel_rss_bytes", ""),
                    hz.get("service_rss_bytes", ""),
                    hz.get("pending", ""),
                    hz.get("admission", ""),
                ]
                w.writerow(row)
                fh.flush()
                self.rows.append(row)
                self.stop.wait(0.5)


def stdout_offset() -> int:
    try:
        return STDOUT_LOG.stat().st_size
    except FileNotFoundError:
        return 0


def read_stdout_since(offset: int) -> tuple[list[dict], int]:
    if not STDOUT_LOG.exists():
        return [], offset
    data = STDOUT_LOG.read_bytes()
    chunk = data[offset:]
    frames = []
    for line in chunk.splitlines():
        if not line.strip():
            continue
        try:
            frames.append(redact(json.loads(line)))
        except json.JSONDecodeError:
            continue
    return frames, len(data)


def sse_request(payload: dict, session: str, timeout: int = TURN_DEADLINE):
    body = json.dumps(payload).encode()
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
    events = []
    text = []
    event_name = "message"
    buf = []
    status = None
    headers = {}
    timed_out = False
    try:
        with urllib.request.urlopen(req, timeout=SOCKET_TIMEOUT) as resp:
            status = resp.status
            headers = {k.lower(): v for k, v in resp.headers.items()}
            while True:
                now = time.perf_counter() - t0
                if now > timeout:
                    timed_out = True
                    events.append({"t": round(now, 3), "event": "timeout", "type": "timeout"})
                    break
                try:
                    raw = resp.readline()
                except TimeoutError:
                    continue
                except Exception as exc:
                    events.append(
                        {
                            "t": round(time.perf_counter() - t0, 3),
                            "event": "error",
                            "type": "error",
                            "data": {"error": str(exc)[:240]},
                        }
                    )
                    break
                if not raw:
                    break
                now = time.perf_counter() - t0
                line = raw.decode("utf-8", "replace")
                s = line.rstrip("\r\n")
                if s.startswith("event:"):
                    event_name = s[6:].strip() or "message"
                    continue
                if s.startswith("data:"):
                    buf.append(s[5:].lstrip())
                    continue
                if s == "":
                    data = "\n".join(buf)
                    buf = []
                    rec = {"t": round(now, 3), "event": event_name}
                    if data and data != "[DONE]":
                        try:
                            obj = json.loads(data)
                        except json.JSONDecodeError:
                            obj = {"_raw": data[:240]}
                        rec["type"] = obj.get("type") or event_name
                        rec["data"] = redact(obj)
                        if rec["type"] == "content_block_delta":
                            delta = obj.get("delta") or {}
                            if delta.get("type") == "text_delta":
                                text.append(delta.get("text") or "")
                    events.append(rec)
                    event_name = "message"
                    if rec.get("type") == "message_stop":
                        break
    except urllib.error.HTTPError as exc:
        status = exc.code
        try:
            body_txt = exc.read()[:1000].decode("utf-8", "replace")
        except Exception:
            body_txt = str(exc)
        events.append(
            {
                "t": round(time.perf_counter() - t0, 3),
                "event": "http_error",
                "type": "error",
                "data": redact({"status": status, "body": body_txt[:400]}),
            }
        )
    except Exception as exc:
        events.append(
            {
                "t": round(time.perf_counter() - t0, 3),
                "event": "error",
                "type": "error",
                "data": {"error": str(exc)[:240]},
            }
        )
    return {
        "status": status,
        "elapsed": round(time.perf_counter() - t0, 3),
        "headers": redact(headers),
        "events": events,
        "text": "".join(text),
        "timed_out": timed_out,
    }


def base_request(turn, history):
    tools = []
    if turn["search"]:
        tools = [{"type": "web_search_20250305", "name": "web_search"}]
    return {
        "model": "claude-sonnet-5",
        "max_tokens": turn["max_tokens"],
        "stream": True,
        "messages": history + [{"role": "user", "content": turn["content"]}],
        "tools": tools,
        "thinking": {"type": "adaptive", "display": "omitted"},
        "system": [{"type": "text", "text": "# Environment\n - Timezone: America/New_York"}],
    }


def persist(request_log, response_frames, sse_all, timing, meta, transcript):
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "request.ndjson").write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in request_log) + "\n"
    )
    (OUT / "response.ndjson").write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in response_frames) + "\n"
    )
    (OUT / "sse.ndjson").write_text(
        "\n".join(json.dumps(redact(row), ensure_ascii=False) for row in sse_all) + "\n"
    )
    (OUT / "timing.json").write_text(json.dumps(timing, ensure_ascii=False, indent=2))
    (OUT / "metadata.json").write_text(json.dumps(redact(meta), ensure_ascii=False, indent=2))
    (OUT / "transcript.md").write_text("".join(transcript))


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    discover_pids()
    t0 = time.perf_counter()
    sampler = MemorySampler(OUT / "memory.csv", t0)
    sampler.start()
    history = []
    request_log = []
    response_frames = []
    sse_all = []
    timing = []
    meta = {
        "session_id": SESSION,
        "model": "claude-sonnet-5",
        "kernel_pid": KERNEL_PID,
        "claude_pid": CLAUDE_PID,
        "turns": [],
        "parents": [],
        "tool_use": [],
        "web_search": 0,
        "usage": [],
    }
    stdout_pos = stdout_offset()
    transcript = [f"# SPCX 长对话录制\n\nsession: `{SESSION}`\n"]

    try:
        for index, turn in enumerate(TURNS, 1):
            payload = base_request(turn, history)
            request_log.append(
                {
                    "t": round(time.perf_counter() - t0, 3),
                    "turn": index,
                    "title": turn["title"],
                    "request": redact(payload),
                }
            )
            persist(request_log, response_frames, sse_all, timing, meta, transcript)
            print(f"TURN {index}/{len(TURNS)} {turn['title']} start", flush=True)
            result = sse_request(payload, SESSION)
            frames, stdout_pos = read_stdout_since(stdout_pos)
            response_frames.extend(
                {"t": round(time.perf_counter() - t0, 3), "turn": index, "frame": fr}
                for fr in frames
            )
            sse_all.append({"turn": index, "title": turn["title"], **{k: result[k] for k in result if k != "text"}})
            parents = []
            tools = []
            for fr in frames:
                f = fr if isinstance(fr, dict) else {}
                pid = f.get("parent_tool_use_id")
                if pid:
                    parents.append(pid)
                msg = (f.get("message") or {}).get("content") or []
                if f.get("type") == "assistant":
                    for block in msg:
                        if isinstance(block, dict) and block.get("type") == "tool_use":
                            tools.append(
                                {
                                    "turn": index,
                                    "name": block.get("name"),
                                    "id": block.get("id"),
                                    "parent": pid,
                                }
                            )
                            if str(block.get("name") or "").lower() in {"websearch", "web_search"}:
                                meta["web_search"] += 1
            meta["parents"].extend(sorted(set(parents)))
            meta["tool_use"].extend(tools)
            usage = None
            for ev in result["events"]:
                if ev.get("type") == "message_delta":
                    usage = (ev.get("data") or {}).get("usage")
            if usage:
                meta["usage"].append({"turn": index, "usage": usage})
            types = Counter(ev.get("type") for ev in result["events"])
            timing.append(
                {
                    "turn": index,
                    "title": turn["title"],
                    "elapsed": result["elapsed"],
                    "status": result["status"],
                    "n_sse": len(result["events"]),
                    "n_stdout": len(frames),
                    "ttft": result["events"][0]["t"] if result["events"] else None,
                    "timed_out": result.get("timed_out"),
                    "sse_types": dict(types),
                    "event_times": [
                        {"t": ev["t"], "type": ev.get("type"), "event": ev.get("event")}
                        for ev in result["events"]
                    ],
                }
            )
            meta["turns"].append(
                {
                    "index": index,
                    "title": turn["title"],
                    "status": result["status"],
                    "elapsed": result["elapsed"],
                    "chars": len(result["text"]),
                    "n_parents": len(set(parents)),
                    "tool_names": [t["name"] for t in tools],
                    "timed_out": result.get("timed_out"),
                }
            )
            history.append({"role": "user", "content": turn["content"]})
            history.append({"role": "assistant", "content": result["text"] or "(empty)"})
            transcript.append(
                f"\n## Turn {index}: {turn['title']}\n\n**User**\n\n{turn['content']}\n\n**Assistant**\n\n{result['text']}\n"
            )
            print(
                f"TURN {index} done status={result['status']} elapsed={result['elapsed']} "
                f"chars={len(result['text'])} stdout={len(frames)} sse={len(result['events'])} "
                f"timeout={result.get('timed_out')}",
                flush=True,
            )
            persist(request_log, response_frames, sse_all, timing, meta, transcript)
            if result.get("timed_out") and not result["text"]:
                print("TURN aborted: empty timeout", flush=True)
                break
    finally:
        sampler.stop.set()
        sampler.join(timeout=2)
        meta["elapsed_total"] = round(time.perf_counter() - t0, 3)
        meta["memory_samples"] = len(sampler.rows)
        meta["kernel_pid"] = KERNEL_PID
        meta["claude_pid"] = CLAUDE_PID
        persist(request_log, response_frames, sse_all, timing, meta, transcript)
        print(
            "DONE",
            json.dumps(
                {k: meta[k] for k in ("elapsed_total", "turns", "web_search") if k in meta},
                ensure_ascii=False,
            )[:1500],
            flush=True,
        )


if __name__ == "__main__":
    main()
