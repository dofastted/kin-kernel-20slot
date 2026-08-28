#!/usr/bin/env bash
set -euo pipefail

kernel_url="${KIN_KERNEL_URL:-http://127.0.0.1:8080}"
control_url="${KIN_CONTROL_URL:-http://127.0.0.1:9090}"
smoke_tmp="$(mktemp -d)"
trap 'rm -rf -- "$smoke_tmp"' EXIT

curl --fail --silent --show-error "$kernel_url/healthz" >/dev/null
curl --fail --silent --show-error "$control_url/healthz" >/dev/null

curl --fail --silent --show-error \
  -X POST "$control_url/api/v1/kernels" \
  -H 'content-type: application/json' \
  --data-binary '{"id":"kernel-demo","address":"http://kernel:8080","capacity":20,"provider":"mock","revision":1}' \
  >/dev/null

curl --fail --silent --show-error \
  -D "$smoke_tmp/headers" \
  -o "$smoke_tmp/first.json" \
  -X POST "$kernel_url/v1/messages" \
  -H 'content-type: application/json' \
  -H 'x-tenant-id: demo' \
  --data-binary '{"model":"mock-agent","max_tokens":256,"messages":[{"role":"user","content":"[use_tool:get_weather]"}],"tools":[{"name":"get_weather","description":"weather","input_schema":{"type":"object"}}]}'

mapfile -t values < <(python3 - "$smoke_tmp/headers" "$smoke_tmp/first.json" <<'PY'
import json
import sys

headers = {}
with open(sys.argv[1], encoding="utf-8") as handle:
    for raw in handle:
        if ":" in raw:
            key, value = raw.split(":", 1)
            headers[key.lower()] = value.strip()
with open(sys.argv[2], encoding="utf-8") as handle:
    body = json.load(handle)
tool = next(item for item in body["content"] if item["type"] == "tool_use")
print(headers["x-kin-session-id"])
print(headers["x-kin-continuation"])
print(tool["id"])
PY
)

session_id="${values[0]}"
continuation="${values[1]}"
tool_use_id="${values[2]}"

python3 - "$tool_use_id" <<'PY' | curl --fail --silent --show-error \
  -o "$smoke_tmp/second.json" \
  -X POST "$kernel_url/v1/messages" \
  -H 'content-type: application/json' \
  -H 'x-tenant-id: demo' \
  -H "x-kin-session-id: $session_id" \
  -H "x-kin-continuation: $continuation" \
  --data-binary @-
import json
import sys

print(json.dumps({
    "model": "mock-agent",
    "max_tokens": 256,
    "messages": [{
        "role": "user",
        "content": [{
            "type": "tool_result",
            "tool_use_id": sys.argv[1],
            "content": {"temperature": 23, "unit": "celsius"}
        }]
    }]
}))
PY

python3 - "$smoke_tmp/second.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    body = json.load(handle)
assert body["stop_reason"] == "end_turn", body
assert "tool result accepted" in body["content"][0]["text"], body
print("smoke: tool continuation completed")
PY

