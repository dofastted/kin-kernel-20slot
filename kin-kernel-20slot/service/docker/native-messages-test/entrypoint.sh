#!/bin/sh
# Start the HTTP CONNECT → SOCKS5 bridge, then exec the given command.
# Never print KIN_SOCKS5 / tokens.
set -eu

if [ -z "${KIN_SOCKS5:-}" ]; then
  echo "KIN_SOCKS5 is required (runtime env / --env-file). Not present in the image." >&2
  exit 1
fi

export KIN_HTTP_BRIDGE_ADDR="${KIN_HTTP_BRIDGE_ADDR:-127.0.0.1:18080}"
export KIN_HTTPS_PROXY="${KIN_HTTPS_PROXY:-http://${KIN_HTTP_BRIDGE_ADDR}}"
export HTTPS_PROXY="$KIN_HTTPS_PROXY"
export HTTP_PROXY="$KIN_HTTPS_PROXY"
export https_proxy="$KIN_HTTPS_PROXY"
export http_proxy="$KIN_HTTPS_PROXY"
export NO_PROXY="${NO_PROXY:-127.0.0.1,localhost}"
export no_proxy="$NO_PROXY"

if [ -n "${KIN_SLOT_TZ:-}" ]; then
  export TZ="$KIN_SLOT_TZ"
  export CLAUDE_CODE_TIMEZONE="${CLAUDE_CODE_TIMEZONE:-$KIN_SLOT_TZ}"
fi

python3 /opt/kin/http_to_socks.py &
bridge_pid=$!

i=0
while [ "$i" -lt 25 ]; do
  if curl -sf "http://${KIN_HTTP_BRIDGE_ADDR}/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$bridge_pid" 2>/dev/null; then
    echo "http_to_socks.py exited before becoming healthy" >&2
    wait "$bridge_pid" || true
    exit 1
  fi
  i=$((i + 1))
  sleep 0.2
done

if ! curl -sf "http://${KIN_HTTP_BRIDGE_ADDR}/health" >/dev/null 2>&1; then
  echo "http_to_socks bridge did not become healthy on ${KIN_HTTP_BRIDGE_ADDR}" >&2
  kill "$bridge_pid" 2>/dev/null || true
  exit 1
fi

cleanup() {
  kill "$bridge_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

exec "$@"
