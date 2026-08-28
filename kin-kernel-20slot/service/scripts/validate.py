#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import tomllib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]

REQUIRED = [
    "README.md",
    "kernel/Cargo.toml",
    "kernel/src/main.rs",
    "control/go.mod",
    "control/cmd/kin-control/main.go",
    "contracts/openapi.yaml",
    "contracts/kernel-config.schema.json",
    "configs/routes.example.yaml",
    "deploy/compose.yaml",
    "deploy/k8s/base/kustomization.yaml",
    "docs/ARCHITECTURE.md",
    "docs/CREDENTIALS.md",
    "docs/DELIVERY_STATUS.md",
    "docs/SECURITY.md",
]


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


for relative in REQUIRED:
    if not (ROOT / relative).is_file():
        fail(f"missing required file: {relative}")

for path in sorted(ROOT.rglob("*.json")):
    try:
        json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        fail(f"invalid JSON {path.relative_to(ROOT)}: {exc}")

for path in sorted(ROOT.rglob("*.yaml")):
    try:
        documents = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
    except Exception as exc:
        fail(f"invalid YAML {path.relative_to(ROOT)}: {exc}")
    if not documents or all(document is None for document in documents):
        fail(f"empty YAML {path.relative_to(ROOT)}")

with (ROOT / "kernel/Cargo.toml").open("rb") as handle:
    cargo = tomllib.load(handle)
if cargo.get("package", {}).get("edition") != "2024":
    fail("kernel must use Rust edition 2024")

openapi = yaml.safe_load((ROOT / "contracts/openapi.yaml").read_text(encoding="utf-8"))
if openapi.get("openapi") != "3.1.0" or not openapi.get("paths"):
    fail("OpenAPI must be 3.1.0 and contain paths")


def walk_refs(value: object):
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "$ref" and isinstance(child, str):
                yield child
            yield from walk_refs(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_refs(child)


def resolve_local_ref(document: object, ref: str) -> None:
    if not ref.startswith("#/"):
        return
    current = document
    for raw_part in ref[2:].split("/"):
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, dict) or part not in current:
            fail(f"unresolved OpenAPI ref: {ref}")
        current = current[part]


for reference in walk_refs(openapi):
    resolve_local_ref(openapi, reference)

kustomization = yaml.safe_load(
    (ROOT / "deploy/k8s/base/kustomization.yaml").read_text(encoding="utf-8")
)
for resource in kustomization.get("resources", []):
    if not (ROOT / "deploy/k8s/base" / resource).is_file():
        fail(f"missing kustomize resource: {resource}")

route_config = yaml.safe_load((ROOT / "configs/routes.example.yaml").read_text(encoding="utf-8"))
schema = json.loads((ROOT / "contracts/kernel-config.schema.json").read_text(encoding="utf-8"))
try:
    import jsonschema

    jsonschema.validate(route_config, schema)
except ImportError:
    if route_config.get("apiVersion") != "kin.openai.local/v1alpha1":
        fail("route config apiVersion is invalid")
    if route_config.get("kind") != "RoutePolicySet":
        fail("route config kind is invalid")
    if not isinstance(route_config.get("revision"), int) or route_config["revision"] < 1:
        fail("route config revision must be a positive integer")
    if not isinstance(route_config.get("routes"), list) or not route_config["routes"]:
        fail("route config must contain at least one route")
    required_route_keys = {"name", "match", "target", "isolation", "admission", "timeouts", "retries"}
    for index, route in enumerate(route_config["routes"]):
        missing = required_route_keys - set(route)
        if missing:
            fail(f"route {index} is missing keys: {sorted(missing)}")
        if route["retries"].get("after_first_byte") != 0:
            fail(f"route {index} must not retry after first byte")
except Exception as exc:
    fail(f"route config does not match JSON Schema: {exc}")

shell = subprocess.run(
    ["bash", "-n", str(ROOT / "scripts/smoke.sh")],
    check=False,
    capture_output=True,
    text=True,
)
if shell.returncode != 0:
    fail(f"smoke.sh syntax error: {shell.stderr.strip()}")

print("static validation: OK")
print(f"files checked: {sum(1 for path in ROOT.rglob('*') if path.is_file())}")
