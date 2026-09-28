#!/usr/bin/env python3
"""Runs the uni-directional Solace bridge setup steps (SEMP v2) in order.

Usage:
    python run_bridge_setup.py --generate-env   # (re)generate .env from parameters.yml
    python run_bridge_setup.py                  # run steps 1a,1b,2a,2b,3,4,5,6,7

Requires: pip install pyyaml requests
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import requests
import yaml

BASE_DIR = Path(__file__).resolve().parent
PARAMETERS_YML = BASE_DIR / "parameters.yml"
STEP_DIR = BASE_DIR / "uni-directional"
ENV_FILE = BASE_DIR / ".env"

STEPS = [
    "1a - Create Bridge Queue.yml",
    "1b - Create Queue Subscriptions.yml",
    "2a - Create Bridge User Producer.yml",
    "2b - Create Bridge User Consumer.yml",
    "3 - Create Bridge Object.yml",
    "4 - Configure Remote MsgVpn.yml",
    "5 - Add Remote Topic Subscriptions.yml",
    "6 - Enable Bridge.yml",
    "7 - Verify Bridge.yml",
]

VAR_PATTERN = re.compile(r"\{\{([\w.-]+)\}\}")


def env_key(var_name: str) -> str:
    return var_name.upper().replace("-", "_")


def load_parameters() -> list[dict]:
    with PARAMETERS_YML.open() as f:
        data = yaml.safe_load(f)
    return data["variables"]


def load_existing_env() -> dict[str, str]:
    values: dict[str, str] = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            value = value.split("  #", 1)[0].strip()
            values[key.strip()] = value
    return values


def generate_env_file() -> None:
    variables = load_parameters()
    existing = load_existing_env()

    lines = [
        "# Generated from parameters.yml",
        "# Fill in the secret values below (they are left blank) before running",
        "# run_bridge_setup.py without --generate-env.",
        "",
    ]
    for var in variables:
        name = var["name"]
        key = env_key(name)
        is_secret = bool(var.get("secret", False))
        if key in existing:
            value = existing[key]
        elif is_secret:
            value = ""
        else:
            value = var.get("value", "")
        marker = "  # secret - fill in" if is_secret and not value else ""
        lines.append(f"{key}={value}{marker}")

    ENV_FILE.write_text("\n".join(lines) + "\n")
    print(f"Wrote {ENV_FILE}")


def build_context(env: dict[str, str], variables: list[dict]) -> dict[str, str]:
    context: dict[str, str] = {}
    missing_secrets = []
    for var in variables:
        name = var["name"]
        key = env_key(name)
        value = env.get(key, "")
        if var.get("secret", False) and not value:
            missing_secrets.append(key)
        context[name] = value
    if missing_secrets:
        sys.exit("Missing secret values in .env: " + ", ".join(missing_secrets))
    return context


def render(template: str, context: dict[str, str]) -> str:
    def repl(match: re.Match) -> str:
        name = match.group(1)
        if name not in context:
            raise KeyError(f"Unresolved variable: {{{{{name}}}}}")
        return context[name]

    return VAR_PATTERN.sub(repl, template)


def run_step(path: Path, context: dict[str, str], session: requests.Session) -> None:
    spec = yaml.safe_load(path.read_text())
    http = spec["http"]
    name = spec["info"]["name"]

    method = http["method"].upper()
    url = render(http["url"], context)

    auth_tuple = None
    auth = http.get("auth")
    if auth and auth.get("type") == "basic":
        auth_tuple = (
            render(auth["username"], context),
            render(auth["password"], context),
        )

    json_body = None
    body = http.get("body")
    if body and body.get("type") == "json":
        json_body = json.loads(render(body["data"], context))

    print(f"--- {name} ---")
    print(f"{method} {url}")

    response = session.request(method, url, auth=auth_tuple, json=json_body, timeout=30)

    print(f"Status: {response.status_code}")
    try:
        print(json.dumps(response.json(), indent=2))
    except ValueError:
        print(response.text)

    if not response.ok:
        raise SystemExit(f"Step '{name}' failed with status {response.status_code}")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--generate-env",
        action="store_true",
        help="(Re)generate .env from parameters.yml and exit without running any steps",
    )
    args = parser.parse_args()

    if args.generate_env or not ENV_FILE.exists():
        generate_env_file()
        if args.generate_env:
            return
        print(f"Fill in the secret values in {ENV_FILE} and re-run this script.")
        return

    variables = load_parameters()
    context = build_context(load_existing_env(), variables)

    session = requests.Session()
    for step_name in STEPS:
        run_step(STEP_DIR / step_name, context, session)

    print("All steps completed successfully.")


if __name__ == "__main__":
    main()
