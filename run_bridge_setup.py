#!/usr/bin/env python3
"""Runs the uni-directional Solace bridge setup steps (SEMP v2) in order.

Usage:
    python run_bridge_setup.py --generate-env         # (re)generate .env from parameters.yml
    python run_bridge_setup.py                        # uni-directional: producer -> consumer
    python run_bridge_setup.py --type=bi-directional  # also create the reverse bridge

--type=bi-directional runs the same 1a,1b,2,3,4,5,6,7 sequence twice: once
as-is, then again with the producer/consumer variables swapped, so a second
bridge is created carrying traffic the other way.

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
    "2 - Create Bridge User.yml",
    "3 - Create Bridge Object.yml",
    "4 - Configure Remote MsgVpn.yml",
    "5 - Add Remote Topic Subscriptions.yml",
    "6 - Enable Bridge.yml",
    "7 - Verify Bridge.yml",
]

VAR_PATTERN = re.compile(r"\{\{([\w.-]+)\}\}")

# Steps whose JSON body carries a topic field that may be a comma-separated
# list (bridge-topics). Each topic in the list gets its own request.
MULTI_TOPIC_FIELDS = {
    "1b - Create Queue Subscriptions.yml": "subscriptionTopic",
    "5 - Add Remote Topic Subscriptions.yml": "remoteSubscriptionTopic",
}

# Variable pairs to swap when creating the reverse bridge for --type=bi-directional.
SWAP_PAIRS = [
    ("producer-url", "consumer-url"),
    ("producer-admin", "consumer-admin"),
    ("producer-vpn", "consumer-vpn"),
    ("producer-admin-password", "consumer-admin-password"),
    ("producer-base-url", "consumer-base-url"),
    ("producer-user", "consumer-user"),
    ("producer-password", "consumer-password"),
]


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


def swap_producer_consumer(context: dict[str, str]) -> dict[str, str]:
    """Swap producer/consumer variable values to build the reverse-direction context."""
    swapped = dict(context)
    for producer_key, consumer_key in SWAP_PAIRS:
        swapped[producer_key], swapped[consumer_key] = (
            context[consumer_key],
            context[producer_key],
        )
    return swapped


def render(template: str, context: dict[str, str]) -> str:
    def repl(match: re.Match) -> str:
        name = match.group(1)
        if name not in context:
            raise KeyError(f"Unresolved variable: {{{{{name}}}}}")
        return context[name]

    return VAR_PATTERN.sub(repl, template)


def send_request(
    name: str,
    method: str,
    url: str,
    auth_tuple: tuple[str, str] | None,
    json_body: dict | None,
    session: requests.Session,
) -> None:
    print(f"--- {name} ---")
    print(f"{method} {url}")
    if json_body is not None:
        print(json.dumps(json_body, indent=2))

    response = session.request(method, url, auth=auth_tuple, json=json_body, timeout=30)

    print(f"Status: {response.status_code}")
    try:
        print(json.dumps(response.json(), indent=2))
    except ValueError:
        print(response.text)

    if not response.ok:
        raise SystemExit(f"Step '{name}' failed with status {response.status_code}")

    print()


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

    topic_field = MULTI_TOPIC_FIELDS.get(path.name)
    if topic_field and json_body and topic_field in json_body:
        topics = [t.strip() for t in json_body[topic_field].split(",") if t.strip()]
        for topic in topics:
            topic_body = {**json_body, topic_field: topic}
            send_request(f"{name} [{topic}]", method, url, auth_tuple, topic_body, session)
    else:
        send_request(name, method, url, auth_tuple, json_body, session)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--generate-env",
        action="store_true",
        help="(Re)generate .env from parameters.yml and exit without running any steps",
    )
    parser.add_argument(
        "--type",
        choices=["uni-directional", "bi-directional"],
        default="uni-directional",
        help="uni-directional (default) creates one bridge; bi-directional also "
        "creates the reverse bridge by swapping producer/consumer",
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

    print("=== Creating bridge: producer -> consumer ===\n")
    for step_name in STEPS:
        run_step(STEP_DIR / step_name, context, session)

    if args.type == "bi-directional":
        print("=== Creating reverse bridge: consumer -> producer ===\n")
        reverse_context = swap_producer_consumer(context)
        for step_name in STEPS:
            run_step(STEP_DIR / step_name, reverse_context, session)

    print("All steps completed successfully.")


if __name__ == "__main__":
    main()
