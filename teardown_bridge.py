#!/usr/bin/env python3
"""Deletes the bridge, queue, and client usernames created by run_bridge_setup.py.

Reads the same parameters.yml / .env as run_bridge_setup.py, so run that
script's setup steps first (or at least generate + fill in .env) before using
this one.

Deletes, on the producer:
  - the client username (producer-user)
  - the bridge queue (bridge-queue)
On the consumer:
  - the bridge object (bridge-name) — this also removes its remoteMsgVpn and
    remoteSubscriptions sub-resources, so those don't need separate deletes.

With --direction=bi, also deletes the reverse-direction resources
(producer/consumer roles swapped), matching what
`run_bridge_setup.py --direction=bi` created.

This is destructive and talks to real brokers. By default it only prints what
it WOULD delete. Pass --yes to actually perform the deletions.

Usage:
    python teardown_bridge.py                        # dry run (no deletes)
    python teardown_bridge.py --yes                  # actually delete
    python teardown_bridge.py --direction=bi --yes

Requires: pip install pyyaml requests
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import requests
import yaml

BASE_DIR = Path(__file__).resolve().parent
PARAMETERS_YML = BASE_DIR / "parameters.yml"
ENV_FILE = BASE_DIR / ".env"

VAR_PATTERN = re.compile(r"\{\{([\w.-]+)\}\}")

SWAP_PAIRS = [
    ("producer-url", "consumer-url"),
    ("producer-admin", "consumer-admin"),
    ("producer-vpn", "consumer-vpn"),
    ("producer-admin-password", "consumer-admin-password"),
    ("producer-base-url", "consumer-base-url"),
    ("producer-user", "consumer-user"),
    ("producer-password", "consumer-password"),
]

# (name, method, url template, username template, password template)
DELETE_REQUESTS = [
    (
        "Delete Bridge",
        "DELETE",
        "{{consumer-url}}/config/msgVpns/{{consumer-vpn}}/bridges/{{bridge-name}},auto",
        "{{consumer-admin}}",
        "{{consumer-admin-password}}",
    ),
    (
        "Delete Queue",
        "DELETE",
        "{{producer-url}}/config/msgVpns/{{producer-vpn}}/queues/{{bridge-queue}}",
        "{{producer-admin}}",
        "{{producer-admin-password}}",
    ),
    (
        "Delete Bridge User",
        "DELETE",
        "{{producer-url}}/config/msgVpns/{{producer-vpn}}/clientUsernames/{{producer-user}}",
        "{{producer-admin}}",
        "{{producer-admin-password}}",
    ),
]


def env_key(var_name: str) -> str:
    return var_name.upper().replace("-", "_")


def load_parameters() -> list[dict]:
    with PARAMETERS_YML.open() as f:
        data = yaml.safe_load(f)
    return data["variables"]


def load_env() -> dict[str, str]:
    if not ENV_FILE.exists():
        sys.exit(f"{ENV_FILE} not found - run run_bridge_setup.py --generate-env first")
    values: dict[str, str] = {}
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.split("  #", 1)[0].strip()
        values[key.strip()] = value
    return values


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


def delete(
    label: str,
    method: str,
    url_template: str,
    user_template: str,
    password_template: str,
    context: dict[str, str],
    session: requests.Session,
    execute: bool,
) -> None:
    url = render(url_template, context)
    auth = (render(user_template, context), render(password_template, context))

    print(f"--- {label} ---")
    print(f"{method} {url}")

    if not execute:
        print("(dry run - not executed)\n")
        return

    response = session.request(method, url, auth=auth, timeout=30)
    print(f"Status: {response.status_code}")

    if response.status_code == 404 or is_not_found_error(response):
        print("Not found - already deleted, skipping.\n")
        return

    if not response.ok:
        print(response.text)
        raise SystemExit(f"'{label}' failed with status {response.status_code}")

    print()


def is_not_found_error(response: requests.Response) -> bool:
    """SEMP v2 reports a missing object as HTTP 400 with meta.error.status == NOT_FOUND."""
    try:
        error = response.json().get("meta", {}).get("error", {})
    except ValueError:
        return False
    return error.get("status") == "NOT_FOUND"


def run_teardown(context: dict[str, str], session: requests.Session, execute: bool) -> None:
    for label, method, url_template, user_template, password_template in DELETE_REQUESTS:
        delete(label, method, url_template, user_template, password_template, context, session, execute)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--direction",
        choices=["uni", "bi"],
        default="uni",
        help="Match the --direction used with run_bridge_setup.py, so the "
        "reverse-direction resources are torn down too",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Actually perform the deletions. Without this flag, only prints what "
        "would be deleted (dry run).",
    )
    args = parser.parse_args()

    variables = load_parameters()
    context = build_context(load_env(), variables)

    session = requests.Session()

    if not args.yes:
        print("DRY RUN - nothing will be deleted. Pass --yes to actually delete.\n")

    print("=== Tearing down bridge: producer -> consumer ===\n")
    run_teardown(context, session, execute=args.yes)

    if args.direction == "bi":
        print("=== Tearing down reverse bridge: consumer -> producer ===\n")
        run_teardown(swap_producer_consumer(context), session, execute=args.yes)

    if args.yes:
        print("Teardown complete.")


if __name__ == "__main__":
    main()
