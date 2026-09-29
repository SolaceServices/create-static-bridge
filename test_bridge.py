#!/usr/bin/env python3
"""Publishes a message on the producer broker and verifies it arrives on the
consumer broker, to confirm the bridge is actually forwarding traffic.

Reads the same parameters.yml / .env as run_bridge_setup.py - run that
script (at least --generate-env, with secrets filled in) first. Uses the
producer-user/consumer-user client usernames (not the admin credentials).

Usage:
    python test_bridge.py                       # persistent delivery, first BRIDGE_TOPICS entry
    python test_bridge.py --topic orders/test     # publish/subscribe on a specific topic
    python test_bridge.py --delivery direct
    python test_bridge.py --timeout 15
    python test_bridge.py --ensure-users          # create producer-user/consumer-user first, if missing

--ensure-users creates PRODUCER_USER/CONSUMER_USER via SEMP v2 (using the
admin credentials) on their respective VPNs if they don't already exist yet.
A plain `run_bridge_setup.py` run only creates PRODUCER_USER; CONSUMER_USER is
otherwise only created by the reverse pass of `--type=bi-directional`.

Requires: pip install pyyaml requests solace-pubsubplus certifi
"""

from __future__ import annotations

import argparse
import re
import sys
import time
import uuid
from pathlib import Path

import certifi
import requests
import yaml
from solace.messaging.builder.direct_message_receiver_builder import DirectMessageReceiverBuilder
from solace.messaging.config.authentication_strategy import BasicUserNamePassword
from solace.messaging.config.solace_properties.service_properties import VPN_NAME
from solace.messaging.config.transport_security_strategy import TLS
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.queue import Queue
from solace.messaging.resources.topic import Topic
from solace.messaging.resources.topic_subscription import TopicSubscription

BASE_DIR = Path(__file__).resolve().parent
PARAMETERS_YML = BASE_DIR / "parameters.yml"
ENV_FILE = BASE_DIR / ".env"
TRUST_STORE_DIR = BASE_DIR / ".trust_store_cache"


def env_key(var_name: str) -> str:
    return var_name.upper().replace("-", "_")


def certifi_trust_store_dir() -> str:
    """The Solace C API's trust store setting expects a directory of individual
    PEM certificate files, not certifi's single bundle file - split it once and
    cache the result."""
    bundle_path = Path(certifi.where())
    marker = TRUST_STORE_DIR / ".source"
    if marker.exists() and marker.read_text() == str(bundle_path):
        return str(TRUST_STORE_DIR)

    TRUST_STORE_DIR.mkdir(exist_ok=True)
    for existing in TRUST_STORE_DIR.glob("*.pem"):
        existing.unlink()

    certs = re.findall(
        r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
        bundle_path.read_text(),
        re.DOTALL,
    )
    for index, cert in enumerate(certs):
        (TRUST_STORE_DIR / f"cert-{index:04d}.pem").write_text(cert + "\n")

    marker.write_text(str(bundle_path))
    return str(TRUST_STORE_DIR)


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


def is_not_found_error(response: requests.Response) -> bool:
    """SEMP v2 reports a missing object as HTTP 400 with meta.error.status == NOT_FOUND."""
    try:
        error = response.json().get("meta", {}).get("error", {})
    except ValueError:
        return False
    return error.get("status") == "NOT_FOUND"


def ensure_client_username(
    semp_url: str, vpn: str, admin_user: str, admin_password: str, username: str, password: str
) -> None:
    """Create a client username via SEMP v2 if it doesn't already exist on the VPN."""
    auth = (admin_user, admin_password)
    response = requests.get(f"{semp_url}/config/msgVpns/{vpn}/clientUsernames/{username}", auth=auth, timeout=30)

    if response.ok:
        print(f"Client username '{username}' already exists on {vpn}.")
        return

    if not is_not_found_error(response):
        print(response.text)
        response.raise_for_status()

    print(f"Creating client username '{username}' on {vpn}...")
    body = {"clientUsername": username, "msgVpnName": vpn, "password": password, "enabled": True}
    response = requests.post(f"{semp_url}/config/msgVpns/{vpn}/clientUsernames", json=body, auth=auth, timeout=30)
    if not response.ok:
        print(response.text)
        response.raise_for_status()
    print(f"Created client username '{username}' on {vpn}.")


def first_topic(bridge_topics: str) -> str:
    topics = [t.strip() for t in bridge_topics.split(",") if t.strip()]
    if not topics:
        sys.exit("BRIDGE_TOPICS is empty - pass --topic explicitly")
    return topics[0]


def concrete_publish_topic(pattern: str) -> str:
    """Turn a subscription pattern like 'orders/>' into a real topic to publish on."""
    if pattern.endswith("/>"):
        return f"{pattern[:-2]}/bridge-test-{uuid.uuid4().hex[:8]}"
    if "*" in pattern or pattern == ">":
        sys.exit(
            f"Topic pattern '{pattern}' contains a wildcard - can't publish on it "
            "directly. Pass a concrete topic with --topic instead."
        )
    return pattern


def connect(base_url: str, vpn: str, username: str, password: str) -> MessagingService:
    properties = {
        "solace.messaging.transport.host": f"tcps://{base_url}",
        VPN_NAME: vpn,
    }
    service = (
        MessagingService.builder()
        .from_properties(properties)
        .with_authentication_strategy(BasicUserNamePassword.of(username, password))
        .with_transport_security_strategy(
            TLS.create().with_certificate_validation(
                ignore_expiration=False, trust_store_file_path=certifi_trust_store_dir()
            )
        )
        .build()
    )
    service.connect()
    return service


def build_receiver(consumer_service: MessagingService, subscribe_pattern: str, delivery: str):
    subscription = TopicSubscription.of(subscribe_pattern)
    if delivery == "direct":
        receiver = (
            consumer_service.create_direct_message_receiver_builder()
            .with_subscriptions([subscription])
            .build()
        )
    else:
        queue = Queue.non_durable_exclusive_queue()
        receiver = (
            consumer_service.create_persistent_message_receiver_builder()
            .with_message_auto_acknowledgement()
            .with_subscriptions([subscription])
            .build(queue)
        )
    receiver.start()
    return receiver


def publish(producer_service: MessagingService, publish_topic: str, payload: str, delivery: str) -> None:
    message = producer_service.message_builder().build(payload)
    if delivery == "direct":
        publisher = producer_service.create_direct_message_publisher_builder().build()
    else:
        publisher = producer_service.create_persistent_message_publisher_builder().build()
    publisher.start()
    publisher.publish(message, Topic.of(publish_topic))
    publisher.terminate()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--topic",
        help="Topic to publish/subscribe on (default: first entry in BRIDGE_TOPICS, "
        "with a trailing '/>' turned into a concrete test topic)",
    )
    parser.add_argument(
        "--delivery",
        choices=["persistent", "direct"],
        default="persistent",
        help="Message delivery mode to test (default: persistent)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Seconds to wait for the message to arrive on the consumer side (default: 10)",
    )
    parser.add_argument(
        "--message",
        default=None,
        help="Payload to send (default: a generated string with a random id)",
    )
    parser.add_argument(
        "--ensure-users",
        action="store_true",
        help="Create PRODUCER_USER/CONSUMER_USER via SEMP on their respective VPNs "
        "if they don't already exist, before connecting",
    )
    args = parser.parse_args()

    variables = load_parameters()
    context = build_context(load_env(), variables)

    if args.ensure_users:
        ensure_client_username(
            context["producer-url"],
            context["producer-vpn"],
            context["producer-admin"],
            context["producer-admin-password"],
            context["producer-user"],
            context["producer-password"],
        )
        ensure_client_username(
            context["consumer-url"],
            context["consumer-vpn"],
            context["consumer-admin"],
            context["consumer-admin-password"],
            context["consumer-user"],
            context["consumer-password"],
        )
        print()

    if args.topic:
        subscribe_pattern = publish_topic = args.topic
    else:
        subscribe_pattern = first_topic(context["bridge-topics"])
        publish_topic = concrete_publish_topic(subscribe_pattern)

    payload = args.message or f"bridge-test {uuid.uuid4()}"

    print(f"Delivery mode:              {args.delivery}")
    print(f"Subscribing on consumer to: {subscribe_pattern}")
    print(f"Publishing on producer to:  {publish_topic}")
    print(f"Payload:                    {payload!r}\n")

    consumer_service = connect(
        context["consumer-base-url"],
        context["consumer-vpn"],
        context["consumer-user"],
        context["consumer-password"],
    )
    producer_service = connect(
        context["producer-base-url"],
        context["producer-vpn"],
        context["producer-user"],
        context["producer-password"],
    )

    receiver = None
    try:
        receiver = build_receiver(consumer_service, subscribe_pattern, args.delivery)

        # Give the subscription a moment to propagate before publishing.
        time.sleep(2)

        publish(producer_service, publish_topic, payload, args.delivery)
        print("Published. Waiting for delivery on the consumer side...")

        inbound = receiver.receive_message(timeout=int(args.timeout * 1000))

        if inbound is None:
            print(
                f"\nFAILED: no message received within {args.timeout}s. The bridge "
                "may be down, misconfigured, or the topic doesn't match what it "
                "was configured to relay."
            )
            sys.exit(1)

        received_payload = inbound.get_payload_as_string()
        print(f"\nSUCCESS: received on consumer -> {received_payload!r}")
        if received_payload != payload:
            print("WARNING: received payload differs from what was sent.")
    finally:
        if receiver is not None:
            receiver.terminate()
        consumer_service.disconnect()
        producer_service.disconnect()


if __name__ == "__main__":
    main()
