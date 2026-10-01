#!/usr/bin/env bash
# Publishes a message on the producer broker and verifies it arrives on the
# consumer broker, using Solace's SDKPerf (sdkperf_java) instead of the
# Python solace-pubsubplus client - a curl/jq-style equivalent of
# test_bridge.py, but for actual message traffic rather than SEMP config.
#
# Reads the same .env as the other scripts, so run run_bridge_setup.py
# --generate-env (and fill in secrets) first.
#
# Usage:
#   ./test_bridge_sdkperf.sh                          persistent delivery, first BRIDGE_TOPICS entry
#   ./test_bridge_sdkperf.sh --topic orders/test      publish/subscribe on a specific topic
#   ./test_bridge_sdkperf.sh --delivery direct
#   ./test_bridge_sdkperf.sh --timeout 15
#
# Requires: SDKPerf for Java (sdkperf_java.sh) - set SDKPERF_JAVA to its
# path, or install it under ~/Development/Tools/sdkperf-jcsmp-*/.
# Download: https://network.solace.com/discussion/sdkperf
#
# Note: unlike test_bridge.py, this does not validate the broker's TLS
# certificate (SDKPerf's -sslvc/-sslvcd/-sslvch default to off) - fine for a
# connectivity test, not a substitute for the Python script's cert checks.

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$BASE_DIR/.env"

TOPIC=""
DELIVERY="persistent"
TIMEOUT=10
MESSAGE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --topic)
            TOPIC="$2"
            shift 2
            ;;
        --delivery)
            DELIVERY="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --message)
            MESSAGE="$2"
            shift 2
            ;;
        -h|--help)
            sed -n '2,20p' "$0"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

if [[ "$DELIVERY" != "persistent" && "$DELIVERY" != "direct" ]]; then
    echo "--delivery must be 'persistent' or 'direct'" >&2
    exit 1
fi

# Locate sdkperf_java.sh.
if [[ -n "${SDKPERF_JAVA:-}" ]]; then
    SDKPERF="$SDKPERF_JAVA"
else
    SDKPERF="$(ls "$HOME"/Development/Tools/sdkperf-jcsmp-*/sdkperf_java.sh 2>/dev/null | head -1 || true)"
fi
if [[ -z "$SDKPERF" || ! -x "$SDKPERF" ]]; then
    echo "sdkperf_java.sh not found - set SDKPERF_JAVA to its path" >&2
    exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
    echo "$ENV_FILE not found - run 'python3 run_bridge_setup.py --generate-env' first" >&2
    exit 1
fi

while IFS='=' read -r key value; do
    [[ -z "$key" || "$key" == \#* ]] && continue
    value="${value%%  #*}"
    export "$key"="$value"
done < <(grep -v '^\s*#' "$ENV_FILE" | grep '=')

required_secrets=(CONSUMER_PASSWORD PRODUCER_PASSWORD)
missing=()
for key in "${required_secrets[@]}"; do
    [[ -z "${!key:-}" ]] && missing+=("$key")
done
if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Missing secret values in .env: ${missing[*]}" >&2
    exit 1
fi

# Work out subscribe_pattern/publish_topic the same way test_bridge.py does:
# default to the first BRIDGE_TOPICS entry, turning a trailing '/>' into a
# concrete topic underneath it so the wildcard still matches.
if [[ -n "$TOPIC" ]]; then
    SUBSCRIBE_PATTERN="$TOPIC"
    PUBLISH_TOPIC="$TOPIC"
else
    SUBSCRIBE_PATTERN="$(echo "$BRIDGE_TOPICS" | cut -d',' -f1 | xargs)"
    if [[ -z "$SUBSCRIBE_PATTERN" ]]; then
        echo "BRIDGE_TOPICS is empty - pass --topic explicitly" >&2
        exit 1
    fi
    if [[ "$SUBSCRIBE_PATTERN" == *"/>" ]]; then
        suffix="$(openssl rand -hex 4 2>/dev/null || echo "$RANDOM$RANDOM")"
        PUBLISH_TOPIC="${SUBSCRIBE_PATTERN%/>}/bridge-test-${suffix}"
    elif [[ "$SUBSCRIBE_PATTERN" == *"*"* || "$SUBSCRIBE_PATTERN" == ">" ]]; then
        echo "Topic pattern '$SUBSCRIBE_PATTERN' contains a wildcard - can't publish on it directly. Pass a concrete topic with --topic instead." >&2
        exit 1
    else
        PUBLISH_TOPIC="$SUBSCRIBE_PATTERN"
    fi
fi

PAYLOAD="${MESSAGE:-bridge-test $(uuidgen 2>/dev/null || echo "$RANDOM-$RANDOM")}"

echo "Delivery mode:              $DELIVERY"
echo "Subscribing on consumer to: $SUBSCRIBE_PATTERN"
echo "Publishing on producer to:  $PUBLISH_TOPIC"
echo "Payload:                    '$PAYLOAD'"
echo

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

SUB_LOG="$WORKDIR/subscriber.log"
PAYLOAD_FILE="$WORKDIR/payload.txt"
printf '%s' "$PAYLOAD" > "$PAYLOAD_FILE"

sub_args=(
    -cip "tcps://$CONSUMER_BASE_URL"
    -cu "$CONSUMER_USER@$CONSUMER_VPN"
    -cp "$CONSUMER_PASSWORD"
    -stl "$SUBSCRIBE_PATTERN"
    -mt "$DELIVERY"
    -md
)
if [[ "$DELIVERY" == "persistent" ]]; then
    # Temporary queue endpoint, equivalent to test_bridge.py's
    # Queue.non_durable_exclusive_queue().
    sub_args+=(-tqe 1)
fi

# Run the subscriber in the background, capped at timeout + startup headroom.
timeout "$((TIMEOUT + 15))" "$SDKPERF" "${sub_args[@]}" > "$SUB_LOG" 2>&1 &
SUB_PID=$!

# Give the subscription a moment to propagate before publishing, same as
# test_bridge.py.
sleep 5

echo "Publishing..."
pub_args=(
    -cip "tcps://$PRODUCER_BASE_URL"
    -cu "$PRODUCER_USER@$PRODUCER_VPN"
    -cp "$PRODUCER_PASSWORD"
    -ptl "$PUBLISH_TOPIC"
    -mt "$DELIVERY"
    -mn 1
    -pfl "$PAYLOAD_FILE"
)
if ! "$SDKPERF" "${pub_args[@]}" > "$WORKDIR/publisher.log" 2>&1; then
    echo "FAILED: publish did not complete successfully:"
    cat "$WORKDIR/publisher.log" >&2
    kill "$SUB_PID" 2>/dev/null || true
    exit 1
fi
echo "Published. Waiting for delivery on the consumer side..."

# Poll the subscriber's -md dump for a completed message block rather than
# waiting the full timeout - "End Message" marks one received message
# regardless of its content, which a plain payload grep can't reliably do
# (the hex dump wraps the payload across several lines).
received=false
for ((i = 0; i < TIMEOUT; i++)); do
    if grep -q "End Message" "$SUB_LOG" 2>/dev/null; then
        received=true
        break
    fi
    sleep 1
done

kill "$SUB_PID" 2>/dev/null || true
wait "$SUB_PID" 2>/dev/null || true

echo
if [[ "$received" == "true" ]]; then
    echo "SUCCESS: message received on consumer:"
    echo
    sed -n '/Start Message/,/End Message/p' "$SUB_LOG"
else
    echo "FAILED: no message received within ${TIMEOUT}s. The bridge may be down,"
    echo "misconfigured, or the topic doesn't match what it was configured to relay."
    echo
    echo "--- subscriber output ---"
    cat "$SUB_LOG"
    exit 1
fi
