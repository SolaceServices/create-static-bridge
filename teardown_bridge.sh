#!/usr/bin/env bash
# Deletes the bridge, queue, and client usernames created by run_bridge_setup.py
# / run_bridge_setup.sh, via curl.
#
# Reads the same .env as run_bridge_setup.sh, so run the setup steps first
# (or at least `python3 run_bridge_setup.py --generate-env` and fill in the
# secrets) before using this one.
#
# Deletes, on the producer:
#   - the client username (producer-user)
#   - the bridge queue (bridge-queue)
# On the consumer:
#   - the bridge object (bridge-name) - this also removes its remoteMsgVpn and
#     remoteSubscriptions sub-resources, so those don't need separate deletes.
#
# With --direction bi, also deletes the reverse-direction resources
# (producer/consumer roles swapped), matching what --direction bi created.
#
# This is destructive and talks to real brokers. By default it only prints
# what it WOULD delete. Pass --yes to actually perform the deletions.
#
# Usage:
#   ./teardown_bridge.sh                    dry run (no deletes)
#   ./teardown_bridge.sh --yes              actually delete
#   ./teardown_bridge.sh --direction bi --yes
#
# Requires: curl, jq

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$BASE_DIR/.env"

DIRECTION="uni"
EXECUTE="false"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --direction)
            DIRECTION="$2"
            shift 2
            ;;
        --yes)
            EXECUTE="true"
            shift
            ;;
        -h|--help)
            sed -n '2,24p' "$0"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

if [[ "$DIRECTION" != "uni" && "$DIRECTION" != "bi" ]]; then
    echo "--direction must be 'uni' or 'bi'" >&2
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

required_secrets=(PRODUCER_ADMIN_PASSWORD CONSUMER_ADMIN_PASSWORD PRODUCER_PASSWORD CONSUMER_PASSWORD)
missing=()
for key in "${required_secrets[@]}"; do
    [[ -z "${!key:-}" ]] && missing+=("$key")
done
if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Missing secret values in .env: ${missing[*]}" >&2
    exit 1
fi

# label method url user pass
semp_delete() {
    local label="$1" method="$2" url="$3" user="$4" pass="$5"

    echo "--- $label ---"
    echo "$method $url"

    if [[ "$EXECUTE" != "true" ]]; then
        echo "(dry run - not executed)"
        echo
        return
    fi

    local status_code body_response
    status_code="$(curl -sS -o /tmp/teardown_response.$$ -w '%{http_code}' \
        -u "$user:$pass" -X "$method" "$url")"
    body_response="$(cat /tmp/teardown_response.$$)"
    rm -f /tmp/teardown_response.$$

    echo "Status: $status_code"

    if [[ "$status_code" == "404" ]]; then
        echo "Not found - already deleted, skipping."
        echo
        return
    fi
    if [[ "$status_code" == "400" ]] && echo "$body_response" | jq -e '.meta.error.status == "NOT_FOUND"' >/dev/null 2>&1; then
        echo "Not found - already deleted, skipping."
        echo
        return
    fi

    if [[ "$status_code" -lt 200 || "$status_code" -ge 300 ]]; then
        echo "$body_response"
        echo "'$label' failed with status $status_code" >&2
        exit 1
    fi
    echo
}

# Runs the three deletes for one direction. Takes every context value
# explicitly so --direction bi can call it a second time with
# producer/consumer swapped.
run_teardown() {
    local producer_url="$1" producer_vpn="$2" producer_admin="$3" producer_admin_password="$4" \
        producer_user="$5" \
        consumer_url="$6" consumer_vpn="$7" consumer_admin="$8" consumer_admin_password="$9" \
        bridge_name="${10}" bridge_queue="${11}"

    semp_delete "Delete Bridge" DELETE \
        "$consumer_url/config/msgVpns/$consumer_vpn/bridges/$bridge_name,auto" \
        "$consumer_admin" "$consumer_admin_password"

    semp_delete "Delete Queue" DELETE \
        "$producer_url/config/msgVpns/$producer_vpn/queues/$bridge_queue" \
        "$producer_admin" "$producer_admin_password"

    semp_delete "Delete Bridge User" DELETE \
        "$producer_url/config/msgVpns/$producer_vpn/clientUsernames/$producer_user" \
        "$producer_admin" "$producer_admin_password"
}

if [[ "$EXECUTE" != "true" ]]; then
    echo "DRY RUN - nothing will be deleted. Pass --yes to actually delete."
    echo
fi

echo "=== Tearing down bridge: producer -> consumer ==="
echo
run_teardown \
    "$PRODUCER_URL" "$PRODUCER_VPN" "$PRODUCER_ADMIN" "$PRODUCER_ADMIN_PASSWORD" "$PRODUCER_USER" \
    "$CONSUMER_URL" "$CONSUMER_VPN" "$CONSUMER_ADMIN" "$CONSUMER_ADMIN_PASSWORD" \
    "$BRIDGE_NAME" "$BRIDGE_QUEUE"

if [[ "$DIRECTION" == "bi" ]]; then
    echo "=== Tearing down reverse bridge: consumer -> producer ==="
    echo
    run_teardown \
        "$CONSUMER_URL" "$CONSUMER_VPN" "$CONSUMER_ADMIN" "$CONSUMER_ADMIN_PASSWORD" "$CONSUMER_USER" \
        "$PRODUCER_URL" "$PRODUCER_VPN" "$PRODUCER_ADMIN" "$PRODUCER_ADMIN_PASSWORD" \
        "$BRIDGE_NAME" "$BRIDGE_QUEUE"
fi

if [[ "$EXECUTE" == "true" ]]; then
    echo "Teardown complete."
fi
