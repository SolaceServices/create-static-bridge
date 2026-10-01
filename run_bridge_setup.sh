#!/usr/bin/env bash
# Runs the uni-directional Solace bridge setup steps (SEMP v2) via curl.
#
# A shell/curl equivalent of run_bridge_setup.py, for environments where a
# quick look at (or tweak of) the raw SEMP calls is more useful than the
# Python step-runner. Reads the same .env file - run
# `python3 run_bridge_setup.py --generate-env` first if it doesn't exist yet,
# and fill in the secret values.
#
# Usage:
#   ./run_bridge_setup.sh                     persistent, uni-directional
#   ./run_bridge_setup.sh --direction bi       also create the reverse bridge
#   ./run_bridge_setup.sh --delivery direct   skip the bridge queue steps
#
# --delivery selects which steps run (default: persistent):
#   persistent  1a,1b,2,3,4,6,7  - guaranteed delivery via the bridge queue,
#               skips step 5 (remote topic subscriptions on the bridge)
#   direct      2,3,4,5,6,7      - skips 1a,1b (no bridge queue), uses step 5's
#               remote topic subscriptions for direct (non-guaranteed) delivery
#
# Requires: curl, jq

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$BASE_DIR/.env"

DIRECTION="uni"
DELIVERY="persistent"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --direction)
            DIRECTION="$2"
            shift 2
            ;;
        --delivery)
            DELIVERY="$2"
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

if [[ "$DIRECTION" != "uni" && "$DIRECTION" != "bi" ]]; then
    echo "--direction must be 'uni' or 'bi'" >&2
    exit 1
fi
if [[ "$DELIVERY" != "persistent" && "$DELIVERY" != "direct" ]]; then
    echo "--delivery must be 'persistent' or 'direct'" >&2
    exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
    echo "$ENV_FILE not found - run 'python3 run_bridge_setup.py --generate-env' first" >&2
    exit 1
fi

# Load .env, stripping inline "  # ..." comments, same as run_bridge_setup.py.
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

# name method url user pass [json_body]
semp_request() {
    local name="$1" method="$2" url="$3" user="$4" pass="$5" body="${6:-}"

    echo "--- $name ---"
    echo "$method $url"
    [[ -n "$body" ]] && echo "$body" | jq .

    local response status_code body_response
    response="$(curl -sS -o /tmp/semp_response.$$ -w '%{http_code}' \
        -u "$user:$pass" \
        -X "$method" "$url" \
        ${body:+-H "Content-Type: application/json" -d "$body"})"
    status_code="$response"
    body_response="$(cat /tmp/semp_response.$$)"
    rm -f /tmp/semp_response.$$

    echo "Status: $status_code"
    echo "$body_response" | jq . 2>/dev/null || echo "$body_response"

    if [[ "$status_code" -lt 200 || "$status_code" -ge 300 ]]; then
        echo "Step '$name' failed with status $status_code" >&2
        exit 1
    fi
    echo
}

# Runs all steps for one direction. Takes every context value explicitly so
# --direction bi can call it a second time with producer/consumer swapped.
create_bridge() {
    local producer_url="$1" producer_vpn="$2" producer_admin="$3" producer_admin_password="$4" \
        producer_base_url="$5" producer_user="$6" producer_password="$7" \
        consumer_url="$8" consumer_vpn="$9" consumer_admin="${10}" consumer_admin_password="${11}" \
        bridge_name="${12}" bridge_queue="${13}" bridge_topics="${14}"

    IFS=',' read -ra topics <<< "$bridge_topics"

    if [[ "$DELIVERY" == "persistent" ]]; then
        semp_request "1a - Create Bridge Queue" POST \
            "$producer_url/config/msgVpns/$producer_vpn/queues" \
            "$producer_admin" "$producer_admin_password" \
            "$(jq -n --arg q "$bridge_queue" --arg vpn "$producer_vpn" \
                '{queueName: $q, msgVpnName: $vpn, accessType: "exclusive", egressEnabled: true, ingressEnabled: true, permission: "consume"}')"

        for topic in "${topics[@]}"; do
            semp_request "1b - Create Queue Subscriptions [$topic]" POST \
                "$producer_url/config/msgVpns/$producer_vpn/queues/$bridge_queue/subscriptions" \
                "$producer_admin" "$producer_admin_password" \
                "$(jq -n --arg q "$bridge_queue" --arg vpn "$producer_vpn" --arg t "$topic" \
                    '{queueName: $q, msgVpnName: $vpn, subscriptionTopic: $t}')"
        done
    fi

    semp_request "2 - Create Bridge User" POST \
        "$producer_url/config/msgVpns/$producer_vpn/clientUsernames" \
        "$producer_admin" "$producer_admin_password" \
        "$(jq -n --arg u "$producer_user" --arg vpn "$producer_vpn" --arg p "$producer_password" \
            '{clientUsername: $u, msgVpnName: $vpn, password: $p, enabled: true}')"

    semp_request "3 - Create Bridge Object" POST \
        "$consumer_url/config/msgVpns/$consumer_vpn/bridges" \
        "$consumer_admin" "$consumer_admin_password" \
        "$(jq -n --arg b "$bridge_name" --arg vpn "$consumer_vpn" --arg u "$producer_user" --arg p "$producer_password" \
            '{bridgeName: $b, bridgeVirtualRouter: "auto", msgVpnName: $vpn, enabled: false, remoteAuthenticationScheme: "basic", remoteAuthenticationBasicClientUsername: $u, remoteAuthenticationBasicPassword: $p}')"

    local remote_msgvpn_body
    if [[ "$DELIVERY" == "persistent" ]]; then
        remote_msgvpn_body="$(jq -n --arg b "$bridge_name" --arg vpn "$consumer_vpn" --arg rvpn "$producer_vpn" \
            --arg loc "$producer_base_url" --arg q "$bridge_queue" \
            '{bridgeName: $b, bridgeVirtualRouter: "auto", msgVpnName: $vpn, remoteMsgVpnName: $rvpn, remoteMsgVpnLocation: $loc, remoteMsgVpnInterface: "", enabled: true, tlsEnabled: true, compressedDataEnabled: false, queueBinding: $q}')"
    else
        remote_msgvpn_body="$(jq -n --arg b "$bridge_name" --arg vpn "$consumer_vpn" --arg rvpn "$producer_vpn" \
            --arg loc "$producer_base_url" \
            '{bridgeName: $b, bridgeVirtualRouter: "auto", msgVpnName: $vpn, remoteMsgVpnName: $rvpn, remoteMsgVpnLocation: $loc, remoteMsgVpnInterface: "", enabled: true, tlsEnabled: true, compressedDataEnabled: false}')"
    fi
    semp_request "4 - Configure Remote MsgVpn" POST \
        "$consumer_url/config/msgVpns/$consumer_vpn/bridges/$bridge_name,auto/remoteMsgVpns" \
        "$consumer_admin" "$consumer_admin_password" "$remote_msgvpn_body"

    if [[ "$DELIVERY" == "direct" ]]; then
        for topic in "${topics[@]}"; do
            semp_request "5 - Add Remote Topic Subscriptions [$topic]" POST \
                "$consumer_url/config/msgVpns/$consumer_vpn/bridges/$bridge_name,auto/remoteSubscriptions" \
                "$consumer_admin" "$consumer_admin_password" \
                "$(jq -n --arg b "$bridge_name" --arg vpn "$consumer_vpn" --arg t "$topic" \
                    '{bridgeName: $b, bridgeVirtualRouter: "auto", msgVpnName: $vpn, remoteSubscriptionTopic: $t, deliverAlwaysEnabled: true}')"
        done
    fi

    semp_request "6 - Enable Bridge" PATCH \
        "$consumer_url/config/msgVpns/$consumer_vpn/bridges/$bridge_name,auto" \
        "$consumer_admin" "$consumer_admin_password" \
        '{"enabled": true}'

    semp_request "7 - Verify Bridge" GET \
        "$consumer_url/monitor/msgVpns/$consumer_vpn/bridges/$bridge_name,auto" \
        "$consumer_admin" "$consumer_admin_password"
}

echo "=== Creating bridge: producer -> consumer ==="
echo
create_bridge \
    "$PRODUCER_URL" "$PRODUCER_VPN" "$PRODUCER_ADMIN" "$PRODUCER_ADMIN_PASSWORD" \
    "$PRODUCER_BASE_URL" "$PRODUCER_USER" "$PRODUCER_PASSWORD" \
    "$CONSUMER_URL" "$CONSUMER_VPN" "$CONSUMER_ADMIN" "$CONSUMER_ADMIN_PASSWORD" \
    "$BRIDGE_NAME" "$BRIDGE_QUEUE" "$BRIDGE_TOPICS"

if [[ "$DIRECTION" == "bi" ]]; then
    echo "=== Creating reverse bridge: consumer -> producer ==="
    echo
    create_bridge \
        "$CONSUMER_URL" "$CONSUMER_VPN" "$CONSUMER_ADMIN" "$CONSUMER_ADMIN_PASSWORD" \
        "$CONSUMER_BASE_URL" "$CONSUMER_USER" "$CONSUMER_PASSWORD" \
        "$PRODUCER_URL" "$PRODUCER_VPN" "$PRODUCER_ADMIN" "$PRODUCER_ADMIN_PASSWORD" \
        "$BRIDGE_NAME" "$BRIDGE_QUEUE" "$BRIDGE_TOPICS"
fi

echo "All steps completed successfully."
