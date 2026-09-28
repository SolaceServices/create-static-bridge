# Broker Bridge Setup

Automates the SEMP v2 API calls needed to create a uni-directional Solace bridge
between two message VPNs ("producer" and "consumer"), using the request
definitions in [uni-directional/](uni-directional/) and the variables in
[parameters.yml](parameters.yml).

The bridge authenticates to the producer using the existing `producer-user`
client username (created in step 2a) — there is no separate `bridge-user`.

## What it does

The bridge connects a **consumer** message VPN to a **producer** message VPN so
that messages published on the producer are relayed to the consumer. Running
the steps in order performs the following against the two Solace brokers:

| Step | File | Action | Runs against |
|------|------|--------|---------------|
| 1a | `1a - Create Bridge Queue.yml` | Create the queue the bridge will read from | producer |
| 1b | `1b - Create Queue Subscriptions.yml` | Add a topic subscription to that queue | producer |
| 2a | `2a - Create Bridge User Producer.yml` | Create the producer-side client username | producer |
| 2b | `2b - Create Bridge User Consumer.yml` | Create the consumer-side client username | consumer |
| 3 | `3 - Create Bridge Object.yml` | Create the bridge object (disabled) | consumer |
| 4 | `4 - Configure Remote MsgVpn.yml` | Point the bridge at the producer VPN/queue | consumer |
| 5 | `5 - Add Remote Topic Subscriptions.yml` | Add the remote topic subscription | consumer |
| 6 | `6 - Enable Bridge.yml` | Enable the bridge | consumer |
| 7 | `7 - Verify Bridge.yml` | Confirm the bridge is up | consumer |

## Prerequisites

- Python 3.9+
- `pip install pyyaml requests`
- Two reachable Solace brokers (producer and consumer) with SEMP v2 admin
  access, and their management URLs/admin credentials.

## Setup

1. **Generate the `.env` file** from `parameters.yml`:

   ```bash
   python3 run_bridge_setup.py --generate-env
   ```

   This writes `.env` with all non-secret values pre-filled and leaves the
   secret fields blank.

2. **Fill in the secrets** in `.env`:

   - `PRODUCER_ADMIN_PASSWORD`
   - `CONSUMER_ADMIN_PASSWORD`
   - `CONSUMER_PASSWORD`
   - `PRODUCER_PASSWORD`

   `.env` contains credentials — do not commit it to version control.

3. **Run the setup**:

   ```bash
   python3 run_bridge_setup.py
   ```

   Each step prints its request method/URL, the response status, and the
   response body, then stops immediately if any step fails.

## Re-running

`--generate-env` is idempotent: it won't overwrite values you've already set
in `.env`, so it's safe to re-run after adding a new variable to
`parameters.yml`.
