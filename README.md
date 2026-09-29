# Static Bridge Setup

Automates the SEMP v2 API calls needed to create a uni-directional Solace bridge
between two message VPNs ("producer" and "consumer"), using the request
definitions in [uni-directional/](uni-directional/) and the variables in
[parameters.yml](parameters.yml).

The bridge authenticates to the producer using the existing `producer-user`
client username (created in step 2) — there is no separate `bridge-user`.

## What it does

The bridge connects a **consumer** message VPN to a **producer** message VPN so
that messages published on the producer are relayed to the consumer. Running
the steps in order performs the following against the two Solace brokers:

| Step | File | Action | Runs against |
|------|------|--------|---------------|
| 1a | `1a - Create Bridge Queue.yml` | Create the queue the bridge will read from | producer |
| 1b | `1b - Create Queue Subscriptions.yml` | Add a topic subscription to that queue | producer |
| 2 | `2 - Create Bridge User.yml` | Create the producer-side client username | producer |
| 3 | `3 - Create Bridge Object.yml` | Create the bridge object (disabled) | consumer |
| 4 | `4 - Configure Remote MsgVpn.yml` | Point the bridge at the producer VPN/queue | consumer |
| 5 | `5 - Add Remote Topic Subscriptions.yml` | Add the remote topic subscription | consumer |
| 6 | `6 - Enable Bridge.yml` | Enable the bridge | consumer |
| 7 | `7 - Verify Bridge.yml` | Confirm the bridge is up | consumer |

`BRIDGE_TOPICS` may be a single topic or a comma-separated list (e.g.
`orders/>, shipments/>`). Steps 1b and 5 send one request per topic in the
list, so every topic ends up subscribed on both the queue and the bridge.

## Prerequisites

- Python 3.9+
- `pip install pyyaml requests`
- Two reachable Solace brokers (producer and consumer) with SEMP v2 admin
  access, and their management URLs/admin credentials.

## Setup
1. **Prepare the `parameters.yml` file**:

Make sure the content of the `parameters.yml` file reflects your setup.
use the service-ids, messageVpn, user and passwords for your producer and consumer. 
Make sure to full in the topics that need to be moved over the bridge

2. **Generate the `.env` file** from `parameters.yml`:

   ```bash
   python3 run_bridge_setup.py --generate-env
   ```

   This writes `.env` with all non-secret values pre-filled and leaves the
   secret fields blank.

3. **Fill in the secrets** in `.env`:

   - `PRODUCER_ADMIN_PASSWORD`
   - `CONSUMER_ADMIN_PASSWORD`
   - `CONSUMER_PASSWORD`
   - `PRODUCER_PASSWORD`

   `.env` contains credentials — do not commit it to version control.

   `CONSUMER_USER`/`CONSUMER_PASSWORD` aren't used by any step in a plain
   uni-directional run (no step creates that client username), but they're
   required for `--type=bi-directional` below — the reverse pass's step 2
   creates that username on the consumer side. Fill them in regardless.

4. **Run the setup**:

   ```bash
   python3 run_bridge_setup.py
   ```

   Each step prints its request method/URL, the response status, and the
   response body, then stops immediately if any step fails.

   Pass `--type=bi-directional` to also create the reverse bridge:

   ```bash
   python3 run_bridge_setup.py --type=bi-directional
   ```

   A Solace bridge is inherently one-directional, so a bi-directional link is
   two uni-directional bridges pointed at each other. This runs steps
   1a,1b,2,3,4,5,6,7 once as-is (producer → consumer), then runs them again
   with the producer/consumer variables swapped (consumer → producer) — the
   swapped run's step 2 creates the client username on the consumer side.

## Tearing down

`teardown_bridge.py` is a separate, standalone script that deletes what
`run_bridge_setup.py` created: the bridge object (consumer), the bridge queue
(producer), and the producer-side client username. Deleting the bridge object
also removes its remoteMsgVpn/remoteSubscriptions sub-resources, so those
don't need separate deletes.

It reads the same `parameters.yml` / `.env`, so run `run_bridge_setup.py
--generate-env` first if you haven't already.

By default it only **prints** what it would delete (dry run) — pass `--yes`
to actually perform the deletions:

```bash
python3 teardown_bridge.py                        # dry run
python3 teardown_bridge.py --yes                   # actually delete
python3 teardown_bridge.py --type=bi-directional --yes  # also tear down the reverse bridge
```

A 404 on delete is treated as "already gone" and not an error, so it's safe
to re-run.

## Re-running

`--generate-env` is idempotent: it won't overwrite values you've already set
in `.env`, so it's safe to re-run after adding a new variable to
`parameters.yml`.
