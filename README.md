# Static Bridge Setup

Automates the SEMP v2 API calls needed to create a uni-directional Solace bridge
between two message VPNs ("producer" and "consumer"), using the request
definitions in [steps/](steps/) and the variables in
[parameters.yml](parameters.yml).

The bridge authenticates to the producer using the existing `producer-user`
client username (created in step 2) — there is no separate `bridge-user`.

## What it does

The bridge connects a **consumer** message VPN to a **producer** message VPN so
that messages published on the producer are relayed to the consumer. Running
the steps in order performs the following against the two Solace brokers:

| Step | File | Action | Runs against |
|------|------|--------|---------------|
| 1a | `1a - Create Bridge Queue.yml` | Create the queue the bridge will read from (only for persistent messaging) | producer |
| 1b | `1b - Create Queue Subscriptions.yml` | Add a topic subscription to that queue (only for persistent messaging) | producer |
| 2 | `2 - Create Bridge User.yml` | Create the producer-side client username | producer |
| 3 | `3 - Create Bridge Object.yml` | Create the bridge object (disabled) | consumer |
| 4 | `4 - Configure Remote MsgVpn.yml` | Point the bridge at the producer VPN/queue | consumer |
| 5 | `5 - Add Remote Topic Subscriptions.yml` | Add the remote topic subscription `(only for direct messaging) | consumer |
| 6 | `6 - Enable Bridge.yml` | Enable the bridge | consumer |
| 7 | `7 - Verify Bridge.yml` | Confirm the bridge is up | consumer |

`BRIDGE_TOPICS` may be a single topic or a comma-separated list (e.g.
`orders/>, shipments/>`). Steps 1b and 5 send one request per topic in the
list, so every topic ends up subscribed on both the queue and the bridge.

## Prerequisites

- Python 3.9+
- Two reachable Solace brokers (producer and consumer) with SEMP v2 admin
  access, and their management URLs/admin credentials.

### Virtual environment

Create and activate a virtual environment, then install everything from
[requirements.txt](requirements.txt) (covers `run_bridge_setup.py`,
`teardown_bridge.py`, and `test_bridge.py`):

```bash
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Re-activate it (`source .venv/bin/activate`) in any new shell before running
the scripts below. `.venv/` should not be committed — see `.gitignore`.

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
   required for `--direction bi` below — the reverse pass's step 2
   creates that username on the consumer side. Fill them in regardless.

4. **Run the setup**:

   ```bash
   python3 run_bridge_setup.py
   ```

   Each step prints its request method/URL, the response status, and the
   response body, then stops immediately if any step fails.

   Pass `--direction bi` to also create the reverse bridge:

   ```bash
   python3 run_bridge_setup.py --direction bi
   ```

   A Solace bridge is inherently one-directional, so a bi-directional link is
   two uni-directional bridges pointed at each other. This runs steps
   1a,1b,2,3,4,5,6,7 once as-is (producer → consumer), then runs them again
   with the producer/consumer variables swapped (consumer → producer) — the
   swapped run's step 2 creates the client username on the consumer side.

   Pass `--delivery` to choose how messages are delivered across the bridge
   (default: `persistent`):

   ```bash
   python3 run_bridge_setup.py --delivery persistent  # default
   python3 run_bridge_setup.py --delivery direct
   ```

   | Delivery | Steps run | Skips |
   |------|-----------|-------|
   | `persistent` (default) | 1a,1b,2,3,4,6,7 | 5 — guaranteed delivery via the bridge queue, no remote topic subscriptions needed |
   | `direct` | 2,3,4,5,6,7 | 1a,1b — no bridge queue; step 5's remote topic subscriptions carry messages directly |

   In `direct` mode, step 4 also omits `queueBinding` from its payload, since
   no bridge queue exists to bind to.

### Alternative: `run_bridge_setup.sh`

`run_bridge_setup.sh` is a curl/jq equivalent of `run_bridge_setup.py`, for
when a plain shell script running raw SEMP v2 calls is more useful than the
Python step-runner (e.g. to read or tweak the requests directly). It reads
the same `.env` file, so steps 1-3 above still apply — generate `.env` with
the Python script first. Requires `curl` and `jq`.

```bash
./run_bridge_setup.sh                       # persistent, uni-directional
./run_bridge_setup.sh --direction bi        # also create the reverse bridge
./run_bridge_setup.sh --delivery direct     # skip the bridge queue steps
```

It supports the same `--direction` and `--delivery` flags, with the same
step-skipping and field-omission behavior described above.

## Testing the bridge

`test_bridge.py` is a separate, standalone script that publishes a message on
the producer broker and waits to receive it on the consumer broker, to
confirm the bridge is actually forwarding traffic. It connects as
`PRODUCER_USER`/`CONSUMER_USER` (the client usernames from `parameters.yml`,
not the SEMP admin credentials) over the messaging ports
(`PRODUCER_BASE_URL`/`CONSUMER_BASE_URL`).

```bash
python3 test_bridge.py                          # persistent delivery, first BRIDGE_TOPICS entry
python3 test_bridge.py --topic orders/test      # publish/subscribe on a specific topic
python3 test_bridge.py --delivery direct
python3 test_bridge.py --timeout 15
```

By default it takes the first entry in `BRIDGE_TOPICS` as the subscription
pattern; if it ends in `/>`, it publishes on a concrete topic underneath it
(e.g. `orders/bridge-test-<id>`) so the wildcard still matches. Use `--topic`
to test an exact topic instead. `--delivery` here should match the
`--delivery` `run_bridge_setup.py` was run with (`persistent`, the default,
or `direct`).

`CONSUMER_USER` is only ever created by the reverse pass of
`run_bridge_setup.py --direction bi` — a plain run never creates it. To avoid
an authentication error, `test_bridge.py` always creates
`PRODUCER_USER`/`CONSUMER_USER` via SEMP (using the admin credentials) on
their respective VPNs first, if missing, before connecting.

Its extra dependencies (`solace-pubsubplus`, `requests`, `certifi`) are
already covered by `requirements.txt` above.

The Solace client library needs a trust store *directory* of individual PEM
files to validate the broker's TLS certificate, not a single bundle file.
On first run, `test_bridge.py` splits `certifi`'s CA bundle into one file per
certificate under `.trust_store_cache/` (gitignored) and reuses it on later
runs.

### Alternative: `test_bridge_sdkperf.sh`

`test_bridge_sdkperf.sh` is a [SDKPerf](https://network.solace.com/discussion/sdkperf)-based
equivalent of `test_bridge.py` — same publish/subscribe round-trip test, but
using Solace's own SDKPerf tool (`sdkperf_java`) instead of the Python
`solace-pubsubplus` client, for environments where SDKPerf is already the
go-to tool for talking to a broker. It reads the same `.env` and follows the
same topic-selection logic.

```bash
./test_bridge_sdkperf.sh                          # persistent delivery, first BRIDGE_TOPICS entry
./test_bridge_sdkperf.sh --topic orders/test      # publish/subscribe on a specific topic
./test_bridge_sdkperf.sh --delivery direct
./test_bridge_sdkperf.sh --timeout 15
```

Requires SDKPerf for Java (`sdkperf_java.sh`) — set `SDKPERF_JAVA` to its
path, or install it under `~/Development/Tools/sdkperf-jcsmp-*/`, which the
script checks by default.

It runs the subscriber in the background (a temporary queue endpoint for
`persistent`, a plain topic subscription for `direct`), publishes one message
with `-pfl` pointing at a generated payload file, then polls the
subscriber's `-md` dump for a completed `Start Message`/`End Message` block
to confirm delivery — a message count check rather than matching the exact
payload text, since the dump's hex/ASCII format wraps the payload across
several lines. Unlike `test_bridge.py`, it does **not** validate the
broker's TLS certificate (SDKPerf's cert-validation flags default to off) —
fine for a connectivity check, not a substitute for the Python script's cert
handling.

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
python3 teardown_bridge.py --direction bi --yes  # also tear down the reverse bridge
```

A missing object on delete — a real 404, or SEMP v2's HTTP 400 with
`meta.error.status: NOT_FOUND` (what it actually returns) — is treated as
"already gone" and not an error, so it's safe
to re-run.

### Alternative: `teardown_bridge.sh`

`teardown_bridge.sh` is a curl/jq equivalent of `teardown_bridge.py`, reading
the same `.env` and deleting the same three resources. It has the same
dry-run-by-default behavior and treats a 404 / `NOT_FOUND` the same way.
Requires `curl` and `jq`.

```bash
./teardown_bridge.sh                     # dry run
./teardown_bridge.sh --yes               # actually delete
./teardown_bridge.sh --direction bi --yes  # also tear down the reverse bridge
```

## Re-running

`--generate-env` is idempotent: it won't overwrite values you've already set
in `.env`, so it's safe to re-run after adding a new variable to
`parameters.yml`.


## Full cycle test persistent messaging uni-directional

```
python3 run_bridge_setup.py

python test_bridge.py

python teardown_bridge.py --yes
```

## Full cycle test direct messaging uni-directional

```
python3 run_bridge_setup.py --delivery direct

python test_bridge.py --delivery direct

python teardown_bridge.py --yes
```
