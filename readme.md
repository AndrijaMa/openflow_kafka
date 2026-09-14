# Openflow Connector Parameter Tools

Two small Python utilities for reading and writing **Openflow (Apache NiFi) connector parameters** on Snowflake-deployment runtimes, using [`nipyapi`](https://github.com/Chaffelson/nipyapi).

| Script | Purpose |
|--------|---------|
| `get_connector_parameters.py` | Read/export the parameters of any connector on a runtime to JSON. |
| `put_connector_parameters.py` | Write/update parameter values on a chosen connector (dry-run by default). |

Both talk to the NiFi REST API of an Openflow runtime through a stored `nipyapi` profile, and share the same runtime-selection logic.

---

## Prerequisites

- Python 3, with `nipyapi` installed (`pip install nipyapi`). Optional: `pyyaml` for YAML parameter files.
- A configured `nipyapi` profile per runtime in `~/.nipyapi/profiles.yml` (holds the NiFi URL + bearer token).
- An Openflow infrastructure cache at `~/.snowflake/cortex/memory/openflow_infrastructure_*.json`, which maps deployments/runtimes to profiles. This is what lets you select a runtime by name instead of a raw profile.
- The target runtime must be running (a suspended/restarting runtime returns HTTP 503).

List what's available: 

```bash
python3 get_connector_parameters.py --list-runtimes
```

---

## Runtime selection (shared by both scripts)

You pick which runtime to talk to using any of these; precedence is `--profile` > `--deployment`/`--runtime` > default (`demo`):

| Flag | Meaning |
|------|---------|
| `--runtime NAME` | Runtime name (e.g. `demo`), resolved to a profile via the cache. |
| `--deployment NAME` | Deployment name; narrows the match and is used for output filenames. |
| `--profile NAME` | Use a `nipyapi` profile directly (overrides the above). |
| `--list-runtimes` | Print the deployments/runtimes/profiles in the cache and exit. |

---

## `get_connector_parameters.py` — read/export

Connects to a runtime, finds connector process group(s), prints their parameters, and writes one JSON file per connector.

### Options

| Flag | Description |
|------|-------------|
| `--connector SUBSTR` | Case-insensitive substring on the connector name. Omit to export **all** connectors. (`--name-filter` is a legacy alias.) |
| `--all` | Recurse into every nested process group, not just top-level connectors. |
| `--out-dir DIR` | Directory for the exported JSON files (default: current directory). |
| `--json` | Also print the combined result JSON to stdout. |

### Output files

One file per connector, named:

```
<deployment>_<runtime>_<connectorname>_<YYYYMMDD_HHMMSS>.json
```

Each file contains metadata plus a `parameters` array (name, value, sensitive flag, description). Sensitive values are shown as `<sensitive>` and are never returned by NiFi.

### Examples

```bash
# Export every connector on the demo runtime
python3 get_connector_parameters.py --deployment test --runtime demo

# Just one connector, into an exports/ folder
python3 get_connector_parameters.py --deployment test --runtime demo \
    --connector "Kafka Highperformance" --out-dir exports

# Recurse into nested groups and also print JSON to stdout
python3 get_connector_parameters.py --runtime demo --all --json
```

---

## `put_connector_parameters.py` — write/update

Selects **exactly one** connector and updates parameter values via nipyapi's
inheritance-aware `configure_inherited_params`, which routes each parameter to
its owning context automatically.

### Safety model

- **Dry run by default.** Nothing is changed unless you pass `--apply`.
- A dry-run plan is always printed first, showing which context each parameter
  will be written to. If the dry run reports errors, the apply is refused.
- The target must resolve to a single connector; an ambiguous `--connector`
  errors and asks you to narrow it.
- Parameter values are never echoed (names only); nipyapi masks sensitive
  values in the plan.

### Options

| Flag | Description |
|------|-------------|
| `--connector SUBSTR` | Substring identifying the target connector. Must match exactly one. |
| `--all` | Also search nested process groups when locating the target. |
| `--set "NAME=VALUE"` | Set a parameter value. Repeatable. Splits on the first `=`. |
| `--from-file FILE` | Load values from a flat JSON/YAML map, or from a `get_connector_parameters.py` export (sensitive/empty values are skipped). |
| `--allow-override` | Create parameters that don't already exist (instead of erroring). |
| `--apply` | Actually write the changes. Without it, the script only dry-runs. |

### Examples

```bash
# 1) Preview a change (safe — makes no changes)
python3 put_connector_parameters.py --deployment test --runtime demo \
    --connector "Kafka Highperformance" --set "Kafka Topics=orders,events"

# 2) Apply it
python3 put_connector_parameters.py --deployment test --runtime demo \
    --connector "Kafka Highperformance" --set "Kafka Topics=orders,events" --apply

# 3) Set several parameters at once
python3 put_connector_parameters.py --runtime demo --connector "Kafka Highperformance" \
    --set "Kafka Topics=orders,events" --set "Kafka Broker Endpoints=broker:9092" --apply

# 4) Apply values from an export file (edit it first as needed)
python3 put_connector_parameters.py --runtime demo --connector "Kafka Highperformance" \
    --from-file exports/test_demo_Kafka_Highperformance_20260911_223539.json --apply
```

### Sensitive parameters

Passwords, keys, and tokens are masked by NiFi and cannot be restored from an
export file (they come back as `<sensitive>` and are skipped by `--from-file`).
Set them explicitly with `--set "Password=..."`; nipyapi detects sensitivity
automatically and does not display the value.

---

## Typical workflow

1. `--list-runtimes` to find the runtime/profile.
2. `get_connector_parameters.py ... --out-dir exports` to snapshot current settings.
3. Edit the exported JSON (or prepare a flat JSON/YAML map).
4. `put_connector_parameters.py ... --from-file ...` (dry run) to preview.
5. Re-run with `--apply` to write.
6. `get_connector_parameters.py ...` again to confirm the new values.

---

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success. |
| `2` | No matching connector found (`get`). |
| `3` | Runtime unavailable (HTTP 503) — resume it and retry. |
| `4` | Authentication failed (HTTP 401) — refresh the profile token. |
| `5` | Write refused/failed due to errors in the plan (`put`). |

---

## Troubleshooting

- **HTTP 503:** the runtime is suspended, restarting, or mid-upgrade. Resume it in the Openflow Control Plane and retry.
- **HTTP 401:** the profile's bearer token is invalid/expired. Refresh the `nipyapi` profile.
- **HTTP 503/401 after a runtime was recreated:** a recreated runtime can change its ingress host/URL key. Get the current URL from `DESCRIBE OPENFLOW RUNTIME <db>.<schema>."<name>";` (the `server_url` field), then update `nifi_url` in `~/.nipyapi/profiles.yml` and the `url` in the infrastructure cache.
- **Ambiguous `--connector` (put):** make the substring specific enough to match one connector.
