#!/usr/bin/env python3
"""
Retrieve the parameters of the SQL Connector running on an Openflow runtime.

Uses nipyapi to connect to the runtime (default: the `demo` runtime via the
`ie_demo99_demo` profile), locate the connector's process group by name, and
print every parameter in its bound parameter context.

Sensitive parameter values are never returned by the NiFi REST API; they are
shown as "<sensitive>" so no secrets are echoed.

Usage:
    python3 get_sql_connector_parameters.py
    python3 get_sql_connector_parameters.py --runtime demo
    python3 get_sql_connector_parameters.py --deployment OPENFLOW_SPCS_DEPLOYMENT --runtime demo
    python3 get_sql_connector_parameters.py --profile ie_demo99_demo --name-filter "SQL"
    python3 get_sql_connector_parameters.py --list-runtimes
    python3 get_sql_connector_parameters.py --json
"""

import argparse
import datetime
import glob
import json
import os
import re
import sys

import nipyapi

CACHE_GLOB = os.path.expanduser(
    "~/.snowflake/cortex/memory/openflow_infrastructure_*.json"
)


def load_runtimes():
    """Read the Openflow infrastructure cache(s) and return a list of runtime records.

    Each record: {deployment, deployment_type, runtime, profile, url, connection}.
    Duplicate (deployment, runtime) pairs are de-duplicated across cache files.
    """
    runtimes = []
    seen = set()
    for path in sorted(glob.glob(CACHE_GLOB)):
        try:
            with open(path) as fh:
                data = json.load(fh)
        except (OSError, ValueError):
            continue
        connection = data.get("connection")
        for dep in data.get("deployments", []):
            dep_name = dep.get("deployment_name") or dep.get("name")
            for rt in dep.get("runtimes", []):
                rt_name = rt.get("runtime_name") or rt.get("name")
                key = (dep_name, rt_name, rt.get("nipyapi_profile") or rt.get("profile"))
                if key in seen:
                    continue
                seen.add(key)
                runtimes.append(
                    {
                        "deployment": dep_name,
                        "deployment_type": dep.get("deployment_type") or dep.get("type"),
                        "runtime": rt_name,
                        "profile": rt.get("nipyapi_profile") or rt.get("profile"),
                        "url": rt.get("url"),
                        "connection": connection,
                    }
                )
    return runtimes


def resolve_record(deployment, runtime):
    """Resolve a single runtime cache record from a deployment and/or runtime name.

    Selection is driven by runtime; deployment narrows the match when the cache
    has a matching deployment name, otherwise it is treated as a label only
    (this account's cache stores deployment as null).
    """
    runtimes = load_runtimes()
    if not runtimes:
        raise SystemExit(
            "No Openflow infrastructure cache found. Pass --profile explicitly, or "
            "run the Openflow session setup to populate the cache."
        )

    matches = runtimes
    if runtime:
        matches = [
            r for r in matches
            if r["runtime"] and r["runtime"].lower() == runtime.lower()
        ]
    if deployment:
        dep_matches = [
            r for r in matches
            if r["deployment"] and r["deployment"].lower() == deployment.lower()
        ]
        if dep_matches:
            matches = dep_matches

    if not matches:
        raise SystemExit(
            f"No runtime matched deployment={deployment!r} runtime={runtime!r}. "
            "Use --list-runtimes to see available options."
        )
    if len(matches) > 1:
        options = ", ".join(
            f"{m['deployment']}/{m['runtime']} (profile {m['profile']})" for m in matches
        )
        raise SystemExit(
            f"Ambiguous selection — {len(matches)} runtimes matched: {options}. "
            "Narrow it with --deployment and/or --runtime, or use --profile."
        )
    return matches[0]


def record_for_profile(profile):
    """Return the cache record whose profile matches, or None."""
    for r in load_runtimes():
        if r["profile"] == profile:
            return r
    return None


def sanitize_component(value, fallback="unknown"):
    """Make a filename-safe token from a name component."""
    if not value:
        return fallback
    token = re.sub(r"[^A-Za-z0-9.-]+", "_", str(value)).strip("_")
    return token or fallback


def print_runtimes():
    """List runtimes available in the cache."""
    runtimes = load_runtimes()
    if not runtimes:
        print("No Openflow infrastructure cache found.", file=sys.stderr)
        return
    print(f"{'DEPLOYMENT':<28} {'RUNTIME':<20} {'PROFILE':<28} CONNECTION")
    for r in runtimes:
        print(
            f"{str(r['deployment']):<28} {str(r['runtime']):<20} "
            f"{str(r['profile']):<28} {r['connection']}"
        )


def find_matching_process_groups(root_pg_id, name_filter):
    """Return process groups whose name contains name_filter (case-insensitive)."""
    needle = name_filter.lower()
    all_pgs = nipyapi.canvas.list_all_process_groups(root_pg_id)
    return [pg for pg in all_pgs if needle in pg.component.name.lower()]


def get_context_parameters(parameter_context_ref):
    """Fetch the full parameter context and return a list of parameter dicts."""
    ctx = nipyapi.nifi.ParameterContextsApi().get_parameter_context(
        parameter_context_ref.id
    )
    params = []
    for entry in ctx.component.parameters:
        p = entry.parameter
        if p.sensitive:
            value = "<sensitive>"
        else:
            value = p.value
        params.append(
            {
                "name": p.name,
                "value": value,
                "sensitive": bool(p.sensitive),
                "description": p.description or "",
            }
        )
    return ctx.component.name, sorted(params, key=lambda x: x["name"])


def main():
    parser = argparse.ArgumentParser(
        description="Retrieve SQL Connector parameters from an Openflow runtime via nipyapi."
    )
    parser.add_argument(
        "--deployment",
        help="Openflow deployment name to select (resolved to a profile via the cache).",
    )
    parser.add_argument(
        "--runtime",
        help="Openflow runtime name to select (e.g. 'demo'); resolved to a profile via the cache.",
    )
    parser.add_argument(
        "--profile",
        help="nipyapi profile to use directly. Overrides --deployment/--runtime. "
        "Defaults to the 'demo' runtime when nothing is specified.",
    )
    parser.add_argument(
        "--list-runtimes",
        action="store_true",
        help="List deployments/runtimes available in the cache, then exit.",
    )
    parser.add_argument(
        "--name-filter",
        default="SQL",
        help="Substring to match the connector's process group name (default: SQL).",
    )
    parser.add_argument(
        "--out-dir",
        default=".",
        help="Directory to write exported JSON files into (default: current directory).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Also print the combined result JSON to stdout.",
    )
    args = parser.parse_args()

    if args.list_runtimes:
        print_runtimes()
        return

    # Resolve which nipyapi profile to use, plus the deployment/runtime labels
    # used for output filenames.
    #   --profile wins; else resolve from --deployment/--runtime via the cache;
    #   else fall back to the default 'demo' runtime profile.
    if args.profile:
        profile = args.profile
        record = record_for_profile(profile)
    elif args.deployment or args.runtime:
        record = resolve_record(args.deployment, args.runtime)
        profile = record["profile"]
    else:
        profile = "ie_demo99_demo"
        record = record_for_profile(profile)
    args.profile = profile

    # Filename labels: explicit args take precedence, then the cache record.
    deployment_label = sanitize_component(
        args.deployment or (record["deployment"] if record else None)
    )
    runtime_label = sanitize_component(
        args.runtime or (record["runtime"] if record else None) or profile
    )

    # Activate the profile (loads NiFi URL + bearer token from ~/.nipyapi/profiles.yml)
    nipyapi.profiles.switch(profile)

    try:
        root_pg_id = nipyapi.canvas.get_root_pg_id()
        matches = find_matching_process_groups(root_pg_id, args.name_filter)
    except nipyapi.nifi.rest.ApiException as exc:
        if exc.status == 503:
            print(
                f"Runtime for profile '{args.profile}' is unavailable (HTTP 503). "
                "It is likely suspended, restarting, or mid-upgrade. Resume it in "
                "the Openflow Control Plane and retry.",
                file=sys.stderr,
            )
            sys.exit(3)
        if exc.status == 401:
            print(
                f"Authentication failed for profile '{args.profile}' (HTTP 401). "
                "The token has likely expired — refresh the nipyapi profile and retry.",
                file=sys.stderr,
            )
            sys.exit(4)
        raise

    if not matches:
        print(
            f"No process group matching '{args.name_filter}' found on profile "
            f"'{args.profile}'.",
            file=sys.stderr,
        )
        sys.exit(2)

    results = []
    for pg in matches:
        pc_ref = pg.component.parameter_context
        if pc_ref is None:
            results.append(
                {
                    "process_group": pg.component.name,
                    "process_group_id": pg.id,
                    "parameter_context": None,
                    "parameters": [],
                }
            )
            continue

        ctx_name, params = get_context_parameters(pc_ref)
        results.append(
            {
                "process_group": pg.component.name,
                "process_group_id": pg.id,
                "parameter_context": ctx_name,
                "parameters": params,
            }
        )

    # Write one JSON export per matched connector.
    #   filename: <deployment>_<runtime>_<connectorname>_<YYYYMMDD_HHMMSS>.json
    os.makedirs(args.out_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    written = []
    for r in results:
        connector_label = sanitize_component(r["process_group"])
        filename = f"{deployment_label}_{runtime_label}_{connector_label}_{timestamp}.json"
        path = os.path.join(args.out_dir, filename)
        export = {
            "exported_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "deployment": args.deployment or (record["deployment"] if record else None),
            "runtime": args.runtime or (record["runtime"] if record else None),
            "profile": profile,
            "connector": r["process_group"],
            "process_group_id": r["process_group_id"],
            "parameter_context": r["parameter_context"],
            "parameters": r["parameters"],
        }
        with open(path, "w") as fh:
            json.dump(export, fh, indent=2)
        written.append(path)

    if args.json:
        print(json.dumps(results, indent=2))

    for r in results:
        print("=" * 70)
        print(f"Connector (process group): {r['process_group']}")
        print(f"  Process group id : {r['process_group_id']}")
        print(f"  Parameter context: {r['parameter_context']}")
        if not r["parameters"]:
            print("  (no parameters bound)")
            continue
        print(f"  Parameters ({len(r['parameters'])}):")
        for p in r["parameters"]:
            flag = " [sensitive]" if p["sensitive"] else ""
            value = p["value"] if p["value"] is not None else ""
            print(f"    - {p['name']}{flag}: {value}")
    print("=" * 70)
    print(f"\nWrote {len(written)} file(s):")
    for path in written:
        print(f"  {path}")


if __name__ == "__main__":
    main()
