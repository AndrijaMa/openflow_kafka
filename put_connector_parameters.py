#!/usr/bin/env python3
"""
Write (set) parameters on a connector running on an Openflow runtime.

Companion to get_connector_parameters.py. Selects a single connector (by
deployment/runtime/profile + --connector) and updates parameter values using
nipyapi's inheritance-aware `configure_inherited_params`, which routes each
parameter to its owning context automatically.

SAFETY:
  - Dry run by default. Nothing is changed unless you pass --apply.
  - The dry-run plan shows which context each parameter would be written to.
  - Sensitive values are masked by nipyapi in the plan and are never echoed here.

Parameter values can come from:
  --set "Name=Value"   (repeatable)
  --from-file FILE      (flat JSON/YAML map, OR a get_connector_parameters.py
                         export file — sensitive/empty values are skipped)

Usage:
    # Preview (dry run) — safe, makes no changes
    python3 put_connector_parameters.py --deployment test --runtime demo \
        --connector "Kafka Highperformance" --set "Kafka Topics=orders,events"

    # Apply the change
    python3 put_connector_parameters.py --deployment test --runtime demo \
        --connector "Kafka Highperformance" --set "Kafka Topics=orders,events" --apply

    # From a file (e.g. an export produced by get_connector_parameters.py)
    python3 put_connector_parameters.py --runtime demo --connector "Kafka Highperformance" \
        --from-file exports/test_demo_Kafka_Highperformance_20260911_223539.json --apply

    python3 put_connector_parameters.py --list-runtimes
"""

import argparse
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
    """Read the Openflow infrastructure cache(s) and return a list of runtime records."""
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
    """Resolve a single runtime cache record from a deployment and/or runtime name."""
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


def list_top_level_connectors(root_pg_id):
    """Return the immediate child process groups of root (each = one connector/flow)."""
    entity = nipyapi.nifi.ProcessGroupsApi().get_process_groups(root_pg_id)
    return list(entity.process_groups or [])


def select_target(root_pg_id, connector_filter, recurse):
    """Select exactly one connector process group to write to.

    Writing to more than one connector at once is not allowed — if the filter is
    ambiguous, the caller is asked to narrow it.
    """
    if recurse:
        pgs = nipyapi.canvas.list_all_process_groups(root_pg_id)
    else:
        pgs = list_top_level_connectors(root_pg_id)
    if connector_filter:
        needle = connector_filter.lower()
        pgs = [pg for pg in pgs if needle in pg.component.name.lower()]

    if not pgs:
        scope = f"matching '{connector_filter}'" if connector_filter else "found"
        raise SystemExit(f"No connector process group {scope} on this runtime.")
    if len(pgs) > 1:
        names = ", ".join(sorted(pg.component.name for pg in pgs))
        raise SystemExit(
            f"--connector matched {len(pgs)} process groups: {names}. "
            "Narrow --connector so it identifies exactly one connector."
        )
    return pgs[0]


def parse_set_args(set_items):
    """Turn ["Name=Value", ...] into {name: value}. Splits on the first '='."""
    out = {}
    for item in set_items or []:
        if "=" not in item:
            raise SystemExit(f"--set must be in 'Name=Value' form, got: {item!r}")
        name, value = item.split("=", 1)
        name = name.strip()
        if not name:
            raise SystemExit(f"--set has an empty parameter name: {item!r}")
        out[name] = value
    return out


def load_params_file(path):
    """Load a parameters file into a flat {name: value} dict.

    Accepts a flat JSON/YAML map, or a get_connector_parameters.py export
    (object with a "parameters" list). Sensitive or empty/masked values from an
    export are skipped — they cannot be restored and must be set via --set.
    """
    with open(path) as fh:
        text = fh.read()
    try:
        data = json.loads(text)
    except ValueError:
        try:
            import yaml  # optional
        except ImportError:
            raise SystemExit(
                f"Could not parse {path} as JSON. Install pyyaml to use YAML files."
            )
        data = yaml.safe_load(text)

    if isinstance(data, dict) and isinstance(data.get("parameters"), list):
        out = {}
        skipped = []
        for p in data["parameters"]:
            if p.get("sensitive"):
                skipped.append(p.get("name"))
                continue
            value = p.get("value")
            if value in (None, "<sensitive>"):
                skipped.append(p.get("name"))
                continue
            out[p["name"]] = value
        if skipped:
            print(
                f"Note: skipped {len(skipped)} sensitive/empty parameter(s) from the "
                f"export (set these with --set): {', '.join(str(s) for s in skipped)}",
                file=sys.stderr,
            )
        return out
    if isinstance(data, dict):
        return dict(data)
    raise SystemExit(f"Unsupported parameters file structure in {path}.")


def main():
    parser = argparse.ArgumentParser(
        description="Write parameters to an Openflow connector via nipyapi (dry-run by default)."
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
        "--connector",
        dest="connector",
        default=None,
        help="Case-insensitive substring identifying the target connector. "
        "Must match exactly one connector.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Search nested process groups too (not just top-level connectors).",
    )
    parser.add_argument(
        "--set",
        action="append",
        metavar="NAME=VALUE",
        help="Set a parameter value. Repeatable. Splits on the first '='.",
    )
    parser.add_argument(
        "--from-file",
        dest="from_file",
        help="Load parameter values from a flat JSON/YAML map or an export file.",
    )
    parser.add_argument(
        "--allow-override",
        action="store_true",
        help="Create parameters that don't already exist (top-level) instead of erroring.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write the changes. Without this flag the script only dry-runs.",
    )
    args = parser.parse_args()

    if args.list_runtimes:
        print_runtimes()
        return

    # Collect the parameters to write (file first, --set overrides).
    params = {}
    if args.from_file:
        params.update(load_params_file(args.from_file))
    params.update(parse_set_args(args.set))
    if not params:
        raise SystemExit("Nothing to write. Provide --set NAME=VALUE and/or --from-file.")

    # Resolve profile (same precedence as get_connector_parameters.py).
    if args.profile:
        profile = args.profile
    elif args.deployment or args.runtime:
        profile = resolve_record(args.deployment, args.runtime)["profile"]
    else:
        profile = "ie_demo99_demo"
    args.profile = profile

    nipyapi.profiles.switch(profile)

    try:
        root_pg_id = nipyapi.canvas.get_root_pg_id()
        target = select_target(root_pg_id, args.connector, args.all)
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

    pg_id = target.id
    print(f"Target connector : {target.component.name}")
    print(f"Process group id : {pg_id}")
    # Values may be sensitive — list names only, never echo values.
    print(f"Parameters to set: {', '.join(sorted(params))}")

    # Always dry-run first to show the plan (which context each param lands in).
    plan = nipyapi.ci.configure_inherited_params(
        process_group_id=pg_id,
        parameters=params,
        dry_run=True,
        allow_override=args.allow_override,
    )
    print("\n--- dry-run plan ---")
    print(json.dumps(plan, indent=2))

    if plan.get("errors"):
        print("\nRefusing to apply: the dry run reported errors above.", file=sys.stderr)
        sys.exit(5)

    if not args.apply:
        print("\nDry run only. Re-run with --apply to write these changes.")
        return

    result = nipyapi.ci.configure_inherited_params(
        process_group_id=pg_id,
        parameters=params,
        dry_run=False,
        allow_override=args.allow_override,
    )
    print("\n--- applied ---")
    print(json.dumps(result, indent=2))
    if result.get("errors"):
        sys.exit(5)


if __name__ == "__main__":
    main()
