#!/usr/bin/env python3
"""Interactively deploy a Snowflake-managed Openflow connector to a chosen deployment/runtime.

Runtimes come from the Openflow cache (~/.snowflake/cortex/memory/openflow_infrastructure_*.json)
and must have a matching nipyapi profile in ~/.nipyapi/profiles.yml.

Usage:
    python3 deploy_connector.py                 # fully interactive
    python3 deploy_connector.py --connector kafka-high-performance --profile ie_demo99_demo
    python3 deploy_connector.py --deployment of2-- --runtime demo --connector kafka-high-performance
"""
import argparse
import glob
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from urllib.parse import urlparse

REGISTRY = "ConnectorFlowRegistryClient"
BUCKET = "connectors"
CACHE_GLOB = os.path.expanduser("~/.snowflake/cortex/memory/openflow_infrastructure_*.json")
PROFILES_FILE = os.path.expanduser("~/.nipyapi/profiles.yml")


def nipyapi(profile, *args):
    """Run a nipyapi CLI command and return parsed JSON output."""
    cmd = ["nipyapi", "--profile", profile, *args]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        sys.exit(f"nipyapi failed: {' '.join(cmd)}\n{res.stderr.strip() or res.stdout.strip()}")
    out = res.stdout
    start = out.find("{")  # skip any log lines before the JSON payload
    return json.loads(out[start:]) if start >= 0 else {}


def existing_profiles():
    if not os.path.exists(PROFILES_FILE):
        return set()
    with open(PROFILES_FILE) as f:
        return {m.group(1) for m in re.finditer(r"^([A-Za-z][\w\-]*):\s*$", f.read(), re.M)}


def discover_runtimes():
    """Return {deployment: [runtime dicts]} for runtimes that have a usable profile."""
    profiles = existing_profiles()
    seen, deployments = set(), {}
    for path in sorted(glob.glob(CACHE_GLOB)):
        with open(path) as f:
            cache = json.load(f)
        for dep in cache.get("deployments", []):
            for rt in dep.get("runtimes", []):
                prof = rt.get("nipyapi_profile")
                if not prof or prof not in profiles or prof in seen:
                    continue
                seen.add(prof)
                # Cache may lack deployment_name; fall back to the runtime host (one host per deployment)
                dep_name = dep.get("deployment_name") or urlparse(rt.get("url", "")).netloc or "unknown"
                deployments.setdefault(dep_name, []).append(rt)
    return deployments


def choose(prompt, options, label=lambda o: str(o)):
    if not options:
        sys.exit(f"No options available for: {prompt}")
    print(f"\n{prompt}")
    for i, o in enumerate(options, 1):
        print(f"  {i:>2}. {label(o)}")
    while True:
        raw = input(f"Select 1-{len(options)}: ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1]
        print("Invalid choice.")


def match_one(kind, wanted, options, keys):
    """Resolve `wanted` against options: exact match first, then unique case-insensitive substring."""
    w = wanted.lower()
    exact = [o for o in options if any(k(o).lower() == w for k in keys)]
    if exact:
        return exact[0]
    partial = [o for o in options if any(w in k(o).lower() for k in keys)]
    if len(partial) == 1:
        return partial[0]
    names = ", ".join(keys[0](o) for o in options)
    sys.exit(f"{kind} '{wanted}' {'is ambiguous' if partial else 'not found'}. Available: {names}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", help="nipyapi profile (skips deployment/runtime prompts)")
    ap.add_argument("--deployment", help="deployment name or host (exact or partial match)")
    ap.add_argument("--runtime", help="runtime name or key (exact or partial match)")
    ap.add_argument("--connector", help="connector flow name (skips connector prompt)")
    ap.add_argument("--version", help="specific connector version (default: latest)")
    ap.add_argument("--out-dir", default=".", help="where to save the exported flow JSON")
    ap.add_argument("--yes", action="store_true", help="don't ask for confirmation")
    args = ap.parse_args()

    # 1. Deployment + runtime
    if args.profile:
        profile, runtime_label = args.profile, args.profile
    else:
        deployments = discover_runtimes()
        if args.deployment:
            dep = match_one("Deployment", args.deployment, sorted(deployments), [lambda d: d])
        elif args.runtime:
            # Runtime given alone: search all deployments, ask only if it is ambiguous
            dep = None
        else:
            dep = choose("Choose a deployment:", sorted(deployments))

        pool = deployments[dep] if dep else [r for rts in deployments.values() for r in rts]
        rt_keys = [lambda r: r["runtime_name"], lambda r: r.get("runtime_key") or "",
                   lambda r: urlparse(r.get("url", "")).path.split("/")[1] if r.get("url") else ""]
        if args.runtime:
            rt = match_one("Runtime", args.runtime, pool, rt_keys)
        else:
            rt = choose("Choose a runtime:", pool,
                        label=lambda r: f"{r['runtime_name']}  ({r['url']})")
        print(f"Target: deployment={dep or urlparse(rt['url']).netloc} runtime={rt['runtime_name']}")
        profile, runtime_label = rt["nipyapi_profile"], rt["runtime_name"]

    # 2. Connector from the managed registry
    flows = nipyapi(profile, "ci", "list_registry_flows",
                    "--registry_client", REGISTRY, "--bucket", BUCKET).get("flows", [])
    names = sorted(f["name"] for f in flows)
    connector = args.connector or choose("Choose a connector:", names)
    if connector not in names:
        sys.exit(f"Connector '{connector}' not found in {REGISTRY}/{BUCKET}")

    # 3. Warn if already on the canvas (shared parameter contexts)
    existing = nipyapi(profile, "ci", "list_flows").get("process_groups", [])
    dupes = [pg for pg in existing if pg.get("flow_name") == connector]
    if dupes:
        print(f"\nWarning: '{connector}' already deployed on this runtime "
              f"({', '.join(pg['name'] for pg in dupes)}). A new copy will share its parameter context.")

    if not args.yes:
        ans = input(f"\nDeploy '{connector}' to runtime '{runtime_label}' (profile {profile})? [y/N]: ")
        if ans.strip().lower() != "y":
            sys.exit("Aborted.")

    # 4. Deploy
    deploy_args = ["ci", "deploy_flow", "--registry_client", REGISTRY,
                   "--bucket", BUCKET, "--flow", connector]
    if args.version:
        deploy_args += ["--version", args.version]
    result = nipyapi(profile, *deploy_args)
    pg_id = result["process_group_id"]
    print(f"\nDeployed {result['process_group_name']} v{result.get('deployed_version')} (PG {pg_id})")

    # 5. Export flow definition with timestamp
    os.makedirs(args.out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = os.path.join(args.out_dir, f"{connector}_{ts}.json")
    nipyapi(profile, "ci", "export_flow_definition", "--process_group_id", pg_id, "--file_path", out_file)
    print(f"Exported flow definition -> {out_file}")

    # 6. Status (expected: stopped, some invalid until parameters are set)
    st = nipyapi(profile, "ci", "get_status", "--process_group_id", pg_id)
    print("Status:", {k: st.get(k) for k in
                      ("running_processors", "stopped_processors", "invalid_processors", "bulletin_errors")})
    print("\nNext: configure parameters, attach an EAI for external sources, enable controllers, start.")


if __name__ == "__main__":
    main()
