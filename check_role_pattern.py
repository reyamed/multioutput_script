#!/usr/bin/env python3
"""
check_role_index_patterns.py

Checks the index patterns granted to one or more Elasticsearch security
roles and reports which of those patterns currently resolve to zero
indices, aliases, or data streams (i.e. "empty" / potentially stale
patterns).

Requires: pip install requests

Examples
--------
# Check a single role using basic auth
python check_role_index_patterns.py \
    --host https://localhost:9200 \
    --user elastic --password changeme \
    --role my_role

# Check multiple roles, using an API key, ignoring TLS verification
python check_role_index_patterns.py \
    --host https://es.internal:9200 \
    --api-key VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw== \
    --role role_a --role role_b \
    --insecure

# Verify the server cert against a custom CA (self-signed / internal CA)
python check_role_index_patterns.py \
    --host https://es.internal:9200 \
    --user elastic --password changeme \
    --role my_role \
    --ca-cert /path/to/ca.crt

# Mutual TLS: authenticate to the cluster with a client certificate
python check_role_index_patterns.py \
    --host https://es.internal:9200 \
    --role my_role \
    --ca-cert /path/to/ca.crt \
    --client-cert /path/to/client.crt \
    --client-key /path/to/client.key

# Check EVERY role defined on the cluster
python check_role_index_patterns.py --host https://localhost:9200 \
    --user elastic --password changeme --all-roles

# Output as JSON instead of a text report
python check_role_index_patterns.py ... --json
"""

import argparse
import sys
import json
from typing import Dict, List, Optional

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    sys.stderr.write(
        "This script requires the 'requests' package.\n"
        "Install it with: pip install requests\n"
    )
    sys.exit(1)


def build_session(args: argparse.Namespace) -> requests.Session:
    session = requests.Session()

    # --- TLS / SSL verification ---------------------------------------
    if args.insecure:
        session.verify = False
        # Suppress the noisy "Unverified HTTPS request" warning that
        # urllib3 prints for every single call when verification is off.
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except ImportError:
            pass
    elif args.ca_cert:
        # Verify the server certificate against a specific CA bundle
        # (useful for self-signed / internal CAs instead of the system
        # trust store).
        session.verify = args.ca_cert
    else:
        session.verify = True  # default: verify against system CA store

    # --- Mutual TLS (client certificate) -------------------------------
    if args.client_cert and args.client_key:
        session.cert = (args.client_cert, args.client_key)
    elif args.client_cert:
        # A single PEM file containing both cert and key is also valid.
        session.cert = args.client_cert
    elif args.client_key and not args.client_cert:
        sys.stderr.write("--client-key requires --client-cert to also be set.\n")
        sys.exit(1)

    # --- Auth ------------------------------------------------------------
    if args.api_key:
        # --api-key can be passed already base64-encoded as "id:api_key"
        # (the format Elasticsearch expects in the Authorization header)
        session.headers["Authorization"] = f"ApiKey {args.api_key}"
    elif args.user:
        session.auth = HTTPBasicAuth(args.user, args.password or "")

    session.headers["Content-Type"] = "application/json"
    return session


def get_roles(session: requests.Session, host: str, role_names: Optional[List[str]]) -> Dict[str, dict]:
    """
    Fetch role definitions. If role_names is None/empty, fetch all roles.
    Returns a dict of {role_name: role_definition}.
    """
    if role_names:
        roles: Dict[str, dict] = {}
        for name in role_names:
            url = f"{host}/_security/role/{name}"
            resp = session.get(url)
            if resp.status_code == 404:
                sys.stderr.write(f"[warn] role '{name}' not found, skipping.\n")
                continue
            resp.raise_for_status()
            roles.update(resp.json())
        return roles
    else:
        url = f"{host}/_security/role"
        resp = session.get(url)
        resp.raise_for_status()
        return resp.json()


def extract_index_patterns(role_def: dict) -> List[str]:
    """
    Pull the list of index name patterns out of a role definition.
    A role can have multiple 'indices' privilege blocks, each with its
    own 'names' list.
    """
    patterns = []
    for block in role_def.get("indices", []):
        for name in block.get("names", []):
            if name not in patterns:
                patterns.append(name)
    return patterns


def resolve_pattern(session: requests.Session, host: str, pattern: str) -> dict:
    """
    Use the _resolve/index API to see what a pattern currently matches.
    Returns the parsed response, or an error marker on failure.
    """
    # _resolve/index doesn't handle exclusion patterns (leading '-') or
    # date-math on its own the same way search does; we still query it,
    # since ES will just return no matches for something it can't resolve,
    # which is an accurate reflection of "nothing found".
    url = f"{host}/_resolve/index/{pattern}"
    params = {"expand_wildcards": "all"}
    try:
        resp = session.get(url, params=params)
        if resp.status_code == 404:
            return {"indices": [], "aliases": [], "data_streams": []}
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        return {"error": str(exc), "indices": [], "aliases": [], "data_streams": []}


def is_empty(resolved: dict) -> bool:
    return not (
        resolved.get("indices")
        or resolved.get("aliases")
        or resolved.get("data_streams")
    )


def main():
    parser = argparse.ArgumentParser(
        description="Find index patterns on a role that resolve to no indices."
    )
    parser.add_argument("--host", default="https://localhost:9200",
                         help="Elasticsearch base URL (default: https://localhost:9200)")
    parser.add_argument("--role", action="append", dest="roles",
                         help="Role name to check. Repeat --role for multiple roles.")
    parser.add_argument("--all-roles", action="store_true",
                         help="Check every role defined on the cluster instead of specific ones.")
    parser.add_argument("--user", help="Username for basic auth.")
    parser.add_argument("--password", help="Password for basic auth.")
    parser.add_argument("--api-key", help="Pre-encoded 'id:api_key' base64 string for ApiKey auth.")
    parser.add_argument("--ca-cert", help="Path to CA certificate bundle (PEM) to verify the server's TLS certificate against.")
    parser.add_argument("--client-cert", help="Path to a client certificate (PEM) for mutual TLS. May also be a combined cert+key PEM.")
    parser.add_argument("--client-key", help="Path to the client certificate's private key (PEM), if not bundled with --client-cert.")
    parser.add_argument("--insecure", action="store_true",
                         help="Skip TLS certificate verification (not recommended; disables hostname/CA checks).")
    parser.add_argument("--json", action="store_true",
                         help="Output machine-readable JSON instead of a text report.")
    args = parser.parse_args()

    if not args.roles and not args.all_roles:
        parser.error("Provide at least one --role NAME, or use --all-roles.")

    if args.user and not args.password:
        import getpass
        args.password = getpass.getpass(f"Password for {args.user}: ")

    host = args.host.rstrip("/")
    if not host.lower().startswith("https://"):
        if args.ca_cert or args.client_cert or args.insecure:
            parser.error(
                f"--host '{host}' does not use https:// but TLS options were given. "
                "Use an https:// URL to connect over SSL."
            )
        else:
            sys.stderr.write(
                f"[warn] '{host}' is not using https:// - the connection to Elasticsearch will NOT be encrypted.\n"
            )

    session = build_session(args)

    try:
        roles = get_roles(session, host, None if args.all_roles else args.roles)
    except requests.RequestException as exc:
        sys.stderr.write(f"Failed to fetch role(s): {exc}\n")
        sys.exit(1)

    if not roles:
        sys.stderr.write("No matching roles found.\n")
        sys.exit(1)

    report = {}

    for role_name, role_def in roles.items():
        patterns = extract_index_patterns(role_def)
        empty_patterns = []
        checked = {}

        for pattern in patterns:
            resolved = resolve_pattern(session, host, pattern)
            checked[pattern] = resolved
            if is_empty(resolved):
                empty_patterns.append(pattern)

        report[role_name] = {
            "all_patterns": patterns,
            "empty_patterns": empty_patterns,
            "details": checked,
        }

    if args.json:
        print(json.dumps(report, indent=2))
        return

    # Text report
    any_empty = False
    for role_name, result in report.items():
        print(f"Role: {role_name}")
        if not result["all_patterns"]:
            print("  (no index patterns defined)")
            print()
            continue

        for pattern in result["all_patterns"]:
            detail = result["details"][pattern]
            if "error" in detail:
                print(f"  [ERROR]  {pattern}  -> {detail['error']}")
                continue
            if pattern in result["empty_patterns"]:
                print(f"  [EMPTY]  {pattern}")
            else:
                count = len(detail.get("indices", [])) + len(detail.get("aliases", [])) + len(detail.get("data_streams", []))
                print(f"  [OK]     {pattern}  ({count} match{'es' if count != 1 else ''})")
        print()

        if result["empty_patterns"]:
            any_empty = True

    print("=" * 60)
    if any_empty:
        print("Patterns with NO matching indices/aliases/data streams:")
        for role_name, result in report.items():
            for pattern in result["empty_patterns"]:
                print(f"  {role_name}: {pattern}")
        sys.exit(2)  # non-zero exit so this is easy to use in automation/CI
    else:
        print("All index patterns resolved to at least one index.")


if __name__ == "__main__":
    main()