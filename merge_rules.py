#!/usr/bin/env python3
"""
Merge rules from multiple sources, deduplicate, apply exclusions,
and output sorted rule files.

Supports plain text format (one entry per line) and dnsmasq conf format
(e.g. server=/example.com/114.114.114.114).
"""

import os
import re
import sys
import yaml
import requests

# Regex to match dnsmasq conf lines like: server=/domain/dns_ip
DNSMASQ_PATTERN = re.compile(r"^server=/(.+)/[\d.]+$")

# Timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 10
# Retry count for failed downloads
MAX_RETRIES = 3


def load_config(config_path: str) -> dict:
    """Load and return the YAML configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def download_source(url: str) -> str:
    """
    Download content from a URL with retries.
    Raises an exception if the download ultimately fails.
    Returns the response text on success.
    """
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"  Downloading (attempt {attempt}/{MAX_RETRIES}): {url}")
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            last_error = e
            print(f"    Attempt {attempt} failed: {e}")
    raise RuntimeError(f"Failed to download {url} after {MAX_RETRIES} attempts: {last_error}")


def parse_lines(text: str) -> list[str]:
    """
    Parse raw text into a list of cleaned entries.
    Handles both plain text (one entry per line) and dnsmasq conf format.
    Skips empty lines and comment lines (starting with #).
    """
    entries = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        # Skip empty and comment lines
        if not line or line.startswith("#"):
            continue
        # Check for dnsmasq conf format
        m = DNSMASQ_PATTERN.match(line)
        if m:
            domain = m.group(1).strip()
            if domain:
                entries.append(domain)
        else:
            entries.append(line)
    return entries


def process_rule(rule_name: str, rule_cfg: dict, output_dir: str) -> None:
    """
    Process a single rule type:
      1. Download all sources (fail fast on any error).
      2. Parse each source into entries.
      3. Merge, deduplicate (exact match), apply exclusions (exact match).
      4. Sort alphabetically and write to output file.
    """
    sources = rule_cfg.get("sources", [])
    exclusions = set(rule_cfg.get("exclusions", []))

    print(f"\n{'='*60}")
    print(f"Processing rule: {rule_name}")
    print(f"  Sources: {len(sources)}")
    print(f"  Exclusions: {len(exclusions)}")
    print(f"{'='*60}")

    all_entries: set[str] = set()

    for url in sources:
        text = download_source(url)
        entries = parse_lines(text)
        print(f"    Parsed {len(entries)} entries from {url}")
        all_entries.update(entries)

    # Apply exclusions (exact match only)
    if exclusions:
        before = len(all_entries)
        all_entries -= exclusions
        removed = before - len(all_entries)
        print(f"  Exclusions applied: removed {removed} entries")

    # Sort alphabetically
    sorted_entries = sorted(all_entries)

    # Write output
    output_file = os.path.join(output_dir, f"{rule_name}.txt")
    with open(output_file, "w", encoding="utf-8") as f:
        for entry in sorted_entries:
            f.write(entry + "\n")

    print(f"  Output: {output_file} ({len(sorted_entries)} entries)")


def main():
    # Determine paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "config.yaml")
    output_dir = os.path.join(script_dir, "output")

    # Allow overriding via environment variables
    config_path = os.environ.get("MERGE_CONFIG", config_path)
    output_dir = os.environ.get("MERGE_OUTPUT", output_dir)

    os.makedirs(output_dir, exist_ok=True)

    print(f"Config: {config_path}")
    print(f"Output: {output_dir}")

    config = load_config(config_path)
    rules = config.get("rules", {})

    if not rules:
        print("No rules defined in config. Nothing to do.")
        sys.exit(0)

    # Process all rules — any download failure will raise and abort
    for rule_name, rule_cfg in rules.items():
        process_rule(rule_name, rule_cfg, output_dir)

    print(f"\n{'='*60}")
    print("All rules processed successfully!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
