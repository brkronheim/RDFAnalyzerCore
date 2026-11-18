#!/usr/bin/env python3
"""Download the pinned optional CMS checkout; never modify an existing checkout."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import tempfile


def main(argv=None):
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", help="Git URL, local repository, or Git bundle")
    parser.add_argument("--revision", help="Override the revision recorded in extensions/cms.lock.json")
    parser.add_argument("--destination", type=Path, default=root / "extensions/cms")
    args = parser.parse_args(argv)
    lock = json.loads((root / "extensions/cms.lock.json").read_text())
    repository = args.repository or lock.get("repository")
    if not repository and lock.get("bundle"):
        bundle = root / "extensions" / lock["bundle"]
        if bundle.is_file():
            repository = str(bundle)
    revision = args.revision or lock["revision"]
    destination = args.destination.resolve()
    if destination.exists():
        result = subprocess.run(["git", "-C", str(destination), "rev-parse", "--show-toplevel"],
                                text=True, capture_output=True)
        if result.returncode or Path(result.stdout.strip()).resolve() != destination:
            parser.error(f"Destination exists and is not a separate repository: {destination}")
        head = subprocess.check_output(["git", "-C", str(destination), "rev-parse", "HEAD"], text=True).strip()
        if head != revision:
            parser.error(f"Existing checkout is at {head}, expected {revision}; leaving it untouched")
        print(f"CMS checkout already present at {destination} ({head})")
        return
    if not repository:
        parser.error("Specify --repository <Git URL or local bundle>; no published repository URL is configured yet")
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Failed downloads cannot leave a partial extension in the final location.
    with tempfile.TemporaryDirectory(prefix=".cms-download-", dir=destination.parent) as temporary:
        checkout = Path(temporary) / "cms"
        subprocess.run(["git", "clone", "--no-checkout", "--", repository, str(checkout)], check=True)
        subprocess.run(["git", "-C", str(checkout), "checkout", "--detach", revision], check=True)
        manifest = json.loads((checkout / "rdf-extension.json").read_text())
        if manifest["core_api_version"] != lock["core_api_version"]:
            parser.error("Downloaded extension has an incompatible framework API")
        checkout.rename(destination)
    print(f"CMS checkout ready at {destination}")


if __name__ == "__main__":
    main()
