#!/usr/bin/env python3
"""Clone or fast-forward the optional CMS checkout."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import tempfile


def main(argv=None):
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", help="Git URL, local repository, or Git bundle for a new checkout")
    parser.add_argument("--revision", help="Request a specific commit, tag, or Git revision")
    parser.add_argument("--destination", type=Path, default=root / "extensions/cms")
    args = parser.parse_args(argv)
    lock = json.loads((root / "extensions/cms.lock.json").read_text())
    repository = args.repository or lock.get("repository")
    if not repository and lock.get("bundle"):
        bundle = root / "extensions" / lock["bundle"]
        if bundle.is_file():
            repository = str(bundle)
    revision = args.revision or lock.get("revision")
    branch = lock.get("branch", "main")

    def require_compatible(manifest):
        if manifest["core_api_version"] != lock["core_api_version"]:
            parser.error("CMS extension has an incompatible framework API")

    destination = args.destination.resolve()
    if destination.exists():
        git = ["git", "-C", str(destination)]
        result = subprocess.run([*git, "rev-parse", "--show-toplevel"],
                                text=True, capture_output=True)
        if result.returncode or Path(result.stdout.strip()).resolve() != destination:
            parser.error(f"Destination exists and is not a separate repository: {destination}")
        if revision:
            head = subprocess.check_output([*git, "rev-parse", "HEAD"], text=True).strip()
            expected = subprocess.run(
                [*git, "rev-parse", "--verify", "--end-of-options", f"{revision}^{{commit}}"],
                capture_output=True, text=True,
            )
            if expected.returncode or head != expected.stdout.strip():
                parser.error(f"Existing checkout is at {head}, expected {revision}; leaving it untouched")
        else:
            current = subprocess.check_output([*git, "branch", "--show-current"], text=True).strip()
            if current != branch:
                parser.error(f"Switch the existing CMS checkout to {branch} before updating it")
            if subprocess.check_output([*git, "status", "--porcelain", "--untracked-files=all"], text=True).strip():
                parser.error(f"CMS checkout has local changes: {destination}")
            tracking = f"refs/remotes/origin/{branch}"
            subprocess.run([*git, "fetch", "--no-write-fetch-head", "origin", f"refs/heads/{branch}:{tracking}"],
                           check=True)
            commit = subprocess.check_output([*git, "rev-parse", tracking], text=True).strip()
            require_compatible(json.loads(subprocess.check_output(
                [*git, "show", f"{commit}:rdf-extension.json"], text=True,
            )))
            subprocess.run([*git, "merge", "--ff-only", "--no-overwrite-ignore", commit], check=True)
        head = subprocess.check_output([*git, "rev-parse", "HEAD"], text=True).strip()
        print(f"CMS checkout ready at {destination} ({head})")
        return
    if not repository:
        parser.error("Specify --repository <Git URL or local bundle>; no published repository URL is configured yet")
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Failed downloads cannot leave a partial extension in the final location.
    with tempfile.TemporaryDirectory(prefix=".cms-download-", dir=destination.parent) as temporary:
        checkout = Path(temporary) / "cms"
        clone_options = ["--no-checkout"] if revision else ["--branch", branch]
        subprocess.run(["git", "clone", *clone_options, "--", repository, str(checkout)], check=True)
        git = ["git", "-C", str(checkout)]
        if revision:
            commit = subprocess.check_output(
                [*git, "rev-parse", "--verify", "--end-of-options", f"{revision}^{{commit}}"], text=True,
            ).strip()
            subprocess.run([*git, "checkout", "--detach", commit], check=True)
        elif subprocess.check_output([*git, "branch", "--show-current"], text=True).strip() != branch:
            parser.error(f"CMS repository does not have a {branch} branch")
        require_compatible(json.loads((checkout / "rdf-extension.json").read_text()))
        checkout.rename(destination)
    print(f"CMS checkout ready at {destination}")


if __name__ == "__main__":
    main()
