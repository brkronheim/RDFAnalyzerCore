"""Compose verified datacards into one explicitly labeled joint likelihood."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess

from .downstream import _FIT_OPTIONS, describe_fit_diagnostics, downstream_paths
from run_bundle import atomic_directory, derived_artifact, load_artifact_receipt, verify_artifact, write_json
from statistical_inference.combine import _combine_runtime_environment, _find_combine


def joint_paths(output_root):
    root = Path(output_root) / ".joint"
    return {name: root / name for name in ("datacards", "datacards.json", "fit", "fit.json",
                                           "fit_diagnostics", "fit_diagnostics.json", "logs")}


def describe_joint_fit(settings, downstream, output_root, *, base=Path(".")):
    allowed = {"cards", "correlations", "combine_cards_exe", "fit", "fit_diagnostics"}
    if not isinstance(settings, dict) or settings.keys() - allowed:
        raise ValueError("joint_fit has unknown settings")
    if settings.get("correlations") != "by_name":
        raise ValueError("joint_fit.correlations must be by_name; set nuisance names in the source cards")
    options = settings.get("fit", {})
    if not isinstance(options, dict) or options.keys() - _FIT_OPTIONS:
        raise ValueError("joint_fit.fit has unknown options")
    if options.get("fitting_backend", "combine") not in ("combine", "rabbit"):
        raise ValueError("joint_fit.fit requires the combine or rabbit fitting backend")
    sources = settings.get("cards")
    if not isinstance(sources, dict) or len(sources) < 2:
        raise ValueError("joint_fit.cards must map at least two labels to cards")
    cards, saved_files = {}, []
    for label, source in sources.items():
        if not isinstance(label, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", label):
            raise ValueError(f"invalid joint fit channel label {label!r}")
        if not isinstance(source, dict) or set(source) not in ({"era", "card"}, {"directory", "receipt", "card"}):
            raise ValueError(f"joint_fit.cards.{label} requires era and card, or directory, receipt and card")
        name = source["card"]
        if not isinstance(name, str) or not name or Path(name).is_absolute() or ".." in Path(name).parts:
            raise ValueError(f"joint_fit.cards.{label}.card must be a relative datacard path")
        if Path(name).suffix != ".txt":
            raise ValueError(f"joint_fit.cards.{label}.card must end in .txt")
        if "era" in source:
            era = str(source["era"])
            if era not in downstream or "datacards" not in downstream[era]:
                raise ValueError(f"joint_fit.cards.{label} requires downstream datacards for {era}")
            paths = downstream_paths(output_root, era)
            directory, receipt = paths["datacards"], paths["datacards_receipt"]
        else:
            if any(not isinstance(source[key], str) or not source[key].strip() for key in ("directory", "receipt")):
                raise ValueError(f"joint_fit.cards.{label} directory and receipt must be paths")
            directory = (base / Path(source["directory"]).expanduser()).resolve()
            receipt = (base / Path(source["receipt"]).expanduser()).resolve()
            recorded = load_artifact_receipt(receipt)
            if (recorded.get("kind") != "datacards"
                    or str((directory / name).resolve()) not in recorded["files"]):
                raise ValueError(f"joint_fit.cards.{label}.receipt does not describe {directory / name}")
            saved_files.extend(recorded["files"])
        cards[label] = {"directory": str(directory), "receipt": str(receipt), "card": name}
    paths = [(source["directory"], source["card"]) for source in cards.values()]
    if len(paths) != len(set(paths)):
        raise ValueError("joint_fit.cards contains duplicate cards")
    result = {"cards": cards, "correlations": "by_name", "saved_files": list(dict.fromkeys(saved_files)),
              "fit": {"fitting_backend": "combine", **options}}
    if "combine_cards_exe" in settings:
        if not isinstance(settings["combine_cards_exe"], str) or not settings["combine_cards_exe"].strip():
            raise ValueError("joint_fit.combine_cards_exe must be an executable")
        command = Path(settings["combine_cards_exe"]).expanduser()
        result["combine_cards_exe"] = str((base / command).resolve()) if command.parent != Path(".") else str(command)
    if "fit_diagnostics" in settings:
        result["fit_diagnostics"] = describe_fit_diagnostics(settings["fit_diagnostics"], ["fit"],
                                                               "joint_fit.fit_diagnostics")
    return result


def joint_targets(settings, output_root):
    paths = joint_paths(output_root)
    return [str(paths["fit_diagnostics.json" if "fit_diagnostics" in settings else "fit.json"])]


def _stage_card(path, recorded, target):
    # Each channel can reference several shape files. Keep each copy beside its
    # temporary card so combineCards.py emits relocatable relative paths.
    target.mkdir(parents=True)
    shape_paths = {}
    lines = []
    for line in path.read_text().splitlines():
        words = line.split()
        if len(words) > 1 and (words[1] == "extArg" or
                               (words[1] == "rateParam" and any(".root:" in word for word in words[2:]))):
            raise ValueError(f"joint card staging does not support external RooWorkspace arguments in {path}")
        if words[:1] != ["shapes"] or len(words) < 4 or words[3] == "FAKE":
            lines.append(line)
            continue
        shape = (path.parent / words[3]).resolve(strict=True)
        if str(shape) not in recorded["files"]:
            raise ValueError(f"unverified shapes file in {path}: {words[3]}")
        if shape not in shape_paths:
            shape_paths[shape] = f"shapes-{len(shape_paths) + 1}.root"
            shutil.copy2(shape, target / shape_paths[shape])
        words[3] = shape_paths[shape]
        lines.append(" ".join(words))
    prepared = target / "input.card"
    prepared.write_text("\n".join(lines) + "\n")
    return prepared


def _canonical_combination(text):
    """Preserve equivalent Combine syntax accepted by the Rabbit reader."""
    lines = ["# " + line if line.startswith("Combination of ") else line for line in text.splitlines()]
    tokens = [line.split("#", 1)[0].split() for line in lines]
    channels = next((set(line[1:]) for line in tokens if line[:1] == ["bin"]), set())
    entries = {i: line for i, line in enumerate(tokens) if line[1:2] == ["autoMCStats"]}
    policies = {tuple(line[2:]) for line in entries.values()}
    # combineCards.py expands a wildcard policy into a directive per channel.
    # Use the equivalent wildcard only when every channel has that policy.
    if (channels and len(channels) == len(entries) and len(policies) == 1
            and channels == {line[0] for line in entries.values()}):
        lines = [line for i, line in enumerate(lines) if i not in entries]
        lines.append("* autoMCStats " + " ".join(next(iter(policies))))
    return "\n".join(lines) + "\n"


def combine_joint_cards(settings, destination, receipt):
    merger = settings.get("combine_cards_exe")
    if merger is None or settings["fit"].get("fitting_backend", "combine") == "combine":
        executable = _find_combine(settings["fit"].get("combine_exe"))
        local = Path(executable).parent / "combineCards.py"
        merger = merger or (str(local) if local.is_file() else "combineCards.py")
        environment, _, _, _ = _combine_runtime_environment(executable)
        merger = shutil.which(merger, path=environment["PATH"])
    else:
        merger = shutil.which(merger)
        if merger:
            environment, _, _, _ = _combine_runtime_environment(merger)
    if merger is None:
        raise ValueError("CMS Combine's combineCards.py is required for joint fits")

    parents = {}
    records = {}
    cards = {}
    with atomic_directory(destination) as staging:
        for label, source in settings["cards"].items():
            receipt_path = source["receipt"]
            if receipt_path not in records:
                records[receipt_path] = verify_artifact(receipt_path)
            recorded = records[receipt_path]
            directory = Path(source["directory"]).resolve()
            card = (directory / source["card"]).resolve(strict=True)
            if (recorded.get("kind") != "datacards" or not card.is_relative_to(directory)
                    or str(card) not in recorded["files"]):
                raise ValueError(f"unverified joint datacard: {card}")
            parents[label] = {"artifact_id": recorded["artifact_id"], "card": str(card)}
            cards[label] = _stage_card(card, recorded, staging / "inputs" / label)

        command = [merger, *(f"{label}={card.relative_to(staging)}" for label, card in cards.items())]
        combined = staging / "datacard_joint.txt"
        with combined.open("w") as stream:
            process = subprocess.run(command, cwd=staging, env=environment, stdout=stream, check=False)
        if process.returncode or not combined.stat().st_size:
            raise RuntimeError("combineCards.py failed to produce a joint datacard")
        contents = _canonical_combination(combined.read_text())
        combined.write_text(contents)
        for line in contents.splitlines():
            words = line.split()
            if words[:1] == ["shapes"] and len(words) >= 4 and words[3] != "FAKE":
                path = (staging / words[3]).resolve()
                if not path.is_relative_to(staging) or not path.is_file():
                    raise ValueError(f"joint card references shapes outside the staged inputs: {words[3]}")
        for card in cards.values():
            card.unlink()
        write_json(staging / "joint_provenance.json", {"correlations": "by_name", "sources": parents,
                                                       "command": command, "cards": list(cards)})
    files = [path for path in destination.rglob("*") if path.is_file()]
    write_json(receipt, derived_artifact(kind="datacards", parents=parents, files=files,
                                        configuration={"joint": True, "correlations": "by_name"}))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--options", required=True, type=json.loads)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        combine_joint_cards(args.options, args.destination.resolve(), args.receipt)
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f"rdf-production joint fit: {exc}\n")


if __name__ == "__main__":
    main()
