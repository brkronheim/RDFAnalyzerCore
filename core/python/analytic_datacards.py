"""Explicit process rates for analytic Combine cards with a common signal strength."""

import math
import re
from pathlib import Path


def configured_rates(config):
    """Read positive reference signal and background yields for every channel."""
    if "combine_rates" not in config:
        return None

    channels = [entry["name"] for entry in config["channels"]]
    if not all(isinstance(ch, str) and re.fullmatch(r"[A-Za-z0-9_]+", ch)
               and ch != "combined" for ch in channels):
        raise ValueError("Combine channel names must use letters, digits or underscores and avoid 'combined'")
    rates = config["combine_rates"]
    if not isinstance(rates, dict) or len(channels) != len(set(channels)) or set(rates) != set(channels):
        raise ValueError("combine_rates must specify exactly one entry for each configured channel")

    result = {}
    for channel in channels:
        entry = rates[channel]
        if not isinstance(entry, dict) or set(entry) != {"signal", "background"}:
            raise ValueError(f"combine_rates.{channel} requires signal and background yields")
        values = entry["signal"], entry["background"]
        if not all(type(value) in (int, float) and math.isfinite(value) and value > 0
                   for value in values):
            raise ValueError(f"combine_rates.{channel} requires finite positive yields")
        result[channel] = values
    return result


def write_rate_card(channels, rates, out_dir, *, tag):
    """Write a card that uses Combine's default signal POI and per-bin background scales."""
    out_dir = Path(out_dir)
    bins = [f"ch_{channel}" for channel in channels]
    lines = [f"imax {len(bins)}", "jmax 1", "kmax *", "------------"]
    for channel, card_bin in zip(channels, bins):
        for process in ("sig", "bkg", "data_obs"):
            lines.append(
                f"shapes {process} {card_bin} ws_{channel}.root "
                f"ws_{channel}:{process}_{channel}"
            )
    lines += [
        "------------",
        "bin " + " ".join(bins),
        "observation " + " ".join("-1" for _ in bins),
        "------------",
        "bin " + " ".join(f"{card_bin} {card_bin}" for card_bin in bins),
        "process " + " ".join("sig bkg" for _ in bins),
        "process " + " ".join("0 1" for _ in bins),
        "rate " + " ".join(f"{rates[channel][0]} {rates[channel][1]}" for channel in channels),
        "------------",
    ]
    for channel, card_bin in zip(channels, bins):
        lines.append(f"bkg_scale_{channel} rateParam {card_bin} bkg 1 [0,100]")
    path = out_dir / f"datacard_{tag}.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)
