#!/usr/bin/env python3
"""
fit_zpeak.py — Standalone workspace and datacard builder for the Z→μμ peak.

This script is a **convenience wrapper** around the generic
``AnalyticWorkspaceFitTask`` (law/combine_tasks.py).  It reads the same
workspace config (``zpeak_workspace.yaml``) that the LAW task uses, builds
per-channel RooWorkspaces and Combine datacards in one step, and prints the
exact ``law run AnalyticWorkspaceFitTask`` command to run the fits inside the
full LAW pipeline.

Use this script when:
  - You want to quickly inspect workspaces before running LAW
  - You are offline and cannot run the LAW scheduler
  - You need a reproducible one-shot script for paper figures

Use ``law run AnalyticWorkspaceFitTask`` instead when:
  - You want LAW dependency tracking, caching, and batch submission
  - You are running on a cluster via HTCondor

Fit model (from zpeak_workspace.yaml)
--------------------------------------
  Signal  : Voigtian(mass; mean, width, sigma)
              mean  — floating Z mass  (init: PDG 91.19 GeV)
              width — Z natural width, fixed to PDG (2.495 GeV)
              sigma — floating Gaussian detector resolution (~2 GeV)
  Background: Exponential(mass; decay)
              decay — floating slope (< 0)
  Yields  : nsig_<ch>, nbkg_<ch> freely floating (rate -1 in datacard)

Shared parameters
------------------
Parameters with ``shared: true`` in the config get a name without a channel
suffix (e.g. ``mean``, ``sigma``).  Combine ties these automatically across
channels in the simultaneous combined fit.  Background slope (``decay``) is
per-channel (``decay_0j``, etc.).

Usage
-----
  # Build workspaces and datacards, print LAW fit commands
  python analyses/CMS_Run2016_DoubleMuon/fit_zpeak.py

  # Explicit paths
  python analyses/CMS_Run2016_DoubleMuon/fit_zpeak.py \\
      --workspace-config analyses/CMS_Run2016_DoubleMuon/zpeak_workspace.yaml \\
      --input  output/dimuon_zpeak.root \\
      --outdir zpeak_fit

Running the fits via LAW
------------------------
After this script completes, run the full pipeline with the generic LAW task::

  source law/env.sh && law index

  law run AnalyticWorkspaceFitTask \\
      --name zpeak_run2016g \\
      --workspace-config analyses/CMS_Run2016_DoubleMuon/zpeak_workspace.yaml \\
      --histogram-file output/dimuon_zpeak.root \\
      --method FitDiagnostics \\
      --combine-options "--saveShapes --floatAllNuisances --saveNormalizations"

The task is generic — other analyses can provide a different workspace config
(e.g. ``jpsi_workspace.yaml``, ``upsilon_workspace.yaml``) without any code
changes.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Try to import the generic workspace helpers from law/combine_tasks.py.
# Fall back to the built-in implementation if the LAW path is not set up.
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
_LAW_DIR = os.path.join(_REPO_ROOT, "law")

_GENERIC_WORKSPACE = False
if _LAW_DIR not in sys.path:
    sys.path.insert(0, _LAW_DIR)

try:
    # Import ROOT-dependent helpers lazily (see _build_workspace below)
    import combine_tasks as _ct
    _GENERIC_WORKSPACE = True
except ImportError:
    pass

import ROOT  # type: ignore

ROOT.gROOT.SetBatch(True)
ROOT.RooMsgService.instance().setGlobalKillBelow(ROOT.RooFit.WARNING)

# Optional CombineHarvester — only used for the manual datacard writer
try:
    import CombineHarvester.CombineTools.ch as ch  # type: ignore
    HAS_CH = True
except ImportError:
    HAS_CH = False


# ---------------------------------------------------------------------------
# Default paths (relative to repo root)
# ---------------------------------------------------------------------------
DEFAULT_WS_CFG = os.path.join(_SCRIPT_DIR, "zpeak_workspace.yaml")
DEFAULT_INPUT  = os.path.join(_SCRIPT_DIR, "output", "dimuon_zpeak.root")
DEFAULT_OUTDIR = os.path.join(_SCRIPT_DIR, "zpeak_fit")


# ---------------------------------------------------------------------------
# Workspace building (delegates to combine_tasks helpers when available)
# ---------------------------------------------------------------------------

def build_workspace(ws_cfg: dict, hist: ROOT.TH1, channel: str) -> ROOT.RooWorkspace:
    """Build a RooWorkspace for one channel.

    Delegates to :func:`_build_analytic_workspace` in ``combine_tasks.py``
    when the LAW environment is available; otherwise falls back to the
    bundled implementation (supports Voigtian + Exponential only).
    """
    if _GENERIC_WORKSPACE:
        return _ct._build_analytic_workspace(ws_cfg, hist, channel)
    return _build_workspace_fallback(ws_cfg, hist, channel)


def _build_workspace_fallback(
    ws_cfg: dict, hist: ROOT.TH1, channel: str
) -> ROOT.RooWorkspace:
    """Voigtian + Exponential fallback workspace builder.

    Used when ``combine_tasks.py`` is not importable.  Parameters are read
    from the ``signal`` and ``background`` sections of ``ws_cfg`` using the
    same format as the generic helper.
    """
    obs_cfg = ws_cfg["observable"]
    obs_lo  = float(obs_cfg["lo"])
    obs_hi  = float(obs_cfg["hi"])
    n_obs   = hist.Integral()

    sig_p = ws_cfg["signal"]["parameters"]
    bkg_p = ws_cfg["background"]["parameters"]

    def _var(name, cfg, channel):
        shared  = cfg.get("shared", False)
        vname   = name if shared else f"{name}_{channel}"
        init    = float(cfg.get("init", 0.0))
        if cfg.get("fixed", False):
            v = ROOT.RooRealVar(vname, cfg.get("title", vname), init)
            v.setConstant(True)
        else:
            v = ROOT.RooRealVar(
                vname, cfg.get("title", vname), init,
                float(cfg.get("min", init - 10)), float(cfg.get("max", init + 10))
            )
        return v

    obs   = ROOT.RooRealVar(obs_cfg.get("name", "mass"), obs_cfg.get("title", "mass"), obs_lo, obs_hi)
    mean  = _var("mean",  sig_p.get("mean",  {"init": 91.1876}), channel)
    width = _var("width", sig_p.get("width", {"init": 2.4952, "fixed": True}), channel)
    sigma = _var("sigma", sig_p.get("sigma", {"init": 2.0, "min": 0.3, "max": 6.0}), channel)
    decay = _var("decay", bkg_p.get("decay", {"init": -0.05, "min": -0.5, "max": -1e-3}), channel)

    sig_pdf = ROOT.RooVoigtian(f"sig_{channel}", f"Signal ({channel})", obs, mean, width, sigma)
    bkg_pdf = ROOT.RooExponential(f"bkg_{channel}", f"Background ({channel})", obs, decay)

    nsig  = ROOT.RooRealVar(f"nsig_{channel}", "Signal yield", 0.85*n_obs, 0.0, 1.5*n_obs+10)
    nbkg  = ROOT.RooRealVar(f"nbkg_{channel}", "Background yield", 0.15*n_obs, 0.0, 0.5*n_obs+10)
    model = ROOT.RooAddPdf(f"model_{channel}", "Signal + Background",
                           ROOT.RooArgList(sig_pdf, bkg_pdf), ROOT.RooArgList(nsig, nbkg))
    dh    = ROOT.RooDataHist(f"data_obs_{channel}", "Observed data", ROOT.RooArgList(obs), hist)

    ws = ROOT.RooWorkspace(f"ws_{channel}")
    _imp = getattr(ws, "import")
    _imp(obs)
    for p in [mean, width, sigma, decay]:
        _imp(p)
    _imp(sig_pdf, ROOT.RooFit.RecycleConflictNodes())
    _imp(bkg_pdf, ROOT.RooFit.RecycleConflictNodes())
    _imp(nsig); _imp(nbkg)
    _imp(model, ROOT.RooFit.RecycleConflictNodes())
    _imp(dh)
    return ws


def write_datacard(channel: str, n_obs: float, ws_path: str, out_dir: str) -> str:
    """Write a Combine datacard.  Delegates to combine_tasks when available."""
    if _GENERIC_WORKSPACE:
        return _ct._write_analytic_datacard(channel, n_obs, ws_path, out_dir)
    return _write_datacard_manual(channel, n_obs, ws_path, out_dir)


def _write_datacard_manual(channel, n_obs, ws_path, out_dir):
    ws_name = f"ws_{channel}"
    lines = [
        f"# CMS Combine datacard — channel: {channel}",
        "imax 1\njmax 1\nkmax 0",
        "-" * 72,
        f"shapes sig       {channel}  {ws_path}  {ws_name}:sig_{channel}",
        f"shapes bkg       {channel}  {ws_path}  {ws_name}:bkg_{channel}",
        f"shapes data_obs  {channel}  {ws_path}  {ws_name}:data_obs_{channel}",
        "-" * 72,
        f"bin          {channel}",
        f"observation  {n_obs:.0f}",
        "-" * 72,
        f"bin          {channel}    {channel}",
        "process      sig          bkg",
        "process      0            1",
        "rate         -1           -1",
    ]
    card_path = os.path.join(out_dir, f"datacard_{channel}.txt")
    with open(card_path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return card_path


def write_combined_datacard(channels: list, out_dir: str) -> str:
    """Write the combined simultaneous-fit datacard."""
    if _GENERIC_WORKSPACE:
        return _ct._write_analytic_combined_datacard(channels, out_dir)
    n = len(channels)
    lines = [
        "imax " + str(n), "jmax 1", "kmax 0", "-" * 72,
    ]
    for ch in channels:
        ws_path = os.path.join(out_dir, f"ws_{ch}.root")
        lines += [
            f"shapes sig       {ch}  {ws_path}  ws_{ch}:sig_{ch}",
            f"shapes bkg       {ch}  {ws_path}  ws_{ch}:bkg_{ch}",
            f"shapes data_obs  {ch}  {ws_path}  ws_{ch}:data_obs_{ch}",
        ]
    lines += [
        "-" * 72,
        "bin " + " ".join(channels),
        "observation " + " ".join(["-1"] * n),
        "-" * 72,
        "bin " + " ".join(f"{c} {c}" for c in channels),
        "process " + " ".join("sig bkg" for _ in channels),
        "process " + " ".join("0 1" for _ in channels),
        "rate " + " ".join("-1 -1" for _ in channels),
    ]
    card_path = os.path.join(out_dir, "datacard_combined.txt")
    with open(card_path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return card_path


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--workspace-config", default=DEFAULT_WS_CFG,
        help=(
            "Path to workspace config YAML "
            f"(default: {os.path.basename(DEFAULT_WS_CFG)})"
        ),
    )
    parser.add_argument(
        "--input", default=DEFAULT_INPUT,
        help=f"Input ROOT file with analysis histograms (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--outdir", default=DEFAULT_OUTDIR,
        help=f"Output directory for workspaces and datacards (default: {DEFAULT_OUTDIR})",
    )
    args = parser.parse_args()

    # ---- Load workspace config -------------------------------------------
    ws_cfg_path = os.path.abspath(args.workspace_config)
    if not os.path.exists(ws_cfg_path):
        sys.exit(f"ERROR: workspace config not found: {ws_cfg_path}")
    with open(ws_cfg_path) as fh:
        ws_cfg = yaml.safe_load(fh)

    out = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)

    if _GENERIC_WORKSPACE:
        print(f"Using generic workspace helpers from law/combine_tasks.py")
    else:
        print(f"INFO: combine_tasks.py not importable; using built-in Voigtian+exp fallback")

    # ---- Open analysis ROOT file ----------------------------------------
    root_file = ROOT.TFile.Open(os.path.abspath(args.input), "READ")
    if not root_file or root_file.IsZombie():
        sys.exit(f"ERROR: cannot open {args.input}")

    active_channels: list[str] = []

    for ch_cfg in ws_cfg.get("channels", []):
        channel  = ch_cfg["name"]
        hist_key = ch_cfg["histogram"]
        label    = ch_cfg.get("label", channel)

        print(f"\n{'─' * 60}")
        print(f" Channel: {channel}  ({label})")
        print(f"{'─' * 60}")

        hist = root_file.Get(f"histograms/{hist_key}")
        if not hist or hist.IsZombie():
            print(f"  WARNING: histogram 'histograms/{hist_key}' not found — skipping.")
            continue

        hist.SetDirectory(0)
        n_obs = hist.Integral()
        print(f"  Events in fit range: {n_obs:.0f}")

        if n_obs < 10:
            print(f"  WARNING: too few events ({n_obs:.0f}) — skipping channel.")
            continue

        ws      = build_workspace(ws_cfg, hist, channel)
        ws_path = str(out / f"ws_{channel}.root")
        ws_file = ROOT.TFile(ws_path, "RECREATE")
        ws.Write()
        ws_file.Close()
        print(f"  Workspace : {ws_path}")

        card_path = write_datacard(channel, n_obs, ws_path, str(out))
        print(f"  Datacard  : {card_path}")
        active_channels.append(channel)

    root_file.Close()

    if not active_channels:
        sys.exit("ERROR: no channels with sufficient statistics.")

    combined_card = write_combined_datacard(active_channels, str(out))
    print(f"\nCombined datacard : {combined_card}")

    # ---- Print the LAW AnalyticWorkspaceFitTask command -----------------
    rel_ws_cfg = os.path.relpath(ws_cfg_path)
    rel_input  = os.path.relpath(os.path.abspath(args.input))
    print(f"\n{'═' * 68}")
    print(" Run the full fit pipeline using the generic LAW task:")
    print(f"{'═' * 68}\n")
    print("  source law/env.sh && law index\n")
    print(f"  law run AnalyticWorkspaceFitTask \\")
    print(f"      --name zpeak_run2016g \\")
    print(f"      --workspace-config {rel_ws_cfg} \\")
    print(f"      --histogram-file {rel_input} \\")
    print(f"      --method FitDiagnostics \\")
    print(f"      --combine-options \"--saveShapes --floatAllNuisances --saveNormalizations\"\n")
    print("  # Results: analyticFit_zpeak_run2016g/")
    print("  #   ws_0j.root, ws_1j.root, ws_2j.root, ws_ge3j.root")
    print("  #   datacard_{0j,1j,2j,ge3j,combined}.txt")
    print("  #   fitDiagnostics_*.root, combine_*.log, provenance.json\n")
    print("  # The same task works for any resonance — just provide a")
    print("  # different workspace config YAML (no code changes needed).")


if __name__ == "__main__":
    main()
