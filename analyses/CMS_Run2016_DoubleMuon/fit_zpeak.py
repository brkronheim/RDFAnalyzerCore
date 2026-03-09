#!/usr/bin/env python3
"""
fit_zpeak.py — Build RooWorkspaces and CMS Combine datacards for the
               dimuon Z peak fit in bins of jet multiplicity.

This script handles the workspace and datacard creation step only.
The actual statistical fit is delegated to the existing ``law run RunCombine``
task (law/combine_tasks.py), which manages Combine execution, logging, and
output organisation.

Fit model (per jet-bin channel)
--------------------------------
  Signal (Z peak)  : Voigtian(mass; μ_Z, Γ_Z, σ)
                       μ_Z  – floating Z mass  (init: 91.19 GeV)
                       Γ_Z  – Z natural width, fixed to PDG (2.495 GeV)
                       σ    – floating Gaussian resolution (~2 GeV)
  Background       : Exponential(mass; τ)
                       τ    – floating slope (< 0)
  Yields           : nsig, nbkg freely floating (rate -1 in datacard)

Jet categories
--------------
  0j    nGoodJets == 0
  1j    nGoodJets == 1
  2j    nGoodJets == 2
  ge3j  nGoodJets >= 3

Prerequisites
-------------
  1. Run the C++ analysis to produce output/dimuon_zpeak.root::

       cd build/analyses/CMS_Run2016_DoubleMuon
       ./analysis ../../../analyses/CMS_Run2016_DoubleMuon/cfg.yaml

  2. Build CMS Combine (once)::

       cmake -S . -B build -DBUILD_COMBINE=ON
       cmake --build build -j$(nproc)

  3. (Recommended) Build CombineHarvester for structured datacard writing::

       cmake -S . -B build -DBUILD_COMBINE=ON -DBUILD_COMBINE_HARVESTER=ON
       cmake --build build -j$(nproc)

Usage
-----
::

  # Build workspaces and datacards, then print LAW fit commands
  python analyses/CMS_Run2016_DoubleMuon/fit_zpeak.py

  # Explicit paths
  python analyses/CMS_Run2016_DoubleMuon/fit_zpeak.py \\
      --input  output/dimuon_zpeak.root \\
      --outdir zpeak_fit

Running the fits
----------------
After this script completes, run the fits using the existing RunCombine
LAW task (law/combine_tasks.py)::

  source law/env.sh && law index

  # Per-channel fit
  law run RunCombine \\
      --datacard-config analyses/CMS_Run2016_DoubleMuon/cfg.yaml \\
      --name zpeak_0j \\
      --datacard-path zpeak_fit/datacard_0j.txt \\
      --method FitDiagnostics \\
      --combine-options "--saveShapes --floatAllNuisances --saveNormalizations"

  # Simultaneous fit across all jet bins
  law run RunCombine \\
      --datacard-config analyses/CMS_Run2016_DoubleMuon/cfg.yaml \\
      --name zpeak_combined \\
      --datacard-path zpeak_fit/datacard_combined.txt \\
      --method FitDiagnostics \\
      --combine-options "--saveShapes --floatAllNuisances --saveNormalizations"

Output
------
::

  zpeak_fit/
    ws_0j.root / ws_1j.root / ws_2j.root / ws_ge3j.root
    datacard_0j.txt / datacard_1j.txt / datacard_2j.txt / datacard_ge3j.txt
    datacard_combined.txt
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import ROOT

ROOT.gROOT.SetBatch(True)
ROOT.RooMsgService.instance().setGlobalKillBelow(ROOT.RooFit.WARNING)

# ---------------------------------------------------------------------------
# CombineHarvester (optional — richer datacard structure when available)
# ---------------------------------------------------------------------------
try:
    import CombineHarvester.CombineTools.ch as ch  # type: ignore
    HAS_CH = True
except ImportError:
    HAS_CH = False
    print(
        "INFO: CombineHarvester Python bindings not found.  "
        "Datacards will be written manually.\n"
        "      Build with: cmake -DBUILD_COMBINE_HARVESTER=ON && "
        "cmake --build build"
    )

# ---------------------------------------------------------------------------
# Analysis constants
# ---------------------------------------------------------------------------
PDG_MZ   = 91.1876   # GeV  (PDG 2022)
PDG_WZ   = 2.4952    # GeV  (PDG 2022)
MASS_LO  = 70.0
MASS_HI  = 110.0

#: (channel_name, histogram_key, human_label)
JET_BINS: list[tuple[str, str, str]] = [
    ("0j",   "DimuonMass_0j",   "0 jets"),
    ("1j",   "DimuonMass_1j",   "1 jet"),
    ("2j",   "DimuonMass_2j",   "2 jets"),
    ("ge3j", "DimuonMass_ge3j", "#geq3 jets"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_histogram(root_file: ROOT.TFile, hist_name: str) -> ROOT.TH1:
    """Load a TH1 from the histograms/ directory; detach from file."""
    h = root_file.Get(f"histograms/{hist_name}")
    if not h or h.IsZombie():
        raise RuntimeError(
            f"Histogram 'histograms/{hist_name}' not found in "
            f"{root_file.GetName()}.  "
            "Did the analysis run with histogramConfig: histograms.yaml?"
        )
    h.SetDirectory(0)
    h.SetName(hist_name)
    return h


# ---------------------------------------------------------------------------
# RooWorkspace builder
# ---------------------------------------------------------------------------

def build_workspace(channel: str, hist: ROOT.TH1) -> ROOT.RooWorkspace:
    """
    Build a RooWorkspace for one jet-bin channel containing:

    - ``mass``                  observable [MASS_LO, MASS_HI] GeV
    - ``mZ_{channel}``          floating Z mass
    - ``wZ_{channel}``          Z natural width (fixed to PDG)
    - ``sigZ_{channel}``        Gaussian detector resolution (floating)
    - ``tau_{channel}``         exponential slope (floating, < 0)
    - ``zpeak_{channel}``       RooVoigtian PDF for the Z peak
    - ``bkg_{channel}``         RooExponential PDF for background
    - ``nsig_{channel}``        Z signal yield (floating)
    - ``nbkg_{channel}``        background yield (floating)
    - ``model_{channel}``       extended sum: zpeak + bkg
    - ``data_obs_{channel}``    RooDataHist from the analysis histogram
    """
    ws = ROOT.RooWorkspace(f"ws_{channel}")
    n_obs = hist.Integral()

    # Observable
    mass = ROOT.RooRealVar("mass", "m_{#mu#mu} [GeV]", MASS_LO, MASS_HI)

    # Signal: Voigtian = Breit-Wigner (Z natural width) ⊗ Gaussian (resolution)
    mZ   = ROOT.RooRealVar(f"mZ_{channel}",   "Z mean [GeV]",
                           PDG_MZ, PDG_MZ - 3.0, PDG_MZ + 3.0)
    wZ   = ROOT.RooRealVar(f"wZ_{channel}",   "Z width [GeV]", PDG_WZ)
    wZ.setConstant(True)  # fixed to PDG; release to study width
    sigZ = ROOT.RooRealVar(f"sigZ_{channel}", "Gaussian #sigma [GeV]",
                           2.0, 0.3, 6.0)
    zpeak = ROOT.RooVoigtian(
        f"zpeak_{channel}", f"Voigtian Z peak ({channel})",
        mass, mZ, wZ, sigZ)

    # Background: falling exponential
    tau = ROOT.RooRealVar(f"tau_{channel}", "Exponential slope",
                          -0.05, -0.5, -1e-4)
    bkg = ROOT.RooExponential(
        f"bkg_{channel}", f"Exponential background ({channel})",
        mass, tau)

    # Extended model
    nsig  = ROOT.RooRealVar(f"nsig_{channel}", "Z signal yield",
                            0.85 * n_obs, 0.0, 1.5 * n_obs + 10.0)
    nbkg  = ROOT.RooRealVar(f"nbkg_{channel}", "Background yield",
                            0.15 * n_obs, 0.0, 0.5 * n_obs + 10.0)
    model = ROOT.RooAddPdf(
        f"model_{channel}", "Voigtian + Exponential",
        ROOT.RooArgList(zpeak, bkg),
        ROOT.RooArgList(nsig, nbkg))

    # Data as RooDataHist (Combine-standard name: data_obs_{channel})
    dh = ROOT.RooDataHist(
        f"data_obs_{channel}", "Observed data",
        ROOT.RooArgList(mass), hist)

    # Import everything into the workspace
    _imp = getattr(ws, "import")
    _imp(mass)
    _imp(mZ);  _imp(wZ);  _imp(sigZ)
    _imp(tau)
    _imp(zpeak, ROOT.RooFit.RecycleConflictNodes())
    _imp(bkg,   ROOT.RooFit.RecycleConflictNodes())
    _imp(nsig); _imp(nbkg)
    _imp(model, ROOT.RooFit.RecycleConflictNodes())
    _imp(dh)

    return ws


# ---------------------------------------------------------------------------
# Datacard writing via CombineHarvester
# ---------------------------------------------------------------------------

def write_datacard_ch(channel: str,
                      n_obs: float,
                      ws_path: str,
                      out_dir: str) -> str:
    """
    Write a per-channel CMS Combine datacard using CombineHarvester.

    CombineHarvester generates the standard imax/jmax/kmax header and the
    bin/observation/rate table.  The shapes section is then patched to point
    to the RooWorkspace containing the analytic PDFs, because CombineHarvester
    is designed for histogram-based shapes while our model uses analytic PDFs.
    """
    cb = ch.CombineHarvester()
    cb.SetFlag("allow-missing-shapes", True)

    cats = [(0, channel)]
    cb.AddObservations(["*"], ["zpeak"], ["Run2016G"], cats)
    cb.AddProcesses(["*"], ["zpeak"], ["Run2016G"], cats,
                    ["zpeak"], [True], True)
    cb.AddProcesses(["*"], ["zpeak"], ["Run2016G"], cats,
                    ["bkg"], [False], False)

    # Set approximate rates from histogram integral
    cb.cp().bin([channel]).process(["zpeak"]).ForEachProc(
        lambda p: p.set_rate(0.85 * n_obs))
    cb.cp().bin([channel]).process(["bkg"]).ForEachProc(
        lambda p: p.set_rate(0.15 * n_obs))
    cb.cp().bin([channel]).ForEachObs(
        lambda o: o.set_rate(n_obs))

    # Write the structural part of the datacard via CombineHarvester
    tmp_card = os.path.join(out_dir, f"_ch_{channel}.txt")
    writer = ch.CardWriter(tmp_card, os.path.join(out_dir, f"_ch_{channel}.root"))
    writer.SetVerbosity(0)
    writer.WriteCards(out_dir, cb.cp().bin([channel]))

    # Read back and patch the shapes section to reference the RooWorkspace
    card_path = os.path.join(out_dir, f"datacard_{channel}.txt")
    _patch_shapes_to_workspace(tmp_card, card_path, channel, ws_path)

    # Clean up temporary CombineHarvester output
    for tmp in [tmp_card, os.path.join(out_dir, f"_ch_{channel}.root")]:
        if os.path.exists(tmp):
            os.remove(tmp)

    return card_path


def _patch_shapes_to_workspace(src: str, dst: str,
                                channel: str, ws_path: str) -> None:
    """Replace CombineHarvester histogram shape lines with workspace shapes."""
    ws_name = f"ws_{channel}"
    shape_lines = [
        f"shapes zpeak    {channel}  {ws_path}  {ws_name}:zpeak_{channel}",
        f"shapes bkg      {channel}  {ws_path}  {ws_name}:bkg_{channel}",
        f"shapes data_obs {channel}  {ws_path}  {ws_name}:data_obs_{channel}",
    ]

    with open(src) as fh:
        lines = fh.readlines()

    out_lines: list[str] = []
    shapes_written = False
    for line in lines:
        if line.strip().startswith("shapes") and not shapes_written:
            out_lines.extend(s + "\n" for s in shape_lines)
            shapes_written = True
        elif line.strip().startswith("shapes"):
            pass  # skip remaining original shapes lines
        else:
            out_lines.append(line)

    if not shapes_written:
        # Insert after kmax line if no shapes section found
        final: list[str] = []
        for line in out_lines:
            final.append(line)
            if line.strip().startswith("kmax"):
                final.extend(s + "\n" for s in shape_lines)
        out_lines = final

    with open(dst, "w") as fh:
        fh.writelines(out_lines)


def write_datacard_manual(channel: str,
                          n_obs: float,
                          ws_path: str,
                          out_dir: str) -> str:
    """
    Write a CMS Combine datacard with analytic shapes from a RooWorkspace.
    Used as a fallback when CombineHarvester is not available.

    'rate -1' instructs Combine to read normalisation from the workspace
    extended PDF parameters (nsig_{channel}, nbkg_{channel}).
    """
    lines = [
        f"# CMS Combine datacard — Z→μμ peak fit, channel: {channel}",
        "# Generated by analyses/CMS_Run2016_DoubleMuon/fit_zpeak.py",
        "imax 1   # 1 channel",
        "jmax 1   # 1 background process",
        "kmax 0   # no nuisance parameters (all shape params float freely)",
        "----------------------------------------------------------------------------",
        f"shapes zpeak    {channel}  {ws_path}  ws_{channel}:zpeak_{channel}",
        f"shapes bkg      {channel}  {ws_path}  ws_{channel}:bkg_{channel}",
        f"shapes data_obs {channel}  {ws_path}  ws_{channel}:data_obs_{channel}",
        "----------------------------------------------------------------------------",
        f"bin          {channel}",
        f"observation  {n_obs:.0f}",
        "----------------------------------------------------------------------------",
        f"bin          {channel}    {channel}",
        "process      zpeak        bkg",
        "process      0            1",
        "# rate -1 means Combine reads yields from workspace PDFs",
        "rate         -1           -1",
        "----------------------------------------------------------------------------",
        "# Shape parameters (mZ, sigZ, tau) float freely in the workspace fit.",
    ]
    card_path = os.path.join(out_dir, f"datacard_{channel}.txt")
    with open(card_path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return card_path


# ---------------------------------------------------------------------------
# Combined datacard (simultaneous fit across all jet bins)
# ---------------------------------------------------------------------------

def write_combined_datacard(channels: list[str], out_dir: str) -> str:
    """
    Write a combined datacard for a simultaneous fit across all jet bins.

    Combine automatically correlates parameters with the same name across
    channels — so mZ and sigZ are shared while tau is per-channel.
    """
    n = len(channels)
    lines = [
        "# CMS Combine combined datacard — simultaneous Z peak fit",
        "# Channels: " + ", ".join(channels),
        f"imax {n}   # {n} jet-multiplicity channels",
        "jmax 1    # 1 background process",
        "kmax 0    # no nuisance parameters",
        "----------------------------------------------------------------------------",
    ]
    for ch_name in channels:
        ws_path = os.path.join(out_dir, f"ws_{ch_name}.root")
        ws_name = f"ws_{ch_name}"
        lines += [
            f"shapes zpeak    {ch_name}  {ws_path}  {ws_name}:zpeak_{ch_name}",
            f"shapes bkg      {ch_name}  {ws_path}  {ws_name}:bkg_{ch_name}",
            f"shapes data_obs {ch_name}  {ws_path}  {ws_name}:data_obs_{ch_name}",
        ]
    lines += [
        "----------------------------------------------------------------------------",
        "bin          " + "    ".join(channels),
        "observation  " + "    ".join(["-1"] * n),
        "----------------------------------------------------------------------------",
        "bin     " + "    ".join(f"{c}    {c}" for c in channels),
        "process " + "    ".join("zpeak    bkg" for _ in channels),
        "process " + "    ".join("0        1"   for _ in channels),
        "rate    " + "    ".join("-1       -1"  for _ in channels),
        "----------------------------------------------------------------------------",
        "# mZ and sigZ share the same RooRealVar name across channels →",
        "# Combine treats them as correlated (one shared Z mass / resolution).",
        "# tau is per-channel (independent background shapes per jet bin).",
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
        "--input", default="output/dimuon_zpeak.root",
        help="Input ROOT file with analysis histograms "
             "(default: output/dimuon_zpeak.root)",
    )
    parser.add_argument(
        "--outdir", default="zpeak_fit",
        help="Output directory for workspaces and datacards "
             "(default: zpeak_fit)",
    )
    args = parser.parse_args()

    out = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)

    # ---- Open analysis output file -----------------------------------------
    print(f"\nOpening: {args.input}")
    root_file = ROOT.TFile.Open(args.input, "READ")
    if not root_file or root_file.IsZombie():
        sys.exit(f"ERROR: cannot open {args.input}")

    active_channels: list[str] = []

    for channel, hist_name, label in JET_BINS:
        print(f"\n{'─' * 60}")
        print(f" Channel: {channel}  ({label})")
        print(f"{'─' * 60}")

        hist   = load_histogram(root_file, hist_name)
        n_obs  = hist.Integral()
        print(f"  Events in Z window [70, 110] GeV : {n_obs:.0f}")

        if n_obs < 10:
            print(f"  WARNING: too few events ({n_obs:.0f}) — skipping channel.")
            continue

        # Build and save RooWorkspace
        ws       = build_workspace(channel, hist)
        ws_path  = str(out / f"ws_{channel}.root")
        ws_file  = ROOT.TFile(ws_path, "RECREATE")
        ws.Write()
        ws_file.Close()
        print(f"  Workspace  : {ws_path}")

        # Write datacard
        if HAS_CH:
            card_path = write_datacard_ch(channel, n_obs, ws_path, str(out))
        else:
            card_path = write_datacard_manual(channel, n_obs, ws_path, str(out))
        print(f"  Datacard   : {card_path}")

        active_channels.append(channel)

    root_file.Close()

    if not active_channels:
        sys.exit("ERROR: no channels with sufficient statistics.")

    # ---- Combined datacard -------------------------------------------------
    combined_card = write_combined_datacard(active_channels, str(out))
    print(f"\nCombined datacard : {combined_card}")

    # ---- Print LAW RunCombine commands (reuse existing task) ---------------
    cfg_yaml = "analyses/CMS_Run2016_DoubleMuon/cfg.yaml"
    print(f"\n{'═' * 68}")
    print(" Run fits using the existing RunCombine LAW task:")
    print(f"{'═' * 68}")
    print()
    print("  source law/env.sh && law index")
    print()
    for ch_name in active_channels:
        card = str(out / f"datacard_{ch_name}.txt")
        print(f"  law run RunCombine \\")
        print(f"      --datacard-config {cfg_yaml} \\")
        print(f"      --name zpeak_{ch_name} \\")
        print(f"      --datacard-path {card} \\")
        print(f"      --method FitDiagnostics \\")
        print(f"      --combine-options \"--saveShapes --floatAllNuisances\"")
        print()

    # Simultaneous (combined) fit
    print(f"  # Simultaneous fit across all jet bins:")
    print(f"  law run RunCombine \\")
    print(f"      --datacard-config {cfg_yaml} \\")
    print(f"      --name zpeak_combined \\")
    print(f"      --datacard-path {combined_card} \\")
    print(f"      --method FitDiagnostics \\")
    print(f"      --combine-options \"--saveShapes --floatAllNuisances\"")
    print()
    print(f"  # Results land in combineRun_<name>/combine_results/")
    print(f"  # Fit ROOT file: fitDiagnostics_<channel>.root")


if __name__ == "__main__":
    main()
