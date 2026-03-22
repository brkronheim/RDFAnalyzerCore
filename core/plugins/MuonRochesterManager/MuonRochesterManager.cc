// All common implementation (applyCorrection, applyResolutionSmearing,
// propagateMET, PhysicsObjectCollection integration, execute(), etc.)
// lives in ObjectEnergyManagerBase.cc.  This translation unit provides
// the Rochester-specific extensions and emits the vtable for this class.

#include <MuonRochesterManager.h>
#include <analyzer.h>
#include <api/ILogger.h>
#include <sstream>
#include <stdexcept>

// ---------------------------------------------------------------------------
// Rochester-specific input columns
// ---------------------------------------------------------------------------

void MuonRochesterManager::setRochesterInputColumns(
    const std::string &chargeColumn, const std::string &genPtColumn,
    const std::string &nLayersColumn, const std::string &u1Column,
    const std::string &u2Column) {
  if (chargeColumn.empty())
    throw std::invalid_argument(
        "MuonRochesterManager::setRochesterInputColumns: "
        "chargeColumn must not be empty");
  chargeColumn_m  = chargeColumn;
  genPtColumn_m   = genPtColumn;
  nLayersColumn_m = nLayersColumn;
  u1Column_m      = u1Column;
  u2Column_m      = u2Column;
}

// ---------------------------------------------------------------------------
// Internal helper
// ---------------------------------------------------------------------------

std::vector<std::string>
MuonRochesterManager::buildRochesterInputColumns(
    const std::string &ptColumn) const {
  if (chargeColumn_m.empty())
    throw std::runtime_error(
        "MuonRochesterManager::buildRochesterInputColumns: "
        "call setRochesterInputColumns() before applying Rochester corrections");
  if (getEtaColumn().empty())
    throw std::runtime_error(
        "MuonRochesterManager::buildRochesterInputColumns: "
        "call setObjectColumns() before applying Rochester corrections");

  // Standard CMS Rochester correctionlib numeric input order:
  //   charge, eta, phi, pt, genPt, nTrackerLayers, u1, u2
  return {chargeColumn_m, getEtaColumn(), getPhiColumn(),
          ptColumn, genPtColumn_m, nLayersColumn_m, u1Column_m, u2Column_m};
}

// ---------------------------------------------------------------------------
// Convenience Rochester correction methods
// ---------------------------------------------------------------------------

void MuonRochesterManager::applyRochesterCorrection(
    CorrectionManager &cm, const std::string &correctionName,
    const std::string &inputPtColumn, const std::string &outputPtColumn) {
  applyCorrectionlib(cm, correctionName, {"nom"},
                     inputPtColumn, outputPtColumn,
                     /*applyToMass=*/false, "", "",
                     buildRochesterInputColumns(inputPtColumn));
}

void MuonRochesterManager::applyRochesterSystematicSet(
    CorrectionManager &cm, const std::string &correctionName,
    const std::string &setName, const std::string &inputPtColumn,
    const std::string &outputPtPrefix) {
  applySystematicSet(cm, correctionName, setName,
                     inputPtColumn, outputPtPrefix,
                     /*applyToMass=*/false,
                     buildRochesterInputColumns(inputPtColumn));
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

const std::string &MuonRochesterManager::getChargeColumn()  const { return chargeColumn_m;  }
const std::string &MuonRochesterManager::getGenPtColumn()   const { return genPtColumn_m;   }
const std::string &MuonRochesterManager::getNLayersColumn() const { return nLayersColumn_m; }
const std::string &MuonRochesterManager::getU1Column()      const { return u1Column_m;      }
const std::string &MuonRochesterManager::getU2Column()      const { return u2Column_m;      }

// ---------------------------------------------------------------------------
// Metadata extension hooks
// ---------------------------------------------------------------------------

void MuonRochesterManager::appendObjectMetadata(std::ostringstream &ss) const {
  if (!chargeColumn_m.empty())
    ss << "  Rochester input columns:"
       << " charge="  << chargeColumn_m
       << " genPt="   << genPtColumn_m
       << " nLayers=" << nLayersColumn_m
       << " u1="      << u1Column_m
       << " u2="      << u2Column_m << "\n";
}

void MuonRochesterManager::appendObjectProvenanceEntries(
    std::unordered_map<std::string, std::string> &entries) const {
  if (!chargeColumn_m.empty()) {
    entries["muon_charge_column"]  = chargeColumn_m;
    entries["muon_gen_pt_column"]  = genPtColumn_m;
    entries["muon_nlayers_column"] = nLayersColumn_m;
    entries["muon_u1_column"]      = u1Column_m;
    entries["muon_u2_column"]      = u2Column_m;
  }
}

std::shared_ptr<MuonRochesterManager> MuonRochesterManager::create(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<MuonRochesterManager>();
    an.addPlugin(role, plugin);
    return plugin;
}
