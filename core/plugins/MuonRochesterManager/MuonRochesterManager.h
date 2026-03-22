#ifndef MUONROCHESTERMANAGER_H_INCLUDED
#define MUONROCHESTERMANAGER_H_INCLUDED

#include <ObjectEnergyManagerBase.h>

/**
 * @class MuonRochesterManager
 * @brief Plugin for applying CMS muon Rochester momentum corrections.
 *
 * Extends ObjectEnergyManagerBase with a dedicated interface for the CMS
 * Rochester correction, which requires additional per-muon input columns
 * (charge, genPt, nTrackerLayers, u1, u2) beyond the standard kinematic
 * columns set via setObjectColumns().
 *
 * @see ObjectEnergyManagerBase for the full shared API (setObjectColumns,
 *      setMETColumns, applyCorrectionlib, applyResolutionSmearing,
 *      propagateMET, PhysicsObjectCollection integration, etc.).
 * @see docs/MUON_ROCHESTER_CORRECTIONS.md for the full user-facing reference.
 *
 * ## CMS Rochester correctionlib input column order
 * @code
 *   [charge, eta, phi, pt, genPt, nTrackerLayers, u1, u2]
 * @endcode
 *
 * ## Typical CMS NanoAOD usage
 * @code
 *   auto* roc = analyzer->getPlugin<MuonRochesterManager>("roc");
 *   auto* cm  = analyzer->getPlugin<CorrectionManager>("corrections");
 *
 *   // 1. Declare muon and MET column names.
 *   roc->setObjectColumns("Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass");
 *   roc->setMETColumns("MET_pt", "MET_phi");
 *
 *   // 2. Declare the Rochester-specific input columns.
 *   roc->setRochesterInputColumns(
 *       "Muon_charge",          // Int_t cast to Float_t via Define
 *       "Muon_genPt",           // 0 for data
 *       "Muon_nTrackerLayers",  // Int_t cast to Float_t
 *       "Muon_u1",              // Gaussian random (0 for data)
 *       "Muon_u2");             // Gaussian random (0 for data)
 *
 *   // 3. Apply nominal Rochester correction.
 *   roc->applyRochesterCorrection(*cm, "rochester",
 *                                  "Muon_pt", "Muon_pt_roc");
 *
 *   // 4. Propagate to MET.
 *   roc->propagateMET("MET_pt", "MET_phi",
 *                     "Muon_pt", "Muon_pt_roc",
 *                     "MET_pt_roc", "MET_phi_roc");
 *
 *   // 5. Apply Rochester systematics (stat + syst).
 *   roc->registerSystematicSources("rochester", {"stat", "syst"});
 *   roc->applyRochesterSystematicSet(*cm, "rochester", "rochester",
 *                                    "Muon_pt", "Muon_pt_roc");
 *
 *   // 6. PhysicsObjectCollection integration.
 *   roc->setInputCollection("goodMuons");
 *   roc->defineCollectionOutput("Muon_pt_roc", "goodMuons_roc");
 *   roc->defineVariationCollections("goodMuons_roc", "goodMuons",
 *                                   "goodMuons_variations");
 * @endcode
 */
class MuonRochesterManager : public ObjectEnergyManagerBase {
public:
  std::string type() const override { return "MuonRochesterManager"; }

  // -------------------------------------------------------------------------
  // Rochester-specific input columns
  // -------------------------------------------------------------------------

  /**
   * @brief Declare the additional per-muon input columns required by the
   *        CMS Rochester correctionlib correction.
   *
   * The Rochester correctionlib evaluator expects numeric inputs in this
   * order:
   * @code
   *   charge, eta, phi, pt, genPt, nTrackerLayers, u1, u2
   * @endcode
   *
   * @c eta, @c phi, and @c pt are supplied via setObjectColumns(); the
   * remaining five are declared here.
   *
   * For data events set @p genPtColumn, @p u1Column, @p u2Column to
   * pre-defined constant-zero columns (e.g.
   * @c df.Define("zero_vec", [](...){return RVec<float>(n,0.f);})).
   *
   * @param chargeColumn   Per-muon charge column (Float_t; cast from Int_t if
   *                       needed via a RDF Define).
   * @param genPtColumn    Per-muon generator-level pT column (Float_t;
   *                       0 for data or unmatched muons).
   * @param nLayersColumn  Per-muon number-of-tracker-layers column (Float_t).
   * @param u1Column       First Gaussian random number per muon (Float_t;
   *                       0 for data).
   * @param u2Column       Second Gaussian random number per muon (Float_t;
   *                       0 for data).
   *
   * @throws std::invalid_argument if @p chargeColumn is empty.
   */
  void setRochesterInputColumns(const std::string &chargeColumn,
                                const std::string &genPtColumn,
                                const std::string &nLayersColumn,
                                const std::string &u1Column,
                                const std::string &u2Column);

  // -------------------------------------------------------------------------
  // Convenience Rochester correction methods
  // -------------------------------------------------------------------------

  /**
   * @brief Apply the nominal Rochester momentum correction via correctionlib.
   *
   * Evaluates @p correctionName with string args @c {"nom"} and the standard
   * Rochester numeric input column order:
   * @code
   *   [charge, eta, phi, inputPtColumn, genPt, nLayers, u1, u2]
   * @endcode
   * then schedules @c outputPtColumn = inputPtColumn × SF.
   *
   * This is a convenience wrapper around applyCorrectionlib().
   *
   * @throws std::runtime_error if setRochesterInputColumns() or
   *         setObjectColumns() (for eta/phi) was not called.
   */
  void applyRochesterCorrection(CorrectionManager &cm,
                                const std::string &correctionName,
                                const std::string &inputPtColumn,
                                const std::string &outputPtColumn);

  /**
   * @brief Apply all sources in a registered systematic set using the
   *        Rochester correctionlib input column ordering.
   *
   * For each source S in the registered set @p setName:
   *  1. Evaluates correctionlib with args @c {S, "up"}   → outputPtPrefix_S_up.
   *  2. Evaluates correctionlib with args @c {S, "down"} → outputPtPrefix_S_down.
   *  3. Calls addVariation(S, up, down).
   *
   * Typical CMS usage:
   * @code
   *   roc->registerSystematicSources("rochester", {"stat", "syst"});
   *   roc->applyRochesterSystematicSet(*cm, "rochester", "rochester",
   *                                    "Muon_pt", "Muon_pt_roc");
   *   // Produces: Muon_pt_roc_stat_up, Muon_pt_roc_stat_down,
   *   //           Muon_pt_roc_syst_up, Muon_pt_roc_syst_down
   * @endcode
   *
   * @throws std::runtime_error if setRochesterInputColumns() or
   *         setObjectColumns() was not called.
   * @throws std::runtime_error if @p setName is not registered.
   */
  void applyRochesterSystematicSet(CorrectionManager &cm,
                                   const std::string &correctionName,
                                   const std::string &setName,
                                   const std::string &inputPtColumn,
                                   const std::string &outputPtPrefix);

  // -------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------

  /// Charge column (from setRochesterInputColumns; empty if not set).
  const std::string &getChargeColumn() const;

  /// Generator-level pT column (from setRochesterInputColumns).
  const std::string &getGenPtColumn() const;

  /// Number-of-tracker-layers column (from setRochesterInputColumns).
  const std::string &getNLayersColumn() const;

  /// First Gaussian random number column (from setRochesterInputColumns).
  const std::string &getU1Column() const;

  /// Second Gaussian random number column (from setRochesterInputColumns).
  const std::string &getU2Column() const;

protected:
  std::string objectName() const override { return "Muon"; }

  /// Append Rochester-specific column names to the metadata log.
  void appendObjectMetadata(std::ostringstream &ss) const override;

  /// Append Rochester-specific entries to the provenance map.
  void appendObjectProvenanceEntries(
      std::unordered_map<std::string, std::string> &entries) const override;

private:
  /**
   * @brief Assemble the Rochester correctionlib numeric input column list
   *        in the standard CMS ordering:
   *        @c [charge, eta, phi, ptColumn, genPt, nLayers, u1, u2].
   *
   * @throws std::runtime_error if setRochesterInputColumns() or
   *         setObjectColumns() was not called.
   */
  std::vector<std::string>
  buildRochesterInputColumns(const std::string &ptColumn) const;

  std::string chargeColumn_m;
  std::string genPtColumn_m;
  std::string nLayersColumn_m;
  std::string u1Column_m;
  std::string u2Column_m;
};


// ---------------------------------------------------------------------------
// Helper: create, register with analyzer, and return as shared_ptr
// ---------------------------------------------------------------------------
#include <memory>
class Analyzer;
std::shared_ptr<MuonRochesterManager> makeMuonRochesterManager(
    Analyzer& an, const std::string& role = "muonRochesterManager");

#endif // MUONROCHESTERMANAGER_H_INCLUDED
