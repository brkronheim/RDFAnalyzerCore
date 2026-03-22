#ifndef WEIGHTMANAGER_H_INCLUDED
#define WEIGHTMANAGER_H_INCLUDED

#include <api/IPluggableManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <api/ISystematicManager.h>
#include <api/ManagerContext.h>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RResultPtr.hxx>
#include <string>
#include <utility>
#include <vector>
#include <memory>

class Analyzer;

/**
 * @brief Audit record for a single weight component.
 *
 * Populated after the RDataFrame computation completes (finalize()).
 */
struct WeightAuditEntry {
  std::string name;          ///< Human-readable label
  std::string column;        ///< Dataframe column that was audited
  double sumWeights = 0.0;   ///< Sum of per-event weight values
  double meanWeight = 0.0;   ///< Arithmetic mean
  double minWeight = 0.0;    ///< Minimum value seen
  double maxWeight = 0.0;    ///< Maximum value seen
  long long negativeCount = 0; ///< Events with weight < 0
  long long zeroCount = 0;     ///< Events with weight == 0
};

/**
 * @brief Descriptor for a single systematic weight variation.
 */
struct WeightVariation {
  std::string name;        ///< Variation label (e.g. "pileup")
  std::string upColumn;    ///< Dataframe column for the "up" shift
  std::string downColumn;  ///< Dataframe column for the "down" shift
};

/**
 * @class WeightManager
 * @brief Plugin that manages nominal and varied event weights.
 *
 * WeightManager supports:
 *  - **Scale factors**: named per-event multiplicative corrections already
 *    defined as dataframe columns.
 *  - **Normalization weights**: scalar factors applied uniformly to all events
 *    (e.g. lumi × cross-section / sum_weights).
 *  - **Systematic weight groups**: named up/down variations that substitute
 *    one component of the nominal weight.
 *  - **Weight auditing**: per-component statistics (sum, mean, min, max,
 *    negative-event count) written to the meta ROOT file and the logger.
 *
 * Typical usage:
 * @code
 *   auto* wm = analyzer->getPlugin<WeightManager>("weights");
 *
 *   // Register components.
 *   wm->addScaleFactor("pileup_sf",    "pu_weight");
 *   wm->addScaleFactor("btag_sf",      "btag_weight");
 *   wm->addNormalization("lumi_xsec",  0.0412);  // e.g. xsec*lumi/sumW
 *
 *   // Register systematic weight variations.
 *   wm->addWeightVariation("pileup", "pu_weight_up", "pu_weight_down");
 *
 *   // Define the nominal weight column on the dataframe.
 *   wm->defineNominalWeight("weight_nominal");
 *
 *   // Define varied-weight columns (for systematic histograms).
 *   wm->defineVariedWeight("pileup", "up",   "weight_pileup_up");
 *   wm->defineVariedWeight("pileup", "down", "weight_pileup_down");
 *
 *   // Retrieve column names for histogram filling.
 *   std::string nomCol = wm->getNominalWeightColumn(); // "weight_nominal"
 *   std::string upCol  = wm->getWeightColumn("pileup", "up"); // "weight_pileup_up"
 *
 *   // After analyzer->run(), inspect audit entries.
 *   for (auto& e : wm->getAuditEntries()) { ... }
 * @endcode
 */
class WeightManager : public IPluggableManager {
public:

  // -------------------------------------------------------------------------
  // Factory: create, register with an Analyzer, and return as shared_ptr.
  // -------------------------------------------------------------------------
  static std::shared_ptr<WeightManager> create(
      Analyzer& an, const std::string& role = "weightManager");

  WeightManager() = default;

  // -------------------------------------------------------------------------
  // Weight component registration
  // -------------------------------------------------------------------------

  /**
   * @brief Register a per-event scale factor column.
   *
   * The column must already be defined on the dataframe when
   * defineNominalWeight() is called.
   *
   * @param name   Human-readable label used in audit output.
   * @param column Dataframe column name (float or double).
   */
  void addScaleFactor(const std::string &name, const std::string &column);

  /**
   * @brief Register a scalar normalization factor applied to every event.
   *
   * @param name  Human-readable label used in audit output.
   * @param value Multiplicative scalar (e.g. lumi × cross-section / sumW).
   */
  void addNormalization(const std::string &name, double value);

  /**
   * @brief Register a systematic weight variation.
   *
   * The up/down columns must be defined on the dataframe before
   * defineVariedWeight() is called for this variation.
   *
   * @param name       Variation label (e.g. "pileup").
   * @param upColumn   Dataframe column for the "up" shift.
   * @param downColumn Dataframe column for the "down" shift.
   */
  void addWeightVariation(const std::string &name,
                          const std::string &upColumn,
                          const std::string &downColumn);

  // -------------------------------------------------------------------------
  // Weight column definition (deferred to execute())
  // -------------------------------------------------------------------------

  /**
   * @brief Schedule definition of the nominal weight column on the dataframe.
   *
   * The column is the product of all registered scale factor columns and
   * all registered scalar normalizations.  The column is actually defined
   * on the dataframe inside execute(), which is called by the framework
   * immediately before the computation is triggered.
   *
   * @param outputColumn Name of the column to define (default: "weight_nominal").
   */
  void defineNominalWeight(const std::string &outputColumn = "weight_nominal");

  /**
   * @brief Schedule definition of a varied weight column on the dataframe.
   *
   * The varied column replaces the scale factor for the named variation with
   * its up or down variant while keeping all other components at nominal.
   * The scalar normalizations are still applied.
   *
   * defineVariedWeight() may be called multiple times to register several
   * systematic columns; all are created in execute().
   *
   * @param variationName Name of the variation (must match a prior
   *                      addWeightVariation() call).
   * @param direction     "up" or "down".
   * @param outputColumn  Dataframe column name to define.
   */
  void defineVariedWeight(const std::string &variationName,
                           const std::string &direction,
                           const std::string &outputColumn);

  // -------------------------------------------------------------------------
  // Column name accessors (for histogram filling)
  // -------------------------------------------------------------------------

  /**
   * @brief Return the nominal weight column name.
   *
   * Returns an empty string if defineNominalWeight() has not been called.
   */
  const std::string &getNominalWeightColumn() const;

  /**
   * @brief Return a varied weight column name.
   *
   * @param variationName Variation label (as registered with addWeightVariation()).
   * @param direction     "up" or "down".
   * @return The column name registered via defineVariedWeight(), or an empty
   *         string if the combination was not registered.
   */
  std::string getWeightColumn(const std::string &variationName,
                               const std::string &direction) const;

  /**
   * @brief Return the total scalar normalization factor.
   *
   * This is the product of all values registered via addNormalization().
   */
  double getTotalNormalization() const;

  // -------------------------------------------------------------------------
  // Audit results (populated after finalize())
  // -------------------------------------------------------------------------

  /**
   * @brief Return per-component audit statistics.
   *
   * Entries are populated in finalize() after the event loop completes.
   * One entry is produced for every column audited (nominal weight column
   * plus each defined varied weight column).
   */
  const std::vector<WeightAuditEntry> &getAuditEntries() const;

  // -------------------------------------------------------------------------
  // IPluggableManager interface
  // -------------------------------------------------------------------------

  std::string type() const override { return "WeightManager"; }

  void setContext(ManagerContext &ctx) override;

  /** No-op: weight components are registered programmatically. */
  void setupFromConfigFile() override {}

  /**
   * @brief Define all weight columns on the dataframe and book audit actions.
   *
   * Called by the framework immediately before the RDataFrame computation is
   * triggered.
   */
  void execute() override;

  /**
   * @brief Retrieve audit results and write them to the meta output file.
   *
   * Called by the framework after the RDataFrame computation completes.
   */
  void finalize() override;

  /**
   * @brief Log weight audit summary via the analysis logger.
   *
   * Called by the framework after finalize().
   */
  void reportMetadata() override;

  /**
   * @brief Contribute structured provenance metadata for this plugin.
   *
   * Returns entries describing:
   *  - "scale_factors": comma-separated "name:column" pairs
   *  - "normalizations": comma-separated "name:value" pairs
   *  - "weight_variations": comma-separated variation names
   *  - "nominal_weight_column": the defined nominal weight column (if set)
   *
   * The Analyzer automatically computes "plugin.<role>.config_hash" from
   * these entries; plugins do not need to compute it themselves.
   */
  std::unordered_map<std::string, std::string>
  collectProvenanceEntries() const override;

private:
  // ---- Registered components ----------------------------------------------
  std::vector<std::pair<std::string, std::string>> scaleFactors_m; ///< name → column
  std::vector<std::pair<std::string, double>> normalizations_m;    ///< name → value
  std::vector<WeightVariation> variations_m;

  // ---- Pending column definitions (scheduled before execute()) ------------
  struct VariedColumnSpec {
    std::string variationName;
    std::string direction; ///< "up" or "down"
    std::string outputColumn;
  };

  std::string nominalOutputColumn_m;
  std::vector<VariedColumnSpec> variedColumnSpecs_m;

  // ---- Resolved column name map (variationName+dir → outputColumn) --------
  struct VariedColumnKey {
    std::string variationName;
    std::string direction;
  };
  // Stored as flat vector for simplicity
  std::vector<std::pair<VariedColumnKey, std::string>> variedColumns_m;

  // ---- Lazy audit RDF results (booked in execute(), read in finalize()) ---
  struct AuditPending {
    std::string name;
    std::string column;
    ROOT::RDF::RResultPtr<double> sumResult;
    ROOT::RDF::RResultPtr<double> meanResult;
    ROOT::RDF::RResultPtr<double> minResult;
    ROOT::RDF::RResultPtr<double> maxResult;
    ROOT::RDF::RResultPtr<ULong64_t> negCount;
    ROOT::RDF::RResultPtr<ULong64_t> zeroCount;
  };
  std::vector<AuditPending> auditPending_m;

  // ---- Final audit entries (populated in finalize()) ----------------------
  std::vector<WeightAuditEntry> auditEntries_m;

  // ---- Context ------------------------------------------------------------
  IConfigurationProvider *configManager_m = nullptr;
  IDataFrameProvider *dataManager_m = nullptr;
  ILogger *logger_m = nullptr;
  IOutputSink *metaSink_m = nullptr;

  // ---- Internal helpers ---------------------------------------------------

  /// Compute the total scalar normalization product.
  double computeNormProduct() const;

  /**
   * @brief Define a weight column as the product of the given SF columns
   *        times the scalar normalization product.
   */
  void defineWeightColumn(const std::string &outputColumn,
                          const std::vector<std::string> &sfColumns);

  /// Book lazy RDF actions to audit the named column.
  void bookAudit(const std::string &label, const std::string &column,
                 ROOT::RDF::RNode &df);
};



#endif // WEIGHTMANAGER_H_INCLUDED
