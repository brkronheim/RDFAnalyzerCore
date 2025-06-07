
include_directories(PUBLIC "${RDFAnalyzer_SOURCE_DIR}/extern/XGBoost-FastForest/include" "${RDFAnalyzer_SOURCE_DIR}/extern/pythia8/include" "${RDFAnalyzer_SOURCE_DIR}/interface")

add_executable(makePlots makePlots.cc)
target_compile_features(makePlots PUBLIC cxx_std_17)
target_include_directories(makePlots PUBLIC "${RDFAnalyzer_SOURCE_DIR}/interface"  "${RDFAnalyzer_SOURCE_DIR}/extern/pythia8/include" "${NegativeReweight_SOURCE_DIR}/extern/XGBoost-FastForest/include")
target_link_libraries(makePlots PUBLIC correctionlib corrections stitching vh util plots selections functions boosted analyzer systematics fastforest ROOT::ROOTDataFrame ROOT::ROOTVecOps ROOT::Core ROOT::Hist ROOT::MathCore
 	Analysis Event Basics FJcore ParticleData StandardModel ResonanceWidths  SusyResonanceWidths ResonanceWidthsDM Settings Plugins PythiaStdlib Logger
	SusyCouplings SusyWidthFunctions MathTools)

add_executable(skim skim.cc)
target_compile_features(skim PRIVATE cxx_std_17)
target_include_directories(skim PUBLIC "${RDFAnalyzer_SOURCE_DIR}/interface" "${RDFAnalyzer_SOURCE_DIR}/extern/pythia8/include" "${NegativeReweight_SOURCE_DIR}/extern/XGBoost-FastForest/include")
target_link_libraries(skim PRIVATE correctionlib corrections vh util trigger stitching plots selections functions vjets analyzer systematics gen fastforest xgboostEvaluator ROOT::ROOTDataFrame ROOT::ROOTVecOps ROOT::Core ROOT::Hist ROOT::MathCore
	Analysis Event Basics FJcore ParticleData StandardModel ResonanceWidths  SusyResonanceWidths ResonanceWidthsDM Settings Plugins PythiaStdlib Logger
	SusyCouplings SusyWidthFunctions MathTools)
