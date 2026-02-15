#!/bin/bash
# complete_analysis_workflow.sh
#
# Example script demonstrating a complete RDFAnalyzerCore workflow:
# 1. Run analysis to produce histograms
# 2. Generate CMS Combine datacards
# 3. Perform statistical analysis with Combine
#
# This script is a template - modify paths and configurations as needed

set -e  # Exit on any error

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== RDFAnalyzerCore Complete Analysis Workflow ===${NC}"
echo ""

# Check if Combine is built
if [ ! -f "build/external/HiggsAnalysis/CombinedLimit/exe/combine" ]; then
    echo -e "${RED}Error: CMS Combine not found!${NC}"
    echo "Please rebuild with: cmake -S . -B build -DBUILD_COMBINE=ON"
    exit 1
fi

# Configuration (modify these for your analysis)
ANALYSIS_EXE="./build/analyses/ExampleAnalysis/example"
OUTPUT_DIR="analysis_output"
DATACARD_CONFIG="example_datacard_config.yaml"

# Create output directory
mkdir -p $OUTPUT_DIR

echo -e "${YELLOW}Step 1: Running RDFAnalyzer analysis...${NC}"
echo "  Processing samples to produce histograms..."

# Example: Run analysis on different samples
# Replace these with your actual analysis configurations
if [ -f "$ANALYSIS_EXE" ]; then
    # Uncomment and modify for real analysis:
    # $ANALYSIS_EXE config_data.txt
    # $ANALYSIS_EXE config_signal.txt
    # $ANALYSIS_EXE config_background.txt
    echo "  [Skipping - modify script with your analysis executable and configs]"
else
    echo -e "${RED}  Warning: Analysis executable not found at $ANALYSIS_EXE${NC}"
    echo "  [Continuing with example...]"
fi

echo ""
echo -e "${YELLOW}Step 2: Creating CMS Combine datacards...${NC}"
echo "  Using datacard generator to create datacards and shape files..."

# Check if example datacard config exists
if [ -f "core/python/example_datacard_config.yaml" ]; then
    echo "  Example config: core/python/example_datacard_config.yaml"
    echo "  (Create your own config for real analysis)"
    
    # Uncomment to run datacard generation:
    # python core/python/create_datacards.py $DATACARD_CONFIG
    echo "  [Skipping - create your own datacard config]"
else
    echo -e "${RED}  Error: Example config not found${NC}"
fi

echo ""
echo -e "${YELLOW}Step 3: Running CMS Combine statistical analysis...${NC}"

# Set path to combine executable
COMBINE="./build/external/HiggsAnalysis/CombinedLimit/exe/combine"
DATACARDS_DIR="datacards"

if [ ! -d "$DATACARDS_DIR" ]; then
    echo "  Creating example datacards directory..."
    mkdir -p $DATACARDS_DIR
    echo "  [No datacards found - generate them first with create_datacards.py]"
    echo ""
    echo -e "${GREEN}=== Workflow template ready! ===${NC}"
    echo ""
    echo "To run a complete analysis:"
    echo "1. Modify this script with your analysis executable and configs"
    echo "2. Create a datacard config YAML file"
    echo "3. Run your analysis to generate ROOT files with histograms"
    echo "4. Run: python core/python/create_datacards.py your_config.yaml"
    echo "5. Run this script to execute Combine fits"
    exit 0
fi

# Change to datacards directory
cd $DATACARDS_DIR

# Find datacards
DATACARDS=$(ls datacard_*.txt 2>/dev/null || true)
if [ -z "$DATACARDS" ]; then
    echo -e "${RED}  No datacards found in $DATACARDS_DIR${NC}"
    echo "  Generate them first with create_datacards.py"
    cd ..
    exit 1
fi

echo "  Found datacards: $DATACARDS"
echo ""

# Run Combine analyses on each datacard
for DATACARD in $DATACARDS; do
    REGION=$(basename $DATACARD .txt | sed 's/datacard_//')
    echo -e "${GREEN}  Analyzing region: $REGION${NC}"
    
    # 1. Asymptotic limits
    echo "    -> Calculating asymptotic limits..."
    $COMBINE -M AsymptoticLimits $DATACARD -n .$REGION 2>&1 | tail -10
    
    # 2. Maximum likelihood fit
    echo "    -> Performing maximum likelihood fit..."
    $COMBINE -M FitDiagnostics $DATACARD -n .$REGION --saveShapes 2>&1 | tail -5
    
    # 3. Significance (if applicable)
    echo "    -> Calculating significance..."
    $COMBINE -M Significance $DATACARD -n .$REGION 2>&1 | tail -3
    
    # 4. Likelihood scan
    echo "    -> Running likelihood scan..."
    $COMBINE -M MultiDimFit $DATACARD -n .$REGION.scan \
        --algo grid --points 30 --setParameterRanges r=0,3 2>&1 | tail -3
    
    echo ""
done

# Return to main directory
cd ..

echo -e "${GREEN}=== Analysis Complete! ===${NC}"
echo ""
echo "Results are in $DATACARDS_DIR/:"
echo "  - Limits: higgsCombine.*.AsymptoticLimits.mH120.root"
echo "  - Fits: fitDiagnostics.*.root"
echo "  - Significance: higgsCombine.*.Significance.mH120.root"
echo "  - Scans: higgsCombine.*.scan.MultiDimFit.mH120.root"
echo ""
echo "To extract results, use ROOT:"
echo "  root -l $DATACARDS_DIR/higgsCombine.*.AsymptoticLimits.mH120.root"
echo "  root [1] limit->Show(0)  // Show expected limit"
echo "  root [2] limit->Show(5)  // Show observed limit"
echo ""
echo "For more details, see docs/COMBINE_INTEGRATION.md"
