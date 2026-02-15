# Example Analysis with Config-Driven Histograms

This example demonstrates how to use config-driven histograms in an analysis.

## Files

- `analysis.cc`: Main analysis code
- `cfg.txt`: Main configuration file
- `histograms.txt`: Histogram definitions
- `CMakeLists.txt`: Build configuration

## Running the Example

```bash
# Build
mkdir -p build
cd build
cmake ..
make

# Run
./ExampleAnalysisConfigHistograms ../cfg.txt
```

## Key Points

1. **Add NDHistogramManager plugin**: The analyzer needs the histogram manager plugin to enable config-driven histograms.

2. **Define variables**: All variables used in histograms must be defined before calling `bookConfigHistograms()`.

3. **Call bookConfigHistograms()**: This triggers the booking of all histograms defined in the config file. Call it after all defines and filters.

4. **Histogram config file**: The `histogramConfig` parameter in `cfg.txt` points to the histogram definitions file.

## Histogram Configuration

The `histograms.txt` file contains histogram definitions with various options:

- **Simple histogram**: Only required fields
- **Labeled histogram**: Includes custom axis label
- **Multi-dimensional**: Includes channel, control region, or sample category dimensions

See `docs/CONFIG_HISTOGRAMS.md` for complete documentation.
