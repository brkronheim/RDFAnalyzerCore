# Build wrappers, presets, dependencies, and installation

Use when reproducing a root script, switching optional toolchains, or diagnosing configuration/link differences. Read the selected wrapper before executing it; its filename is not a guarantee of its flags or side effects.

| Entry point | Current behavior to account for |
|---|---|
| [build.sh](../../../../build.sh) | Sources root `env.sh`, initializes submodules recursively under `core/extern`, configures Release with tests and CPU-only optional paths, then builds `build/`. |
| [buildFast.sh](../../../../buildFast.sh) | Initializes submodules, disables tests/Combine/CUDA, configures/builds `build/`; it does not perform the same environment sourcing as build.sh. |
| [buildFastCombine.sh](../../../../buildFastCombine.sh) | Builds with Combine and CombineHarvester enabled, tests and CUDA disabled. |
| [buildFull.sh](../../../../buildFull.sh) | Probes remote Combine availability, can disable Combine/Harvester on failure, enables CUDA paths and builds with bounded parallelism. Successful completion does not imply every optional feature remained enabled. |
| [buildTest.sh](../../../../buildTest.sh), [test.sh](../../../../test.sh) | The build/test wrapper runs tests only after a successful build. The test wrapper runs CTest against the checkout’s `build/` directory, prints failure output and treats zero tests as an error. |
| [clean.sh](../../../../clean.sh), [cleanBuild.sh](../../../../cleanBuild.sh), [cleanTest.sh](../../../../cleanTest.sh) | Delete build/testing directories and compose rebuild/test steps. They discard state; do not use them merely to inspect or diagnose an existing build. |
| [test_python_bindings.sh](../../../../test_python_bindings.sh) | Invokes [the Python integration test](../../../../core/tests/cpp/test_python_bindings.py) directly with `python3` and returns its exit status. The test selects the module path and reports missing prerequisites with status 77. |

These wrappers resolve paths from the script location. Build and cleanup sequences stop when a required step fails. Sourcing the environment can change the caller’s environment. Prefer the affected configured CMake target for a bounded change when a full wrapper would perform unrelated work.

[CMakePresets.json](../../../../CMakePresets.json) configures GCC (preset `g++`) in `build/` and Clang (preset `clang`) in `build-clang/`. Use the selected preset’s directory for subsequent build and test commands. Preset CVMFS paths and root `env.sh` need not select identical ROOT/Python stacks. Check them in the actual environment instead of copying version strings into new instructions.

[SetupOnnxRuntime.cmake](../../../../cmake/SetupOnnxRuntime.cmake) and [core/extern/CMakeLists.txt](../../../../core/extern/CMakeLists.txt) own native dependency setup. Read whether the chosen configuration fetches or builds resources and how GPU support is selected. [requirements-production.txt](../../../../requirements-production.txt) and [requirements-ci.txt](../../../../requirements-ci.txt) describe Python dependencies; the latter includes the production requirements. Optional Dask and ABI-matched PyROOT/native libraries are distinct requirements.

[SetupCombine.cmake](../../../../cmake/SetupCombine.cmake) creates external Combine/Harvester builds and applies repository patches via [PatchCombinePyROOT](../../../../cmake/PatchCombinePyROOT.cmake), [PatchCombineRobustHesse](../../../../cmake/PatchCombineRobustHesse.cmake), and [PatchCombineHarvesterForRoot](../../../../cmake/PatchCombineHarvesterForRoot.cmake). Trace configured tags, patch commands/stamps and actual external build outputs. Editing a patch file does not prove a previously built external project reapplied it. Keep Harvester's standalone CMake support in view at [CombineHarvesterStandalone](../../../../cmake/CombineHarvesterStandalone).

For install/package tasks, read active `install()` rules across [root](../../../../CMakeLists.txt), [core source](../../../../core/src/CMakeLists.txt), [bindings](../../../../core/python/bindings/CMakeLists.txt) and [LAW CMake](../../../../core/python/law/CMakeLists.txt). Do not assume all source-tree scripts, payloads or native libraries are installed together. Inspect the requested prefix and resulting import/runtime layout. Relevant checks include [VerifyCombineConfig](../../../../core/tests/cpp/VerifyCombineConfig.cmake), [VerifyCombineHarvesterConfig](../../../../core/tests/cpp/VerifyCombineHarvesterConfig.cmake) and the configured binding tests.
