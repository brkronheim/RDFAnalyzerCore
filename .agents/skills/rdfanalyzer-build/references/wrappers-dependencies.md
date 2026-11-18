# Build wrappers, presets, dependencies, and installation

Use when reproducing a root script, switching optional toolchains, or diagnosing configuration/link differences. Read the selected wrapper before executing it; its filename is not a guarantee of its flags or side effects.

| Entry point | Build or action |
|---|---|
| [build.sh](../../../../build.sh) | CPU build with tests. |
| [buildFast.sh](../../../../buildFast.sh) | CPU build without tests. |
| [buildFastCombine.sh](../../../../buildFastCombine.sh) | CPU build with Combine and CombineHarvester, without tests. |
| [buildFull.sh](../../../../buildFull.sh) | CUDA build with tests. Enables Combine and CombineHarvester by default when the Combine repository is reachable. |
| [buildTest.sh](../../../../buildTest.sh), [test.sh](../../../../test.sh) | Build and run CTest, or run CTest on the existing build. |
| [clean.sh](../../../../clean.sh), [cleanBuild.sh](../../../../cleanBuild.sh), [cleanTest.sh](../../../../cleanTest.sh) | Remove the checkout's build and testing directories, optionally rebuild and run CTest. |

The build scripts share [the wrapper implementation](../../../../scripts/build_common.sh).
They reuse an active ROOT environment or source the repository's `env.sh`,
initialize the pinned core submodules, and configure the checkout's `build/`
directory. They accept CMake cache options and can run from another directory.
Set `CMAKE_BUILD_PARALLEL_LEVEL` to select the number of build workers. The full
build defaults to at most 16 workers. The wrappers can be sourced to keep the
environment active in the calling shell.

[CMakePresets.json](../../../../CMakePresets.json) defines GCC and Clang configurations with explicit dependency paths and flags, currently sharing the `build` directory. Inspect the active cache/compiler/generator before switching presets; do not reuse a GCC cache as though it were a fresh Clang configuration. Preset CVMFS paths and root `env.sh` need not select identical ROOT/Python stacks. Check them in the actual environment instead of copying version strings into new instructions.

[SetupOnnxRuntime.cmake](../../../../cmake/SetupOnnxRuntime.cmake) and [core/extern/CMakeLists.txt](../../../../core/extern/CMakeLists.txt) own native dependency setup. Linux CPU and CUDA downloads fall back to Microsoft's pinned NuGet archives if the GitHub release cannot be fetched. Read the setup modules for alternate package roots and GPU selection. [requirements-production.txt](../../../../requirements-production.txt) and [requirements-ci.txt](../../../../requirements-ci.txt) describe Python dependencies; the latter includes the production requirements. Optional Dask and ABI-matched PyROOT/native libraries are distinct requirements.

[SetupCombine.cmake](../../../../cmake/SetupCombine.cmake) creates external Combine/Harvester builds and applies repository patches via [PatchCombinePyROOT](../../../../cmake/PatchCombinePyROOT.cmake), [PatchCombineRobustHesse](../../../../cmake/PatchCombineRobustHesse.cmake), and [PatchCombineHarvesterForRoot](../../../../cmake/PatchCombineHarvesterForRoot.cmake). Trace configured tags, patch commands/stamps and actual external build outputs. Editing a patch file does not prove a previously built external project reapplied it. Keep Harvester's standalone CMake support in view at [CombineHarvesterStandalone](../../../../cmake/CombineHarvesterStandalone).

For install/package tasks, read active `install()` rules across [root](../../../../CMakeLists.txt), [core source](../../../../core/src/CMakeLists.txt), [bindings](../../../../core/python/bindings/CMakeLists.txt) and [LAW CMake](../../../../core/python/law/CMakeLists.txt). Do not assume all source-tree scripts, payloads or native libraries are installed together. Inspect the requested prefix and resulting import/runtime layout. Relevant checks include [VerifyCombineConfig](../../../../core/tests/cpp/VerifyCombineConfig.cmake), [VerifyCombineHarvesterConfig](../../../../core/tests/cpp/VerifyCombineHarvesterConfig.cmake) and the configured binding tests.
