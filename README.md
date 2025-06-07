## Dev Branch
This is the branch on which active development is performed, as such it may not always be stable or work. Below is the general readme.

This contains the Core, analysis agnositc version of RDFAnalyzer. This isn't enough on its own to run an analysis yet, but it contains all the coded used to construct an Analyzer object. 

## Requirements:
This has been tested and works with ROOT = 6.30/02. The progress bar was added around ROOT 6.28. There is currently a macro to conditionally compile it, but this hasn't been extensively tested and the version cutoff may be wrong. If necesary the code can simply be commented out in lines 30-40 of src/analyzer.cc.

## Organization
### core
This is where the code from this repo will live.
### analyses
This is where code specific to certain analyses will live. It should be contained in its own repo and cloned here.
### runners
This is where specific applications which actually run the code will live. Most of these should live in their own repos.
### python
This will contain various helper python scripts to help launch jobs.

## Instaling
Install with git:
```
git@github.com:brkronheim/RDFAnalyzerCore.git
```
There are also subpackages which will be installed, but this can be done in the build step. At the moment this covers everything, though eventually ONNX will be incorporated, likely through directly downloading some binary files as building the whole package into this project will be complicated. That will most likely also go in the build script.

## Building
To run on lxplus, first run
```
source env.sh
```

This is built using cmake but the build commands are wrapped in a build script. Run
```
source build.sh
```
to create the build files and compile them in the build directory. Note that clang can be used instead by replacing g++ with clang in the cmake statement.

