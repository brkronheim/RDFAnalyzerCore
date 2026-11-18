#!/bin/bash

source "$(dirname -- "${BASH_SOURCE[0]}")/env.sh" &&

# CMS is a separate optional checkout. Only initialize core dependencies here.
git submodule update --init --recursive -- core/extern &&

cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_FLAGS_RELEASE="-O3 -DNDEBUG" \
    -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DBUILD_TESTS=ON \
    -DBUILD_COMBINE=OFF \
    -DBUILD_COMBINE_HARVESTER=OFF \
    -DONNXRUNTIME_USE_CUDA=OFF \
    -DUSE_CUDA=OFF "$@" &&
cmake --build build -j"$(nproc)"
