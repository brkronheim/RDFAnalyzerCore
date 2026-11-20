#!/bin/bash

(
    CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" || exit 1
    git submodule update --init --recursive || exit 1

    cmake -S . -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_FLAGS_RELEASE="-O3 -DNDEBUG" \
        -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_TESTS=OFF \
        -DBUILD_COMBINE=OFF \
        -DBUILD_COMBINE_HARVESTER=OFF \
        -DUSE_CUDA=OFF \
        -DONNXRUNTIME_USE_CUDA=OFF &&
    cmake --build build -j"$(nproc)"
)
