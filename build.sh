#!/bin/bash

source "$(dirname "$0")/env.sh"

git submodule update --init --recursive

cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_TESTS=ON -DBUILD_COMBINE=OFF -DBUILD_COMBINE_HARVESTER=OFF -DONNXRUNTIME_USE_CUDA=OFF -DUSE_CUDA=OFF # `correction config --cmake`  # --trace
cmake --build build -j"$(nproc)"
