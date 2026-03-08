#!/bin/bash

git submodule update --init --recursive

cmake . -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_TESTS=OFF -DBUILD_COMBINE=OFF -DBUILD_COMBINE_HARVESTER=OFF -DUSE_CUDA=OFF -DONNXRUNTIME_USE_CUDA=OFF -B build # `correction config --cmake`  # --trace
cd build
make -j$(nproc)
cd ..
