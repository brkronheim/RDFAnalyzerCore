#!/bin/bash

git submodule update --init --recursive

cmake . -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_TESTS=ON -B build # `correction config --cmake`  # --trace
cd build
make -j$(nproc)
cd ..
