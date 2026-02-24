#!/bin/bash

git submodule update --init --recursive

cmake . -DCMAKE_BUILD_TYPE=g++ -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_TESTS=OFF -B build # `correction config --cmake`  # --trace
cd build
make -j$(nproc)
cd ..
