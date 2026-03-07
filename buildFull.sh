#!/bin/bash

git submodule update --init --recursive

# Determine whether the external CMS Combine/CombineHarvester repositories
# are reachable.  If the network is offline or GitHub is blocked we'll disable
# the corresponding build options so configuration can still succeed.
COMBINE_FLAG=ON
COMBINE_HARV_FLAG=ON


cmake . \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DBUILD_TESTS=ON \
    -DBUILD_COMBINE=${COMBINE_FLAG} \
    -DBUILD_COMBINE_HARVESTER=${COMBINE_HARV_FLAG} \
    -DUSE_CUDA=ON \
    -DONNXRUNTIME_USE_CUDA=ON \
    -B build # `correction config --cmake`  # --trace

cd build
make -j$(( $(nproc) > 16 ? 16 : $(nproc) ))
cd ..
