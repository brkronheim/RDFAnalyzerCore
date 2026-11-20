#!/bin/bash

(
    CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" || exit 1
    git submodule update --init --recursive || exit 1

    # Determine whether the external CMS Combine/CombineHarvester repositories
    # are reachable.  If the network is offline or GitHub is blocked we'll disable
    # the corresponding build options so configuration can still succeed.
    COMBINE_FLAG=ON
    COMBINE_HARV_FLAG=ON

    if ! timeout 5 git ls-remote --heads https://github.com/cms-analysis/HiggsAnalysis-CombinedLimit.git > /dev/null 2>&1; then
        echo "Combine repository not reachable; disabling BUILD_COMBINE and BUILD_COMBINE_HARVESTER."
        COMBINE_FLAG=OFF
        COMBINE_HARV_FLAG=OFF
    fi


    cmake -S . -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_FLAGS_RELEASE="-O3 -DNDEBUG" \
        -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_TESTS=ON \
        -DBUILD_COMBINE=${COMBINE_FLAG} \
        -DBUILD_COMBINE_HARVESTER=${COMBINE_HARV_FLAG} \
        -DUSE_CUDA=ON \
        -DONNXRUNTIME_USE_CUDA=ON &&
    cmake --build build -j"$(( $(nproc) > 16 ? 16 : $(nproc) ))"
)
