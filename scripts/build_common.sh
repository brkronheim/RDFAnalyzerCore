#!/usr/bin/env bash

rdf_build() {
    local mode=$1
    shift
    local repo_dir tests=OFF combine=OFF harvester=OFF cuda=OFF jobs
    repo_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P) || return

    case "$mode" in
        standard) tests=ON ;;
        fast) ;;
        fast-combine) combine=ON; harvester=ON ;;
        full)
            tests=ON
            cuda=ON
            # Keep the full CUDA build usable when Combine cannot be fetched.
            if GIT_TERMINAL_PROMPT=0 timeout 5 git ls-remote --heads \
                https://github.com/cms-analysis/HiggsAnalysis-CombinedLimit.git >/dev/null 2>&1; then
                combine=ON
                harvester=ON
            else
                printf '%s\n' 'Combine repository unavailable. Defaulting to a build without Combine and CombineHarvester.' >&2
            fi
            ;;
        *) printf 'Unknown build mode: %s\n' "$mode" >&2; return 2 ;;
    esac

    if [[ -z ${ROOTSYS:-} ]] || ! command -v root-config >/dev/null; then
        source "$repo_dir/env.sh" || return
    fi

    # CMS is a separate optional checkout that is fetched explicitly.
    git -C "$repo_dir" submodule update --init --recursive -- core/extern || return
    cmake -S "$repo_dir" -B "$repo_dir/build" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_FLAGS_RELEASE="-O3 -DNDEBUG" \
        -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_TESTS="$tests" \
        -DBUILD_COMBINE="$combine" \
        -DBUILD_COMBINE_HARVESTER="$harvester" \
        -DUSE_CUDA="$cuda" \
        -DONNXRUNTIME_USE_CUDA="$cuda" "$@" || return

    jobs=${CMAKE_BUILD_PARALLEL_LEVEL:-}
    if [[ -z "$jobs" ]]; then
        jobs=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN) || return
        if [[ "$mode" == full ]] && (( jobs > 16 )); then
            jobs=16
        fi
    fi
    cmake --build "$repo_dir/build" --parallel "$jobs"
}

rdf_build "$@"
