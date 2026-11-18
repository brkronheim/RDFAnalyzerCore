#!/usr/bin/env bash

rdf_clean_build() {
    local repo_dir
    repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P) || return
    rm -rf -- "$repo_dir/Testing" "$repo_dir/build"
}

rdf_clean_build
