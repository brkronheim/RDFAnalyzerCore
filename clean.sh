#!/usr/bin/env bash
(
    CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" || exit 1
    rm -rf -- Testing build
)
