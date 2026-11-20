#!/usr/bin/env bash
(
    CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/build" || exit 1
    ctest --output-on-failure --no-tests=error
)
