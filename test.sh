#!/usr/bin/env bash

ctest --test-dir "$(dirname -- "${BASH_SOURCE[0]}")/build" --output-on-failure --no-tests=error "$@"
