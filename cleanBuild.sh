#!/usr/bin/env bash

bash "$(dirname -- "${BASH_SOURCE[0]}")/clean.sh" &&
    source "$(dirname -- "${BASH_SOURCE[0]}")/build.sh" "$@"
