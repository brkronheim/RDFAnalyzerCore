#!/usr/bin/env bash

source "$(dirname -- "${BASH_SOURCE[0]}")/build.sh" "$@" &&
    bash "$(dirname -- "${BASH_SOURCE[0]}")/test.sh"
