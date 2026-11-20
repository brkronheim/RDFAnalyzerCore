#!/usr/bin/env bash

source "$(dirname -- "${BASH_SOURCE[0]}")/cleanBuild.sh" "$@" &&
    bash "$(dirname -- "${BASH_SOURCE[0]}")/test.sh"
