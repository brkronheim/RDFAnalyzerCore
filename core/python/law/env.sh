#!/usr/bin/env bash

action() {
    local shell_is_zsh="false"
    if [ -n "${ZSH_VERSION:-}" ]; then
        shell_is_zsh="true"
    fi

    local this_file=""
    if [ "${shell_is_zsh}" = "true" ]; then
        this_file="${(%):-%x}"
    else
        this_file="${BASH_SOURCE[0]:-$0}"
    fi

    local this_dir="$( CDPATH= cd "$( dirname "${this_file}" )" && pwd )"
    local repo_dir="$( CDPATH= cd "${this_dir}/../../.." && pwd )"
    local repo_venv_activate="${repo_dir}/.venv/bin/activate"

    if [ -z "${VIRTUAL_ENV:-}" ] && [ -f "${repo_venv_activate}" ]; then
        # Prefer the repository-local virtualenv for LAW and Luigi imports.
        source "${repo_venv_activate}"
    fi

    if [ -n "${PYTHONPATH:-}" ]; then
        export PYTHONPATH="${this_dir}:${this_dir}/..:${PYTHONPATH}"
    else
        export PYTHONPATH="${this_dir}:${this_dir}/.."
    fi
    export LAW_HOME="${this_dir}/.law"
    export LAW_CONFIG_FILE="${this_dir}/law.cfg"
    export DATA_PATH="${this_dir}/data"

    if [[ $- == *i* ]] && command -v law >/dev/null 2>&1; then
        local completion_path=""
        completion_path="$(law completion 2>/dev/null || true)"
        if [ -n "${completion_path}" ] && [ -f "${completion_path}" ]; then
            source "${completion_path}" "" >/dev/null 2>&1 || true
        fi
    fi
}
action
