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

    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local repo_dir="$( cd "${this_dir}/.." && pwd )"
    local repo_venv_activate="${repo_dir}/.venv/bin/activate"

    if [ -z "${VIRTUAL_ENV:-}" ] && [ -f "${repo_venv_activate}" ]; then
        # Prefer the repository-local virtualenv for LAW and Luigi imports.
        source "${repo_venv_activate}"
    fi

    # setup external software once when not in the example image
    #if [ -z "${LAW_DOCKER_EXAMPLE}" ]; then
    #    local sw_dir="${this_dir}/tmp"
    #    if [ ! -d "${sw_dir}" ]; then
    #        mkdir -p "${sw_dir}"
    #        git clone https://github.com/spotify/luigi.git "${sw_dir}/luigi"
    #        ( cd "${sw_dir}/luigi" && git checkout tags/2.8.13 )
    #        git clone https://github.com/benjaminp/six.git "${sw_dir}/six"
    #    fi
    #
    #    # adjust paths to find local software
    #    export PATH="${law_base}/bin:${sw_dir}/luigi/bin:${PATH}"
    #    export PYTHONPATH="${law_base}:${sw_dir}/luigi:${sw_dir}/six:${PYTHONPATH}"
    #fi

    if [ -n "${PYTHONPATH:-}" ]; then
        export PYTHONPATH="${this_dir}:${this_dir}/../core/python:${PYTHONPATH}"
    else
        export PYTHONPATH="${this_dir}:${this_dir}/../core/python"
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
