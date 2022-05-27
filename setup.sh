#!/usr/bin/env bash

action() {
    # determine the directory of this file
    local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"

    # custom  (needed for our error handling)  law version in Torbens home:
    export PATH="/home/tolange/share/law/bin/:$PATH"
    export PYTHONPATH="/home/tolange/share/law/:$this_dir:$PYTHONPATH"

    export LAW_HOME="$this_dir/.law"
    export LAW_CONFIG_FILE="$this_dir/law.cfg"

    law index --verbose

    # where should log files go (if not debug and sucessfull these are immidieatly cleaned -> home should be ok)
    export ANALYSIS_LOGFILE_PATH="$this_dir/logs"
    mkdir -p $ANALYSIS_LOGFILE_PATH

    #workarea where can we safely create files e.g. not hdfs but scratch?
    export ANALYSIS_WORKAREA="$this_dir"

    # config like output e.g. scratch-persistent
    export ANALYSIS_CONFIG_PATH="$ANALYSIS_PATH/config"
    # root like file output e.g hdfs?
    export ANALYSIS_ROOT_PATH="$ANALYSIS_PATH/root"

    export ANALYSIS_PATH="$this_dir"
    export ANALYSIS_DATA_PATH="$ANALYSIS_PATH/data"

    source "$( law completion )" ""
}
action
