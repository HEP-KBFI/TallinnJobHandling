#!/usr/bin/env bash

LAW_INSTALLATION_DIR="/home/tolange/share/law"

action() {
    # determine the directory of this file
    local SETUP_SCRIPT_PATH="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local SETUP_SCRIPT_DIR="$( cd "$( dirname "$SETUP_SCRIPT_PATH" )" && pwd )"

    # cd $LAW_INSTALLATION_DIR
    # pip install -r requirements.txt --user
    # cd $SETUP_SCRIPT_DIR

    # custom  (needed for our error handling)  law version in Torbens home:
    export PATH="$LAW_INSTALLATION_DIR/bin/:$PATH"
    export PYTHONPATH="$LAW_INSTALLATION_DIR/:$SETUP_SCRIPT_DIR:$PYTHONPATH"

    export LAW_HOME="$SETUP_SCRIPT_DIR/.law"
    export LAW_CONFIG_FILE="$SETUP_SCRIPT_DIR/law.cfg"

    law index --verbose

    export ANALYSIS_PATH="$SETUP_SCRIPT_DIR"
    # where should log files go (if not debug and sucessfull these are immidieatly cleaned -> home should be ok)
    export ANALYSIS_LOGFILE_PATH="$SETUP_SCRIPT_DIR/logs"
    mkdir -p $ANALYSIS_LOGFILE_PATH

    #workarea where can we safely create files e.g. not hdfs but scratch?
    export ANALYSIS_WORKAREA="$SETUP_SCRIPT_DIR"

    # config like output e.g. scratch-persistent
    export ANALYSIS_CONFIG_PATH="$ANALYSIS_PATH/config"
    # root like file output e.g hdfs?
    export ANALYSIS_ROOT_PATH="$ANALYSIS_PATH/root"

    export ANALYSIS_DATA_PATH="$ANALYSIS_PATH/data"

    source "$( law completion )" ""
}
action
