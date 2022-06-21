#!/bin/bash

VERSION=$1
WORKFLOW=$2
ERA=$3
ANALYSIS=$4

law run MetaDictCreator --version $VERSION --workflow $WORKFLOW --era $ERA --analysis $ANALYSIS
law run Postprocessing --version $VERSION --workflow $WORKFLOW --era $ERA --analysis $ANALYSIS