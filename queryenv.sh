#!/usr/bin/env bash

WORKING_DIR=`pwd`
CLASSPATH="$WORKING_DIR:$WORKING_DIR/classes:$WORKING_DIR/lib/CUP:$WORKING_DIR/lib/JLEX:."
COMPONENT="$WORKING_DIR"

export CLASSPATH
export COMPONENT

echo "Query environment setup successfully"
