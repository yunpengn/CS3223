#!/usr/bin/env bash

# Setup the environment first.
echo "Begin to setup the environment..."

WORKING_DIR=`pwd`
CLASSPATH="$WORKING_DIR:$WORKING_DIR/classes:$WORKING_DIR/lib/CUP:$WORKING_DIR/lib/JLEX:."
COMPONENT="$WORKING_DIR"

export CLASSPATH
export COMPONENT

echo "Query environment setup successfully!"

# Begin to compile the project.
echo "Begin to build the project..."

javac -d ${COMPONENT}/classes ${COMPONENT}/src/qp/utils/*.java
javac -d ${COMPONENT}/classes ${COMPONENT}/src/qp/parser/*.java
javac -d ${COMPONENT}/classes ${COMPONENT}/src/qp/operators/*.java
javac -d ${COMPONENT}/classes ${COMPONENT}/src/qp/optimizer/*.java
javac -d ${COMPONENT}/classes ${COMPONENT}/testcases/*.java
javac -d ${COMPONENT}/classes ${COMPONENT}/src/QueryMain.java

echo "Project has been built successfully!"
