#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "$0")

echo "Acquiring sudo privilege..."
sudo -v

echo "Building streamvis_udfs library..."
make -C ${SCRIPT_DIR}/../udf 

echo "Installing streamvis_udfs library to Postgres pkglibdir..."
sudo make -C ${SCRIPT_DIR}/../udf install

echo "Installing extension..."
psql -U streamvis -d streamvis -f ${SCRIPT_DIR}/005_install_streamvis_udfs.sql

echo "Patch applied successfully"

