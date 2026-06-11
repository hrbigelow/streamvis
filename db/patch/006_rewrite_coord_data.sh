#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "$0")

echo "Acquiring sudo privilege..."
sudo -v

while true; do sudo -n true; sleep 60; done 2>/dev/null &
KEEPALIVE_PID=$!

echo "Building temporary patch library..."
make -C ${SCRIPT_DIR}/udf

echo "Installing temporary patch library to Postgres pkglibdir..."
sudo make -C ${SCRIPT_DIR}/udf install

cleanup() {
  echo "Running cleanup..."
  kill "$KEEPALIVE_PID" 2>/dev/null || true

  sudo make -C ${SCRIPT_DIR}/udf uninstall
  make -C ${SCRIPT_DIR}/udf clean
}

trap cleanup EXIT

echo "Applying database patch via psql..."
psql -U streamvis -d streamvis -f ${SCRIPT_DIR}/006_rewrite_coord_data.sql

echo "Patch applied successfully"

