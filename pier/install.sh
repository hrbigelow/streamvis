#!/bin/bash
if [ "$EUID" -ne 0 ]; then 
  echo "Please run as root (e.g., sudo ./install.sh)"
  exit 1
fi

# Exit on any errors
set -e

BINARY_SRC="./pier"
BINARY_DST="/usr/local/bin/pier"
SERVICE_SRC="./streamvis-rpc.service"
SERVICE_DST="/etc/systemd/system/streamvis-rpc.service"

echo "Installing Streamvis RPC Server..."

install -m 755 "$BINARY_SRC" "$BINARY_DST"
install -m 644 "$SERVICE_SRC" "$SERVICE_DST"

echo "Reloading systemd daemon..."
systemctl daemon-reload

echo "Starting streamvis-rpc..."
systemctl enable streamvis-rpc.service
systemctl restart streamvis-rpc.service

echo "Installation complete.  Check status with: systemctl status streamvis-rpc"

