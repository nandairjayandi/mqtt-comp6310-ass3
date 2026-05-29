#!/bin/bash
#
# Convenience script to connect analyser in docker container.
# Usage: ./scripts/connect_analyser.sh [ip:port]
#   e.g: ./scripts/connect_analyser.sh 192.168.1.100:1883

set -e

TARGET=${1:-redacted:1883}

# Validate the format (optional but helpful)
if [[ ! "$TARGET" =~ ^[a-zA-Z0-9\.\-]+:[0-9]+$ ]]; then
    echo "Error: Invalid format for ip:port. Expected format: <ip>:<port>"
    exit 1
fi

echo "Connecting analyser to $TARGET..."
docker compose run --rm -d analyser sh -c "./analyser tcp://$TARGET"
docker compose logs -f -t analyser
