#!/bin/bash
#
# Downloads publisher TSV logs from the remote AWS VM after a test run.
# Run this script from the project root (~/mqtt-comp6310-ass3).
#
# Usage: ./scripts/fetch_publisher_logs.sh <aws_user> <aws_ip> [ssh_key]
#   e.g: ./scripts/fetch_publisher_logs.sh ec2-user 3.24.5.6 ~/.ssh/aws-key.pem

set -e

# Validate arguments
USER=${1:?Error: Missing AWS username. Usage: $0 <aws_user> <aws_ip> [ssh_key]}
HOST=${2:?Error: Missing AWS IP. Usage: $0 <aws_user> <aws_ip> [ssh_key]}
KEY=${3:-~/.ssh/id_rsa}

# Define paths
REMOTE_PATH="~/mqtt-comp6310-ass3/out/"
LOCAL_PATH="./out/"

# Ensure local directory exists
mkdir -p "$LOCAL_PATH"

echo "Fetching logs from $USER@$HOST..."
scp -i "$KEY" -r "$USER@$HOST:$REMOTE_PATH" "$LOCAL_PATH"
echo "Done. Files saved to $LOCAL_PATH"