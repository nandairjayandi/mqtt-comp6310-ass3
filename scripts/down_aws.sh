#!/bin/bash
#
# Downloads publisher TSV logs from the remote AWS VM after a test run.
# Run this script from local machine after tests complete.
#
# Usage: ./scripts/fetch_publisher_logs.sh <aws_user> <aws_ip> [ssh_key]
#   e.g: ./scripts/fetch_publisher_logs.sh ubuntu 3.24.5.6 ~/.ssh/aws-key.pem

set -e

USER=${1:?Usage: $0 <aws_user> <aws_ip> [ssh_key]}
HOST=${2:?Usage: $0 <aws_user> <aws_ip> [ssh_key]}
KEY=${3:-~/.ssh/id_rsa}

# TODO change the remote path
REMOTE_PATH="/path/to/mqtt-comp6310-ass3/out/publisher/"
LOCAL_PATH="./out/publisher/"

echo "Fetching publisher logs from $USER@$HOST..."
scp -i "$KEY" -r "$USER@$HOST:$REMOTE_PATH" "$LOCAL_PATH"
echo "Done. Files in out/publisher/"