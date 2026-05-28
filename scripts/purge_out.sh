#!/bin/bash

echo "[WARNING]: This script will delete all the output data in ./out/ directory"

read -p "Are you sure you want to delete all output data? Type YES to confirm: " confirm
if [ "$confirm" != "YES" ]; then
    echo "Aborted. Confirmation not received"
    exit 1
fi

read -p "Create a .zip backup before deletion? (y/n): " backup
if [ "$backup" = "y" ] || [ "$backup" = "Y" ]; then
    backup_file="backup_out_$(date +%Y%m%d_%H%M%S).zip"
    echo "Creating backup: $backup_file"
    tar -czf "$backup_file" ./out/
    if [ $? -ne 0 ]; then
        echo "[Error]: Backup failed. Aborting deletion."
        exit 1
    fi
    echo "Backup created successfully"
fi

# Safe deletion using find to avoid "Argument list too long"
LOCAL_PATH="./out"
echo "Deleting files..."
find "$LOCAL_PATH/publisher" -type f -delete 2>/dev/null

find "$LOCAL_PATH/analyser" -type f -delete 2>/dev/null
find "$LOCAL_PATH/analyser/sys" -type f -delete 2>/dev/null

find "./monitoring/influxdb/" -type f -delete 2>/dev/null
find "./monitoring/grafana/" -type f ! -path "*/provisioning/*" -delete 2>/dev/null

echo "All output data has been deleted."