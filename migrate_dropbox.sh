#!/bin/bash

# Exit on error
set -e

# Check for dry-run flag
DRY_RUN=false
if [[ "$1" == "--dry-run" ]]; then
  DRY_RUN=true
  echo "Running in dry-run mode. No data will be transferred."
fi

# Constants
DROPBOX_PATH="/your/source/path"      # Change to the Dropbox folder path
BOX_PATH="/your/target/path"          # Change to the Box folder path
LOG_FILE="rclone_migration_$(date +%Y%m%d_%H%M%S).log"
TRANSFERS=4
CHECKERS=8
BANDWIDTH="100M"

# Build base command
CMD=(
  rclone copy "dropbox:$DROPBOX_PATH" "box:$BOX_PATH"
  --transfers=$TRANSFERS
  --checkers=$CHECKERS
  --bwlimit=$BANDWIDTH
  --progress
  --log-file="$LOG_FILE"
  --log-level=INFO
)

# Add dry-run if specified
if $DRY_RUN; then
  CMD+=(--dry-run)
fi

echo "Starting Dropbox to Box migration..."
echo "Source: dropbox:$DROPBOX_PATH"
echo "Target: box:$BOX_PATH"
echo "Logging to $LOG_FILE"

# Run rclone command
"${CMD[@]}"

echo "Migration completed."