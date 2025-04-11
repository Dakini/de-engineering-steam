#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "=== Setting up Prefect environment ==="

# Check if prefect is installed
if ! command -v prefect &> /dev/null; then
    echo "Error: prefect is not installed. Please install it first."
    exit 1
fi

# Start Prefect server in the background
echo "Starting Prefect server..."
prefect server start --background
sleep 5  # Give the server a moment to start up

# Configure the Prefect API URL
echo "Configuring API URL..."
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

# Create work queue
echo "Creating work queue..."
prefect work-queue create --pool "steam_de" "default" || echo "Work queue already exists. Continuing..."

# Start a worker
echo "Starting worker..."
if pgrep -f "prefect worker start --pool steam_de" > /dev/null; then
    echo "Worker already running. Skipping worker start."
else
    nohup prefect worker start --pool "steam_de" --work-queue "default" &> worker.out &
    echo "Worker started in background. Output redirected to worker.out"
fi

# Deploy the workflows
echo "Deploying workflows..."
cd "$(dirname "$0")" || exit 1  # Navigate to script's directory (ingestion folder)
prefect deploy --all

echo "Prefect setup complete!"
echo "You can run the ingestion workflow with: prefect deployment run 'stream-data-workflow/SteamIngest'"
echo "You can run the cleaning workflow with: prefect deployment run 'run-clean-dataworkflow/SteamClean'"