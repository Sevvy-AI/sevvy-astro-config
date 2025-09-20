#!/bin/bash

# Health check script for development deployments
# This script can be called from DAGs to verify deployment health

set -e

ENDPOINT=${1:-"http://localhost:8080/health"}
TIMEOUT=${2:-30}

echo "Performing health check on: $ENDPOINT"
echo "Timeout: $TIMEOUT seconds"

# Perform the health check
response=$(curl -s -o /dev/null -w "%{http_code}" --max-time $TIMEOUT "$ENDPOINT")

if [ "$response" = "200" ]; then
    echo "✅ Health check passed (HTTP $response)"
    exit 0
elif [ "$response" = "000" ]; then
    echo "❌ Health check failed: Unable to connect (timeout or connection refused)"
    exit 1
else
    echo "❌ Health check failed (HTTP $response)"
    exit 1
fi 