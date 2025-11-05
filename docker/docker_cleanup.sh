#!/bin/bash

# Docker Cleanup Script
# Automatically removes unused Docker containers, images, volumes, and build cache
# Run this script every month to keep Docker storage clean

set -euo pipefail

# Log file location
LOG_FILE="${HOME}/.docker_cleanup.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Function to log messages
log() {
    echo "[$TIMESTAMP] $1" | tee -a "$LOG_FILE"
}

log "🧹 Starting Docker cleanup..."

# Get disk usage before cleanup
BEFORE=$(docker system df --format "{{.Size}}" | head -1)
log "📊 Disk usage before cleanup: $(docker system df --format '{{.Size}}' | head -1)"

# Remove stopped containers
log "🗑️  Removing stopped containers..."
STOPPED_CONTAINERS=$(docker container prune -f 2>&1 | grep -oP 'Total reclaimed space: \K[0-9.]+[A-Z]+' || echo "0B")
log "   Reclaimed: $STOPPED_CONTAINERS"

# Remove unused images (older than 7 days to be safe)
log "🗑️  Removing unused images (older than 7 days)..."
UNUSED_IMAGES=$(docker image prune -a -f --filter "until=168h" 2>&1 | grep -oP 'Total reclaimed space: \K[0-9.]+[A-Z]+' || echo "0B")
log "   Reclaimed: $UNUSED_IMAGES"

# Remove dangling images
log "🗑️  Removing dangling images..."
DANGLING_IMAGES=$(docker image prune -f 2>&1 | grep -oP 'Total reclaimed space: \K[0-9.]+[A-Z]+' || echo "0B")
log "   Reclaimed: $DANGLING_IMAGES"

# Remove unused volumes
log "🗑️  Removing unused volumes..."
UNUSED_VOLUMES=$(docker volume prune -f 2>&1 | grep -oP 'Total reclaimed space: \K[0-9.]+[A-Z]+' || echo "0B")
log "   Reclaimed: $UNUSED_VOLUMES"

# Remove build cache
log "🗑️  Removing build cache..."
BUILD_CACHE=$(docker builder prune -f 2>&1 | grep -oP 'Total: \K[0-9.]+[A-Z]+' || echo "0B")
log "   Reclaimed: $BUILD_CACHE"

# Get disk usage after cleanup
AFTER=$(docker system df --format "{{.Size}}" | head -1)
log "📊 Disk usage after cleanup: $(docker system df --format '{{.Size}}' | head -1)"

log "✅ Docker cleanup completed successfully!"
log ""

