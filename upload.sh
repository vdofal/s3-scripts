#!/bin/bash
# S3 upload — minimal logging
# Usage:
#   ./upload.sh                          # use DEFAULT_SOURCE_DIR, unlimited bandwidth
#   ./upload.sh /path/to/dir             # single source dir
#   ./upload.sh /path/dir1 /path/dir2    # multiple source dirs (synced sequentially)
#   ./upload.sh [source_dirs...] 10MB/s  # with bandwidth cap (last arg matches N+unit/s)
#   CLIENT_SUBFOLDER=Chevron ./upload.sh /path/to/dir   # under raw-data/Chevron/

set -u

# ====== CONFIG — EDIT THESE ======
DEFAULT_SOURCE_DIR=""
S3_DEST="s3://spot-client-data/raw-data"
AWS_PROFILE_NAME="playground"
LOG_FILE="upload.log"
CONCURRENT_REQUESTS=10
CHUNK_SIZE="128MB"
MULTIPART_THRESHOLD="64MB"
CLIENT_SUBFOLDER="${CLIENT_SUBFOLDER:-}"
# =================================

export AWS_REQUEST_CHECKSUM_CALCULATION=when_required

BANDWIDTH_LIMIT=""
SOURCE_DIRS=()

if [[ $# -gt 0 && "${@: -1}" =~ ^[0-9]+[KMGT]B/s$ ]]; then
    BANDWIDTH_LIMIT="${@: -1}"
    SOURCE_DIRS=("${@:1:$#-1}")
else
    SOURCE_DIRS=("$@")
fi

[ ${#SOURCE_DIRS[@]} -eq 0 ] && SOURCE_DIRS=("$DEFAULT_SOURCE_DIR")

export AWS_PROFILE="$AWS_PROFILE_NAME"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# S3 destination for a source: dir -> raw-data/<dir>, file -> raw-data/<parent>/<file>
dest_for() {
    local prefix="$S3_DEST"
    [ -n "$CLIENT_SUBFOLDER" ] && prefix="$S3_DEST/$CLIENT_SUBFOLDER"
    if [ -f "$1" ]; then
        echo "${prefix}/$(basename "$(dirname "$1")")/$(basename "$1")"
    else
        echo "${prefix}/$(basename "$1")"
    fi
}

# Header
{
    echo ""
    echo "=========================================="
    echo "S3 UPLOAD"
    echo "Started: $(date)"
    for d in "${SOURCE_DIRS[@]}"; do
        echo "Source : $d  →  $(dest_for "$d")"
    done
    if [ -z "$BANDWIDTH_LIMIT" ]; then
        echo "Limit  : UNLIMITED"
    else
        echo "Limit  : $BANDWIDTH_LIMIT"
    fi
    echo "=========================================="
} | tee -a "$LOG_FILE"

for d in "${SOURCE_DIRS[@]}"; do
    if [ ! -d "$d" ] && [ ! -f "$d" ]; then
        HINT=""
        [[ "$d" == *\\* ]] && HINT=" (path contains a literal backslash — quote OR escape spaces, not both)"
        log "ERROR: Source $d does not exist.$HINT"
        exit 1
    fi
done

if ! aws sts get-caller-identity > /dev/null 2>&1; then
    log "ERROR: AWS credentials invalid or expired."
    exit 1
fi

aws configure set default.s3.max_concurrent_requests "$CONCURRENT_REQUESTS"
aws configure set default.s3.multipart_chunksize    "$CHUNK_SIZE"
aws configure set default.s3.multipart_threshold    "$MULTIPART_THRESHOLD"

if [ -z "$BANDWIDTH_LIMIT" ]; then
    aws configure set default.s3.max_bandwidth "100GB/s"
    EFFECTIVE_LIMIT="unlimited"
else
    aws configure set default.s3.max_bandwidth "$BANDWIDTH_LIMIT"
    EFFECTIVE_LIMIT="$BANDWIDTH_LIMIT"
fi

log "Config: concurrent=$CONCURRENT_REQUESTS chunk=$CHUNK_SIZE threshold=$MULTIPART_THRESHOLD bandwidth=$EFFECTIVE_LIMIT"

log "Sync starting (${#SOURCE_DIRS[@]} source(s))..."

OVERALL_EXIT=0
START_TIME=$(date +%s)

DIR_INDEX=0
for SOURCE_DIR in "${SOURCE_DIRS[@]}"; do
    DIR_INDEX=$((DIR_INDEX + 1))
    DIR_DEST=$(dest_for "$SOURCE_DIR")
    if [ -f "$SOURCE_DIR" ]; then
        SOURCE_COUNT=1
    else
        SOURCE_COUNT=$(find "$SOURCE_DIR" -maxdepth 1 -type f | wc -l)
    fi
    SOURCE_SIZE=$(du -sh "$SOURCE_DIR" 2>/dev/null | cut -f1)
    DEST_COUNT=$(aws s3 ls "$DIR_DEST" --recursive 2>/dev/null | wc -l)
    log "[$DIR_INDEX/${#SOURCE_DIRS[@]}] Source: $SOURCE_DIR — $SOURCE_COUNT files ($SOURCE_SIZE) | Dest: $DIR_DEST (already has: $DEST_COUNT files)"

    # Filter aws output:
    #   - --no-progress disables per-chunk progress bar
    #   - grep keeps only meaningful event lines
    #   - awk dedupes identical lines (in case sync still emits duplicates)
    #     and adds a timestamp prefix
    if [ -f "$SOURCE_DIR" ]; then
        AWS_CMD=(aws s3 cp "$SOURCE_DIR" "$DIR_DEST")
    else
        AWS_CMD=(aws s3 sync "$SOURCE_DIR" "$DIR_DEST" --exclude "*/*")
    fi
    "${AWS_CMD[@]}" \
        --no-progress \
        --cli-read-timeout 0 \
        --cli-connect-timeout 60 \
        2>&1 | \
        grep --line-buffered -E "upload:|error|warning|failed|Expired|Timeout|SlowDown" | \
        awk '!seen[$0]++ { print strftime("[%H:%M:%S]"), $0; fflush() }' | \
        tee -a "$LOG_FILE"

    EXIT_CODE=${PIPESTATUS[0]}
    [ $EXIT_CODE -gt $OVERALL_EXIT ] && OVERALL_EXIT=$EXIT_CODE

    DEST_COUNT_AFTER=$(aws s3 ls "$DIR_DEST" --recursive 2>/dev/null | wc -l)
    UPLOADED=$((DEST_COUNT_AFTER - DEST_COUNT))
    log "[$DIR_INDEX/${#SOURCE_DIRS[@]}] Done. Uploaded: $UPLOADED | Total in dest: $DEST_COUNT_AFTER/$SOURCE_COUNT | Exit: $EXIT_CODE"
done

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
HOURS=$((ELAPSED / 3600))
MINUTES=$(((ELAPSED % 3600) / 60))
SECS=$((ELAPSED % 60))

log "All done. Total duration: ${HOURS}h${MINUTES}m${SECS}s | Overall exit: $OVERALL_EXIT"

exit "$OVERALL_EXIT"