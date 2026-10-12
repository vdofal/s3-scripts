#!/bin/bash
# list-upload-dates.sh — for each file in a local folder, fetch its S3 upload date
# and write a sorted list (oldest first) to a TSV file.
#
# Usage:
#   ./list-upload-dates.sh <local_dir> <s3_destination> [output_file]
#
# Examples:
#   ./list-upload-dates.sh /d/LN18201 s3://spot-client-data/raw-data/LN18201
#   ./list-upload-dates.sh /d/LN18201 s3://spot-client-data/raw-data/LN18201 dates.tsv

set -u

LOCAL_DIR="${1:-}"
S3_DEST="${2:-}"
OUTPUT_FILE="${3:-upload-dates.tsv}"

AWS_PROFILE_NAME="${AWS_PROFILE:-playground-key}"
export AWS_PROFILE="$AWS_PROFILE_NAME"

if [ -z "$LOCAL_DIR" ] || [ -z "$S3_DEST" ]; then
    echo "Usage: $0 <local_dir> <s3_destination> [output_file]"
    echo "Example: $0 /d/LN18201 s3://spot-client-data/raw-data/LN18201 dates.tsv"
    exit 1
fi

if [ ! -d "$LOCAL_DIR" ]; then
    echo "ERROR: Local directory not found: $LOCAL_DIR"
    exit 1
fi

# Normalize: strip trailing slash from S3_DEST so we can append cleanly
S3_DEST="${S3_DEST%/}"

echo "Listing S3 objects under $S3_DEST/ ..."
# Fetch the entire S3 listing once (much faster than head-object per file)
# Output: date<SPACE>time<SPACE>size<SPACE>key
S3_LISTING=$(mktemp)
aws s3 ls "$S3_DEST/" --recursive > "$S3_LISTING" 2>/dev/null

if [ ! -s "$S3_LISTING" ]; then
    echo "WARNING: S3 listing is empty for $S3_DEST/"
fi

# Build a lookup: full_key -> "date time"
# `aws s3 ls --recursive` outputs lines like:
#   2026-05-19 22:34:51   83968000000 raw-data/LN18201/file.sgy
S3_KEYS_TMP=$(mktemp)
awk '{ date=$1; time=$2; $1=$2=$3=""; sub(/^   /,""); print $0 "\t" date " " time }' "$S3_LISTING" > "$S3_KEYS_TMP"

# Derive the S3 key prefix to strip when matching local filenames
# E.g., s3://spot-client-data/raw-data/LN18201 → "raw-data/LN18201/"
S3_PREFIX="${S3_DEST#s3://*/}/"

echo "Scanning local files in $LOCAL_DIR ..."
LOCAL_TMP=$(mktemp)
( cd "$LOCAL_DIR" && find . -type f -printf '%P\n' ) | sort > "$LOCAL_TMP"
LOCAL_COUNT=$(wc -l < "$LOCAL_TMP")
echo "Found $LOCAL_COUNT local files."

# For each local file, look up its S3 timestamp.
# Output format: <date> <time><TAB><filename>
RESULTS_TMP=$(mktemp)
MISSING=0
while IFS= read -r relpath; do
    fullkey="${S3_PREFIX}${relpath}"
    # Find the line in S3_KEYS_TMP where col 1 == fullkey
    match=$(awk -F'\t' -v k="$fullkey" '$1 == k { print $2; exit }' "$S3_KEYS_TMP")
    if [ -z "$match" ]; then
        echo "MISSING-IN-S3	$relpath" >> "$RESULTS_TMP"
        MISSING=$((MISSING + 1))
    else
        echo "${match}	${relpath}" >> "$RESULTS_TMP"
    fi
done < "$LOCAL_TMP"

# Sort: oldest first. "MISSING-IN-S3" lines sort to bottom because of the 'M' prefix
# being later in ASCII than digit-leading dates; force them to the end explicitly.
{
    grep -v '^MISSING-IN-S3' "$RESULTS_TMP" | sort
    grep '^MISSING-IN-S3' "$RESULTS_TMP" | sort
} > "$OUTPUT_FILE"

FOUND=$((LOCAL_COUNT - MISSING))
echo ""
echo "Done. Wrote $OUTPUT_FILE"
echo "  Found in S3:        $FOUND"
echo "  Missing from S3:    $MISSING"
echo "  Sorted oldest-first (missing files listed at the end)."

rm -f "$S3_LISTING" "$S3_KEYS_TMP" "$LOCAL_TMP" "$RESULTS_TMP"