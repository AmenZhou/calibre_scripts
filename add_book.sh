#!/usr/bin/env bash

# Define directories
DEST_DIR="./"
FAILED_DIR="./failed"
SUCCESS_DIR="./success"
LOG_FILE="failed_additions.log"
mkdir -p "$DEST_DIR" "$FAILED_DIR" "$SUCCESS_DIR"

BATCH_SIZE=500  # Number of books per batch (increased for better performance)
TIMEOUT_DURATION=6000  # Timeout in seconds per batch (increased for larger batches)

###############################################################################
echo "===== Processing Started: Renaming and Importing Books ====="

###############################################################################

###############################################################################
# Step 3: Process books in batches and add them to Calibre

###############################################################################
# Step 3: Process books in batches and add them to Calibre
# Step 3: Process books in batches and add them to Calibre
mapfile -t book_list < <(find "$DEST_DIR" -type f \( -iname "*.pdf" -o -iname "*.epub" -o -iname "*.mobi" -o -iname "*.azw3" -o -iname "*.fb2" -o -iname "*.cbz" -o -iname "*.cbr" \) -not -path "$SUCCESS_DIR/*")

echo "[INFO] Found ${#book_list[@]} ebook files to process."

echo "[INFO] Duplicate detection is DISABLED. All files will be added to Calibre."
ADD_FLAGS="--recurse --duplicates"

consecutive_failures=0

for ((i=0; i<${#book_list[@]}; i+=BATCH_SIZE)); do
    echo "[INFO] Processing batch $((i / BATCH_SIZE + 1))..."
    batch=("${book_list[@]:i:BATCH_SIZE}")

    # Add in bulk with a timeout + error logging
    if printf "%s\\0" "${batch[@]}" | timeout "$TIMEOUT_DURATION"s xargs -0 calibredb add $ADD_FLAGS 2>> "$LOG_FILE"; then
        echo "[SUCCESS] Batch $((i / BATCH_SIZE + 1)) added successfully!"
        # Move successfully added books to SUCCESS_DIR
        for book in "${batch[@]}"; do
            mv "$book" "$SUCCESS_DIR/" 2>/dev/null
        done
        consecutive_failures=0 # Reset the counter on success
    else
        echo "[ERROR] Batch $((i / BATCH_SIZE + 1)) failed. Moving files to FAILED_DIR."
        for book in "${batch[@]}"; do
            mv "$book" "$FAILED_DIR/" 2>/dev/null
        done
        ((consecutive_failures++))
        echo "[ERROR] Consecutive failures: $consecutive_failures"
        if ((consecutive_failures >= 5)); then
            echo "[ERROR] Five consecutive batch failures detected. Terminating the script."
            kill $$ # Kill the current script process
            exit 1 # Exit with an error code
        fi
    fi

    echo "[INFO] Batch $((i / BATCH_SIZE + 1)) completed."
done

echo "[INFO] All batches processed."

unset IFS

echo "===== Book Import Process Completed Successfully ====="
