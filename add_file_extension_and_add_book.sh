#!/usr/bin/env bash

# Define directories
DEST_DIR="./"
FAILED_DIR="./failed"
SUCCESS_DIR="./success"
LOG_FILE="failed_additions.log"
mkdir -p "$DEST_DIR" "$FAILED_DIR" "$SUCCESS_DIR"

BATCH_SIZE=100  # Number of books per batch
TIMEOUT_DURATION=300  # Timeout in seconds per batch
NUM_THREADS=100 # Number of parallel processes

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Optional monitoring setup
MONITOR_LOG="performance_monitor.log"
MONITOR_PID=""

# Check if monitoring script exists and source it if available
if [ -f "$SCRIPT_DIR/monitor_resources.sh" ]; then
    source "$SCRIPT_DIR/monitor_resources.sh"
    MONITORING_ENABLED=true
else
    MONITORING_ENABLED=false
    echo "[INFO] Monitoring script not found. Performance monitoring will be disabled."
fi

# Function to clean up monitoring on script exit
cleanup() {
    if [ "$MONITORING_ENABLED" = true ] && [ -n "$MONITOR_PID" ]; then
        kill "$MONITOR_PID" 2>/dev/null
        generate_performance_summary "$MONITOR_LOG"
    fi
}

# Set up trap to ensure cleanup runs on script exit
trap cleanup EXIT

echo "===== Processing Started: Renaming and Importing Books ====="

###############################################################################
# Step 1 & 2: Combined file processing for better performance
# First, collect all files that need processing
echo "Collecting files to process..."
files_to_process=()
while IFS= read -r -d '' f; do
    files_to_process+=("$f")
done < <(find "$DEST_DIR" -type f ! -path "$SUCCESS_DIR/*" ! -name "*.*" -print0)

echo "Processing ${#files_to_process[@]} files..."

# Check if rename_files.sh exists
if [ -f "$SCRIPT_DIR/rename_files.sh" ]; then
    echo "[INFO] Using rename_files.sh for file processing..."
    chmod +x "$SCRIPT_DIR/rename_files.sh"

    # Process files in parallel using xargs with rename_files.sh
    echo "Starting parallel processing with $NUM_THREADS threads..."
    printf '%s\0' "${files_to_process[@]}" | xargs -0 -P "$NUM_THREADS" -n 1 "$SCRIPT_DIR/rename_files.sh" | tee -a "$LOG_FILE"
else
    echo "[ERROR] rename_files.sh not found. Please create the file with the renaming logic."
fi

echo "===== File Renaming Completed. Proceeding with Book Import. ====="

###############################################################################
# Step 3: Process books in batches and add them to Calibre
books=()
while IFS= read -r -d '' book; do
    books+=("$book")
done < <(find "$DEST_DIR" -type f \( -iname "*.pdf" -o -iname "*.epub" -o -iname "*.mobi" -o -iname "*.azw3" -o -iname "*.fb2" -o -iname "*.cbz" -o -iname "*.cbr" -o -iname "*.bbe" -o -iname "*.djvu" -o -iname "*.lit" \) -not -path "$SUCCESS_DIR/*" -print0)

echo "[INFO] Found ${#books[@]} ebook files to process."

consecutive_failures=0

for ((i=0; i<${#books[@]}; i+=BATCH_SIZE)); do
    echo "[INFO] Processing batch $((i / BATCH_SIZE + 1))..."
    batch=("${books[@]:i:BATCH_SIZE}")

    if timeout "$TIMEOUT_DURATION"s calibredb add --recurse "${batch[@]}" 2>> "$LOG_FILE"; then
        echo "[SUCCESS] Batch $((i / BATCH_SIZE + 1)) added successfully!"
        for book in "${batch[@]}"; do
            mv "$book" "$SUCCESS_DIR/" 2>/dev/null
        done
        consecutive_failures=0
    else
        echo "[ERROR] Batch $((i / BATCH_SIZE + 1)) failed. Moving files to FAILED_DIR."
        for book in "${batch[@]}"; do
            mv "$book" "$FAILED_DIR/" 2>/dev/null
        done
        ((consecutive_failures++))
        echo "[ERROR] Consecutive failures: $consecutive_failures"
        # Run remove_books.sh after 5 consecutive failures
        if ((consecutive_failures >= 5)); then
            echo "[ERROR] Five consecutive batch failures detected."
            if [ -f "$SCRIPT_DIR/remove_books.sh" ]; then
                echo "[INFO] Running remove_books.sh..."
                "$SCRIPT_DIR/remove_books.sh"
                consecutive_failures=0  # Reset counter after cleanup
            else
                echo "[ERROR] remove_books.sh not found in $SCRIPT_DIR. Terminating the script."
                exit 1
            fi
        fi
    fi

    echo "[INFO] Batch $((i / BATCH_SIZE + 1)) completed."
done

echo "[INFO] All batches processed."

echo "===== Book Import Process Completed Successfully ====="
