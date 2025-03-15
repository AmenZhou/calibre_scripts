DEST_DIR="./"
FAILED_DIR="./failed"
SUCCESS_DIR="./success"
LOG_FILE="failed_additions.log"

BATCH_SIZE=5  # Number of books per batch
TIMEOUT_DURATION=60 
mkdir -p "$DEST_DIR" "$FAILED_DIR" "$SUCCESS_DIR"

mapfile -t book_list < <(find "$DEST_DIR" -type f ! -iname "*.htm" ! -iname "*.lit" ! -iname "*.zip")

echo "[INFO] Found ${#book_list[@]} files to process."

# Process books in batches
for ((i=0; i<${#book_list[@]}; i+=BATCH_SIZE)); do
    echo "[INFO] Processing batch $((i / BATCH_SIZE + 1))..."
    batch=("${book_list[@]:i:BATCH_SIZE}")  # Select the next batch of books

    # Add books in bulk with a timeout and error logging
    if printf "%s\0" "${batch[@]}" | timeout "$TIMEOUT_DURATION"s xargs -0 calibredb add --recurse 2>> "$LOG_FILE"; then
        echo "[SUCCESS] Batch $((i / BATCH_SIZE + 1)) added successfully!"
        # Move successfully added books to SUCCESS_DIR
        for book in "${batch[@]}"; do
            mv "$book" "$SUCCESS_DIR/" 2>/dev/null
        done
    else
        echo "[ERROR] Batch $((i / BATCH_SIZE + 1)) failed. Moving files to failed directory."
        for book in "${batch[@]}"; do
            mv "$book" "$FAILED_DIR/" 2>/dev/null
        done
    fi

    echo "[INFO] Batch $((i / BATCH_SIZE + 1)) completed."
done
