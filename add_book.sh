#!/usr/bin/env bash

# Define directories
DEST_DIR="./"
FAILED_DIR="./failed"
SUCCESS_DIR="./success"
LOG_FILE="failed_additions.log"
mkdir -p "$DEST_DIR" "$FAILED_DIR" "$SUCCESS_DIR"

BATCH_SIZE=20  # Number of books per batch
TIMEOUT_DURATION=60  # Timeout in seconds per batch

echo "===== Processing Started: Renaming and Importing Books ====="

# Step 1: Rename .rar files to .cbr without extracting
find "$DEST_DIR" -type f ! -path "$SUCCESS_DIR/*" | while IFS= read -r f; do
    if file --mime-type "$f" | grep -E "application/x-rar|application/vnd.rar"; then
        new_name="${f%.rar}.cbr"
        if [[ ! -f "$new_name" ]]; then
            mv "$f" "$new_name"
            echo "[INFO] Renamed '$f' to '$new_name' (RAR to CBR)"
        else
            echo "[WARNING] Skipped renaming '$f' as '$new_name' already exists"
        fi
    fi

    if file --mime-type "$f" | grep -q "application/zip"; then
        # Ensure renaming avoids double extensions
        if [[ "$f" == *.zip ]]; then
            new_name="${f%.zip}.cbz"
        else
            new_name="$f.cbz"
        fi
        
        # Rename file to .cbz only if it is not already .cbz
        if [[ "$f" != *.cbz ]]; then
            mv -n "$f" "$new_name"
            echo "[INFO] Renamed '$f' to '$new_name' (ZIP to CBZ)"
            f="$new_name"  # Update variable after renaming
        fi
    fi
done

echo "===== RAR and ZIP Files Renamed Successfully ====="

# Step 2: Rename extracted and existing files based on detected format
IFS=$'\n'
find "$DEST_DIR" -type f ! -path "$SUCCESS_DIR/*" | while IFS= read -r f; do
  if [[ -f "$f" ]]; then
    type=$(file -b "$f")
    filename="$(dirname "$f")/$(basename "$f" | sed 's/\.[^.]*$//')"

    if [[ $type == *"PDF document"* ]]; then
      mv "$f" "$filename.pdf"
      echo "[INFO] Renamed '$f' to '$filename.pdf'"

    elif [[ $type == *"EPUB document"* ]]; then
      mv "$f" "$filename.epub"
      echo "[INFO] Renamed '$f' to '$filename.epub'"

    elif [[ $type == *"Mobipocket E-book"* ]]; then
      if [[ "$f" == *.prc ]]; then
        mv "$f" "$filename.prc"
        echo "[INFO] Renamed '$f' to '$filename.prc' (Detected as PRC format)"
      else
        mv "$f" "$filename.mobi"
        echo "[INFO] Renamed '$f' to '$filename.mobi' (Detected as MOBI format)"
      fi

    elif [[ $type == *"FictionBook document"* ]] || [[ $type == *"application/x-fictionbook+xml"* ]]; then
      mv "$f" "$filename.fb2"
      echo "[INFO] Renamed '$f' to '$filename.fb2'"

    else
      echo "[WARNING] Skipped '$f' (Unknown format)"
    fi
  fi
done
IFS=$' \t\n'

echo "===== File Renaming Completed. Proceeding with Book Import. ====="

# Step 3: Process books in batches and add them to Calibre, excluding already imported books
mapfile -t book_list < <(find "$DEST_DIR" -type f \(
    -iname "*.pdf" -o \
    -iname "*.epub" -o \
    -iname "*.mobi" -o \
    -iname "*.azw3" -o \
    -iname "*.fb2" -o \
    -iname "*.cbz" -o \
    -iname "*.cbr" \) \
    -not -path "$SUCCESS_DIR/*")
    
echo "[INFO] Found ${#book_list[@]} ebook files to process."

# Process books in batches
for ((i=0; i<${#book_list[@]}; i+=BATCH_SIZE)); do
    echo "[INFO] Processing batch $((i / BATCH_SIZE + 1))..."
    batch=("${book_list[@]:i:BATCH_SIZE}")  # Select next batch of books

    # Add books in bulk with a timeout and error logging
    if printf "%s\0" "${batch[@]}" | timeout "$TIMEOUT_DURATION"s xargs -0 calibredb add --recurse 2>> "$LOG_FILE"; then
        echo "[SUCCESS] Batch $((i / BATCH_SIZE + 1)) added successfully!"
        # Move successfully added books to SUCCESS_DIR
        for book in "${batch[@]}"; do
            mv "$book" "$SUCCESS_DIR/" 2>/dev/null
        done
    else
        echo "[ERROR] Batch $((i / BATCH_SIZE + 1)) failed. Moving files to FAILED_DIR."
        for book in "${batch[@]}"; do
            mv "$book" "$FAILED_DIR/" 2>/dev/null
        done
    fi

    echo "[INFO] Batch $((i / BATCH_SIZE + 1)) completed."
done

# Reset IFS to default
unset IFS

echo "===== Book Import Process Completed Successfully ====="
