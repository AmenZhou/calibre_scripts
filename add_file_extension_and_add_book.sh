#!/usr/bin/env bash

# Define directories
DEST_DIR="./"
FAILED_DIR="./failed"
SUCCESS_DIR="./success"
LOG_FILE="failed_additions.log"
mkdir -p "$DEST_DIR" "$FAILED_DIR" "$SUCCESS_DIR"

BATCH_SIZE=100  # Number of books per batch
TIMEOUT_DURATION=300  # Timeout in seconds per batch

###############################################################################
echo "===== Processing Started: Renaming and Importing Books ====="

###############################################################################
# Step 1: Rename .rar files to .cbr without extracting
find "$DEST_DIR" -type f ! -path "$SUCCESS_DIR/*" | while IFS= read -r f; do
    # Check for RAR (including uppercase)
    if file --mime-type "$f" | grep -Eiq "application/x-rar|application/vnd.rar"; then
        # If it does not already have .cbr or .CBR
        if [[ ! "$f" =~ \.(cbr|CBR)$ ]]; then
            new_name="${f%.*}.cbr"  # Remove .rar/.RAR then append .cbr
            if [[ ! -f "$new_name" ]]; then
                mv "$f" "$new_name"
                echo "[INFO] Renamed '$f' -> '$new_name' (RAR to CBR)"
            else
                echo "[WARNING] Skipped renaming '$f' as '$new_name' already exists"
            fi
        fi
    fi

    # Check for ZIP (including uppercase)
    if file --mime-type "$f" | grep -iq "application/zip"; then
        # If .ZIP or no extension, rename to .cbz
        if [[ "$f" =~ \.(zip|ZIP)$ ]]; then
            new_name="${f%.*}.cbz"
        else
            new_name="$f.cbz"
        fi
        # Rename only if not .cbz or .CBZ
        if [[ ! "$f" =~ \.(cbz|CBZ)$ ]]; then
            mv -n "$f" "$new_name"
            echo "[INFO] Renamed '$f' -> '$new_name' (ZIP to CBZ)"
            f="$new_name"  # Update variable after renaming
        fi
    fi

done

echo "===== RAR and ZIP Files Renamed Successfully ====="

###############################################################################
# Step 2: Rename existing files based on detected format
IFS=$'\n'
find "$DEST_DIR" -type f ! -path "$SUCCESS_DIR/*" | while IFS= read -r f; do
  if [[ -f "$f" ]]; then
    type=$(file -b "$f")
    filename="$(dirname "$f")/$(basename "$f" | sed 's/\.[^.]*$//')"

    # PDF check (ignore .PDF uppercase if it's correct extension)
    if [[ ! "$f" =~ \.(pdf|PDF)$ && $type == *"PDF document"* ]]; then
      mv "$f" "$filename.pdf"
      echo "[INFO] Renamed '$f' -> '$filename.pdf' (Detected PDF)"

    # EPUB check
    elif [[ ! "$f" =~ \.(epub|EPUB)$ && $type == *"EPUB document"* ]]; then
      mv "$f" "$filename.epub"
      echo "[INFO] Renamed '$f' -> '$filename.epub' (Detected EPUB)"

    # MOBI / PRC check
    elif [[ $type == *"Mobipocket E-book"* ]]; then
      # If file is .prc or .PRC
      if [[ "$f" =~ \.(prc|PRC)$ ]]; then
        mv "$f" "$filename.prc"
        echo "[INFO] Renamed '$f' -> '$filename.prc' (Detected PRC)"
      elif [[ ! "$f" =~ \.(mobi|MOBI)$ ]]; then
        mv "$f" "$filename.mobi"
        echo "[INFO] Renamed '$f' -> '$filename.mobi' (Detected MOBI)"
      fi

    # FictionBook checks
    elif [[ ! "$f" =~ \.(fb2|FB2)$ && ($type == *"FictionBook document"* || $type == *"application/x-fictionbook+xml"*) ]]; then
      mv "$f" "$filename.fb2"
      echo "[INFO] Renamed '$f' -> '$filename.fb2' (Detected FictionBook)"
    elif grep -iq "<FictionBook" "$f"; then
      # If we find the FictionBook XML marker
      if [[ ! "$f" =~ \.(fb2|FB2)$ ]]; then
        mv "$f" "$filename.fb2"
        echo "[INFO] Renamed '$f' -> '$filename.fb2' (Detected FictionBook XML)"
      fi

    else
      echo "[WARNING] Skipped '$f' (Unknown format)"
    fi
  fi
done
IFS=$' \t\n'

echo "===== File Renaming Completed. Proceeding with Book Import. ====="

###############################################################################
# Step 3: Process books in batches and add them to Calibre

###############################################################################
# Step 3: Process books in batches and add them to Calibre
# Step 3: Process books in batches and add them to Calibre
# #!/usr/bin/env bash

# Define directories
DEST_DIR="./"
FAILED_DIR="./failed"
SUCCESS_DIR="./success"
LOG_FILE="failed_additions.log"
mkdir -p "$DEST_DIR" "$FAILED_DIR" "$SUCCESS_DIR"

BATCH_SIZE=100  # Number of books per batch
TIMEOUT_DURATION=300  # Timeout in seconds per batch

###############################################################################
echo "===== Processing Started: Renaming and Importing Books ====="

###############################################################################

###############################################################################
# Step 3: Process books in batches and add them to Calibre

###############################################################################
# Step 3: Process books in batches and add them to Calibre
mapfile -t book_list < <(find "$DEST_DIR" -type f \( -iname "*.pdf" -o -iname "*.epub" -o -iname "*.mobi" -o -iname "*.azw3" -o -iname "*.fb2" -o -iname "*.cbz" -o -iname "*.cbr" \) -not -path "$SUCCESS_DIR/*")

echo "[INFO] Found ${#book_list[@]} ebook files to process."

consecutive_failures=0

for ((i=0; i<${#book_list[@]}; i+=BATCH_SIZE)); do
    echo "[INFO] Processing batch $((i / BATCH_SIZE + 1))..."
    batch=("${book_list[@]:i:BATCH_SIZE}")

    # Add in bulk with a timeout + error logging
    if printf "%s\\0" "${batch[@]}" | timeout "$TIMEOUT_DURATION"s xargs -0 calibredb add --recurse 2>> "$LOG_FILE"; then
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
