#!/usr/bin/env bash

# Define directories
DEST_DIR="./"
FAILED_DIR="./failed"
SUCCESS_DIR="./success"
LOG_FILE="failed_additions.log"
mkdir -p "$DEST_DIR" "$FAILED_DIR" "$SUCCESS_DIR"

BATCH_SIZE=100  # Number of books per batch
TIMEOUT_DURATION=300  # Timeout in seconds per batch

echo "===== Processing Started: Renaming and Importing Books ====="

###############################################################################
# Step 1 & 2: Rename .rar/.zip and other files based on detected format
IFS=$'\n'
find "$DEST_DIR" -type f ! -path "$SUCCESS_DIR/*" | while IFS= read -r f; do
    if [[ -f "$f" ]]; then
        filename="$(dirname "$f")/$(basename "$f" | sed 's/\.[^.]*$//')"
        type=$(file --mime-type "$f")
        filetype=$(file -b "$f")

        # RAR to CBR renaming
        if $type | grep -Eiq "application/x-rar|application/vnd.rar"; then
            if [[ ! "$f" =~ \.(cbr|CBR)$ ]]; then
                new_name="${f%.*}.cbr"
                if [[ ! -f "$new_name" ]]; then
                    mv "$f" "$new_name"
                    echo "[INFO] Renamed '$f' -> '$new_name' (RAR to CBR)"
                    f="$new_name" # Update filename
                else
                    echo "[WARNING] Skipped renaming '$f' as '$new_name' already exists"
                fi
            fi
        # ZIP to CBZ renaming
        elif $type | grep -iq "application/zip"; then
            if [[ "$f" =~ \.(zip|ZIP)$ ]]; then
                new_name="${f%.*}.cbz"
            else
                new_name="$f.cbz"
            fi
            if [[ ! "$f" =~ \.(cbz|CBZ)$ ]]; then
                mv -n "$f" "$new_name"
                echo "[INFO] Renamed '$f' -> '$new_name' (ZIP to CBZ)"
                f="$new_name" # Update filename
            fi
        # PDF check
        elif [[ ! "$f" =~ \.(pdf|PDF)$ && $filetype == *"PDF document"* ]]; then
            mv "$f" "$filename.pdf"
            echo "[INFO] Renamed '$f' -> '$filename.pdf' (Detected PDF)"
        # EPUB check
        elif [[ ! "$f" =~ \.(epub|EPUB)$ && $filetype == *"EPUB document"* ]]; then
            mv "$f" "$filename.epub"
            echo "[INFO] Renamed '$f' -> '$filename.epub' (Detected EPUB)"
        # MOBI / PRC check
        elif [[ $filetype == *"Mobipocket E-book"* ]]; then
            if [[ "$f" =~ \.(prc|PRC)$ ]]; then
                mv "$f" "$filename.prc"
                echo "[INFO] Renamed '$f' -> '$filename.prc' (Detected PRC)"
            elif [[ ! "$f" =~ \.(mobi|MOBI)$ ]]; then
                mv "$f" "$filename.mobi"
                echo "[INFO] Renamed '$f' -> '$filename.mobi' (Detected MOBI)"
            fi
        # BBeB ebook data check
        elif [[ ! "$f" =~ \.(bbe|BBE)$ && $filetype == *"BBeB ebook data, unencrypted, version -6397, front-to-back"* ]]; then
            mv "$f" "$filename.bbe"
            echo "[INFO] Renamed '$f' -> '$filename.bbe' (Detected BBeB ebook data)"
        # DjVu check
        elif [[ $filetype == *"DjVu multiple page document"* ]]; then
            if [[ ! "$f" =~ \.(djvu|DJVU)$ ]]; then
                mv "$f" "$filename.djvu"
                echo "[INFO] Renamed '$f' -> '$filename.djvu' (Detected DjVu multiple page document)"
            fi
        # Microsoft Reader eBook Data check
        elif [[ ! "$f" =~ \.(lit|LIT)$ && $filetype == *"Microsoft Reader eBook Data, version 1"* ]]; then
            mv "$f" "$filename.lit"
            echo "[INFO] Renamed '$f' -> '$filename.lit' (Detected Microsoft Reader eBook)"
        # FictionBook checks
        elif [[ ! "$f" =~ \.(fb2|FB2)$ && ($filetype == *"FictionBook document"* || $filetype == *"application/x-fictionbook+xml"*) ]]; then
            mv "$f" "$filename.fb2"
            echo "[INFO] Renamed '$f' -> '$filename.fb2' (Detected FictionBook)"
        elif grep -iq "<FictionBook" "$f"; then
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
mapfile -t book_list < <(find "$DEST_DIR" -type f \( -iname "*.pdf" -o -iname "*.epub" -o -iname "*.mobi" -o -iname "*.azw3" -o -iname "*.fb2" -o -iname "*.cbz" -o -iname "*.cbr" -o -iname "*.bbe" -o -iname "*.djvu" -o -iname "*.lit" \) -not -path "$SUCCESS_DIR/*")

echo "[INFO] Found ${#book_list[@]} ebook files to process."

consecutive_failures=0

for ((i=0; i<${#book_list[@]}; i+=BATCH_SIZE)); do
    echo "[INFO] Processing batch $((i / BATCH_SIZE + 1))..."
    batch=("${book_list[@]:i:BATCH_SIZE}")

    if printf "%s\\0" "${batch[@]}" | timeout "$TIMEOUT_DURATION"s xargs -0 calibredb add --recurse 2>> "$LOG_FILE"; then
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
        if ((consecutive_failures >= 5)); then
            echo "[ERROR] Five consecutive batch failures detected. Terminating the script."
            kill $$
            exit 1
        fi
    fi

    echo "[INFO] Batch $((i / BATCH_SIZE + 1)) completed."
done

echo "[INFO] All batches processed."

unset IFS

echo "===== Book Import Process Completed Successfully ====="
