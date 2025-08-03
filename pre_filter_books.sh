#!/usr/bin/env bash

# Pre-filter books to remove obvious duplicates based on filename
# This runs much faster than Calibre's duplicate detection

SOURCE_DIR="./"
FILTERED_DIR="./filtered"
DUPLICATE_DIR="./likely_duplicates"

mkdir -p "$FILTERED_DIR" "$DUPLICATE_DIR"

echo "Pre-filtering books to remove obvious duplicates..."

# Create associative array to track seen titles
declare -A seen_titles

find "$SOURCE_DIR" -type f \( -iname "*.pdf" -o -iname "*.epub" -o -iname "*.mobi" -o -iname "*.azw3" -o -iname "*.fb2" -o -iname "*.cbz" -o -iname "*.cbr" \) -print0 | while IFS= read -r -d '' file; do
    # Extract basename and normalize (remove extension, lowercase, remove special chars)
    basename=$(basename "$file")
    normalized=$(echo "${basename%.*}" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]//g')
    
    if [[ -n "${seen_titles[$normalized]}" ]]; then
        # Likely duplicate
        mv "$file" "$DUPLICATE_DIR/"
        echo "Moved likely duplicate: $basename"
    else
        # First occurrence
        seen_titles[$normalized]=1
        mv "$file" "$FILTERED_DIR/"
    fi
done

echo "Pre-filtering complete. Check $DUPLICATE_DIR for potential duplicates." 