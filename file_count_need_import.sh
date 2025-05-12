#!/usr/bin/env bash

# Define directories
DEST_DIR="./"
SUCCESS_DIR="./success"

# Debug: Print current working directory
echo "Current working directory: $(pwd)"

# Debug: Check if DEST_DIR is readable
if [ ! -r "$DEST_DIR" ]; then
  echo "Error: Destination directory '$DEST_DIR' is not readable."
  exit 1
fi

# Count files with specific ebook formats using -print0
count=0
while IFS= read -r -d '' file; do
    ((count++))
done < <(find "$DEST_DIR" -type f \( -iname "*.pdf" -o -iname "*.epub" -o -iname "*.mobi" -o -iname "*.azw3" -o -iname "*.fb2" -o -iname "*.cbz" -o -iname "*.cbr" -o -iname "*.bbe" -o -iname "*.djvu" -o -iname "*.lit" \) -not -path "$SUCCESS_DIR/*" -print0)

# Print the result
echo "Number of files that need to be imported: $count"
