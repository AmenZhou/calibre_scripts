#!/usr/bin/env bash

# Define the search query for invalid formats
SEARCH_QUERY="not format:PDF and not format:EPUB and not format:MOBI and not format:AZW3 and not format:FB2 and not format:CBZ and not format:CBR and not format:LIT"

# Fetch the list of book IDs matching the search query
mapfile -t book_ids < <(calibredb list --search "$SEARCH_QUERY" --fields id | awk 'NR>1 {print $1}')

# Check if any books were found
if [[ ${#book_ids[@]} -eq 0 ]]; then
    echo "[INFO] No books found matching the criteria."
    exit 0
fi

echo "[INFO] Found ${#book_ids[@]} books to delete."

# Delete books in batches for better performance
BATCH_SIZE=10
for ((i=0; i<${#book_ids[@]}; i+=BATCH_SIZE)); do
    batch=("${book_ids[@]:i:BATCH_SIZE}")
    echo "[INFO] Deleting batch $((i / BATCH_SIZE + 1))..."
    calibredb remove "${batch[@]}"
done

echo "[INFO] All invalid books have been deleted."
