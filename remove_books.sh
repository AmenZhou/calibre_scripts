#!/usr/bin/env bash

# Define the search query for invalid formats
SEARCH_QUERY="not format:PDF and not format:EPUB and not format:MOBI and not format:AZW3 and not format:FB2 and not format:CBZ and not format:CBR"

# Fetch the list of book IDs matching the search query
mapfile -t book_ids < <(calibredb list --search "$SEARCH_QUERY" --fields id | awk 'NR>1 {print $1}')

# Check if any books were found
if [[ ${#book_ids[@]} -eq 0 ]]; then
    echo "[INFO] No books found matching the criteria."
    exit 0
fi

echo "[INFO] Found ${#book_ids[@]} books to delete."

# Loop through each book ID and delete one by one
for book_id in "${book_ids[@]}"; do
    echo "[INFO] Deleting book ID: $book_id"
    calibredb remove "$book_id"
done

echo "[INFO] All invalid books have been deleted."
