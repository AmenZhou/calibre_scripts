#!/usr/bin/env bash

# Count files without extensions
count=$(find . -type f ! -name "*.*" ! -path "./.*" | wc -l)

# Print the result
echo "Number of files without extensions: $count"

# Optional: List the files without extensions
echo "Files without extensions:"
find . -type f ! -name "*.*" ! -path "./.*" 