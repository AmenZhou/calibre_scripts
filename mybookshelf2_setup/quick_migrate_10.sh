#!/bin/bash
# Quick migration of 10 books from Calibre to MyBookshelf2
# Converts FB2 and other formats to EPUB before uploading

# Use docker without sudo if possible
if groups | grep -q docker; then
    DOCKER_CMD="docker"
else
    DOCKER_CMD="sudo docker"
    echo "Note: Using sudo for docker commands"
fi

CALIBRE_LIB="/home/haimengzhou/compressed-pilimi-zlib-120000-419999"
MBS2_CONTAINER="mybookshelf2_app"
EBOOK_CONVERT="/usr/bin/ebook-convert"
EBOOK_META="/usr/bin/ebook-meta"
TEMP_DIR="/tmp/mbs2_migration_$$"
COUNT=0
LIMIT=10

# Create temp directory
mkdir -p "$TEMP_DIR"

# Cleanup function
cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

# Function to delete all books from MyBookshelf2
delete_all_books() {
    echo "Deleting all existing books from MyBookshelf2..."
    $DOCKER_CMD exec "$MBS2_CONTAINER" python3 << 'PYEOF'
import sys
sys.path.insert(0, '/code')
import os
os.chdir('/code')
from app import app, db
import app.model as model

with app.app_context():
    from sqlalchemy import text
    count_before = db.session.execute(text("SELECT COUNT(*) FROM ebook")).scalar()
    print(f"Found {count_before} ebooks to delete...")
    sys.stdout.flush()
    
    if count_before == 0:
        print("No books to delete.")
    else:
        # Delete in correct order to handle foreign key constraints:
        # 1. Delete conversions (references source)
        # 2. Delete sources (references ebook)
        # 3. Delete ebooks
        db.session.execute(text("DELETE FROM conversion"))
        db.session.execute(text("DELETE FROM source"))
        db.session.execute(text("DELETE FROM ebook"))
        db.session.commit()
        
        count_after = db.session.execute(text("SELECT COUNT(*) FROM ebook")).scalar()
        print(f"Deleted {count_before} ebooks. Remaining: {count_after}")
        sys.stdout.flush()
PYEOF
    echo ""
}

# Delete all existing books first
delete_all_books

echo "Migrating 10 books from Calibre to MyBookshelf2 (with format conversion)..."
echo ""

# Find book files and process them
FILES=($(find "$CALIBRE_LIB" -type f \( -iname "*.epub" -o -iname "*.mobi" -o -iname "*.pdf" -o -iname "*.fb2" \) | head -n $LIMIT))

for FILE in "${FILES[@]}"; do
    if [ -f "$FILE" ]; then
        COUNT=$((COUNT+1))
        FILENAME=$(basename "$FILE")
        FILE_EXT="${FILENAME##*.}"
        FILE_EXT_LOWER=$(echo "$FILE_EXT" | tr '[:upper:]' '[:lower:]')
        
        echo "[$COUNT/$LIMIT] Processing: $FILENAME"
        
        UPLOAD_FILE=""
        IS_TEMP=false
        
        # Check if file needs conversion to EPUB
        if [ "$FILE_EXT_LOWER" != "epub" ]; then
            echo "  Converting $FILE_EXT to EPUB..."
            EPUB_FILE="$TEMP_DIR/${FILENAME%.*}.epub"
            
            # Convert to EPUB
            if $EBOOK_CONVERT "$FILE" "$EPUB_FILE" 2>/dev/null; then
                if [ -f "$EPUB_FILE" ]; then
                    UPLOAD_FILE="$EPUB_FILE"
                    IS_TEMP=true
                    echo "  ✓ Converted to EPUB: $(basename "$EPUB_FILE")"
                else
                    echo "  ✗ Conversion failed: EPUB file not created"
                    continue
                fi
            else
                echo "  ✗ Conversion failed: ebook-convert error"
                continue
            fi
        else
            # EPUB file - use directly
            UPLOAD_FILE="$FILE"
            IS_TEMP=false
        fi
        
        # Extract metadata from the file (try original first, then converted)
        echo "  Extracting metadata..."
        META_TITLE=""
        META_AUTHOR=""
        META_LANGUAGE=""
        META_SERIES=""
        META_SERIES_INDEX=""
        
        # Try extracting from original file first (if it was converted)
        META_SOURCE="$UPLOAD_FILE"
        if [ "$IS_TEMP" = true ] && [ -f "$FILE" ]; then
            META_SOURCE="$FILE"
        fi
        
        META_OUTPUT=$($EBOOK_META "$META_SOURCE" 2>/dev/null)
        if [ $? -eq 0 ]; then
            META_TITLE=$(echo "$META_OUTPUT" | grep "^Title:" | cut -d: -f2- | sed 's/^[[:space:]]*//' | head -1)
            META_AUTHOR=$(echo "$META_OUTPUT" | grep "^Author(s):" | cut -d: -f2- | sed 's/^[[:space:]]*//' | head -1)
            META_LANGUAGE=$(echo "$META_OUTPUT" | grep "^Language:" | cut -d: -f2- | sed 's/^[[:space:]]*//' | tr '[:upper:]' '[:lower:]' | head -1)
            META_SERIES=$(echo "$META_OUTPUT" | grep "^Series:" | cut -d: -f2- | sed 's/^[[:space:]]*//' | head -1)
            META_SERIES_INDEX=$(echo "$META_OUTPUT" | grep "^Series Index:" | cut -d: -f2- | sed 's/^[[:space:]]*//' | head -1)
        fi
        
        # If metadata is incomplete, try extracting from converted EPUB
        if [ -z "$META_TITLE" ] || [ -z "$META_LANGUAGE" ]; then
            if [ "$IS_TEMP" = true ] && [ -f "$UPLOAD_FILE" ]; then
                META_OUTPUT2=$($EBOOK_META "$UPLOAD_FILE" 2>/dev/null)
                if [ $? -eq 0 ]; then
                    [ -z "$META_TITLE" ] && META_TITLE=$(echo "$META_OUTPUT2" | grep "^Title:" | cut -d: -f2- | sed 's/^[[:space:]]*//' | head -1)
                    [ -z "$META_AUTHOR" ] && META_AUTHOR=$(echo "$META_OUTPUT2" | grep "^Author(s):" | cut -d: -f2- | sed 's/^[[:space:]]*//' | head -1)
                    [ -z "$META_LANGUAGE" ] && META_LANGUAGE=$(echo "$META_OUTPUT2" | grep "^Language:" | cut -d: -f2- | sed 's/^[[:space:]]*//' | tr '[:upper:]' '[:lower:]' | head -1)
                    [ -z "$META_SERIES" ] && META_SERIES=$(echo "$META_OUTPUT2" | grep "^Series:" | cut -d: -f2- | sed 's/^[[:space:]]*//' | head -1)
                    [ -z "$META_SERIES_INDEX" ] && META_SERIES_INDEX=$(echo "$META_OUTPUT2" | grep "^Series Index:" | cut -d: -f2- | sed 's/^[[:space:]]*//' | head -1)
                fi
            fi
        fi
        
        # Fix language code (rus -> ru)
        if [ "$META_LANGUAGE" = "rus" ]; then
            META_LANGUAGE="ru"
        fi
        
        # If still no language, default to 'ru' for Russian books
        if [ -z "$META_LANGUAGE" ]; then
            META_LANGUAGE="ru"
        fi
        
        # Debug: show extracted metadata
        if [ -n "$META_TITLE" ] || [ -n "$META_LANGUAGE" ]; then
            echo "    Title: ${META_TITLE:-'(none)'}, Language: ${META_LANGUAGE:-'(none)'}"
        fi
        
        # Copy file to container
        CONTAINER_FILE="/tmp/$(basename "$UPLOAD_FILE")"
        echo "  Copying to container..."
        $DOCKER_CMD cp "$UPLOAD_FILE" "$MBS2_CONTAINER:$CONTAINER_FILE"
        
        # Build upload command with metadata
        UPLOAD_CMD="python3 cli/mbs2.py -u admin -p mypassword123 --ws-url ws://mybookshelf2_backend:8080/ws upload --file $CONTAINER_FILE"
        
        if [ -n "$META_TITLE" ]; then
            UPLOAD_CMD="$UPLOAD_CMD --title \"$META_TITLE\""
        fi
        
        if [ -n "$META_AUTHOR" ]; then
            # Handle multiple authors separated by &
            IFS='&' read -ra AUTHORS <<< "$META_AUTHOR"
            for author in "${AUTHORS[@]}"; do
                author=$(echo "$author" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                if [ -n "$author" ]; then
                    UPLOAD_CMD="$UPLOAD_CMD --author \"$author\""
                fi
            done
        fi
        
        if [ -n "$META_LANGUAGE" ]; then
            UPLOAD_CMD="$UPLOAD_CMD --language $META_LANGUAGE"
        fi
        
        if [ -n "$META_SERIES" ]; then
            UPLOAD_CMD="$UPLOAD_CMD --series \"$META_SERIES\""
            if [ -n "$META_SERIES_INDEX" ]; then
                UPLOAD_CMD="$UPLOAD_CMD --series-index $META_SERIES_INDEX"
            fi
        fi
        
        # Upload to MyBookshelf2
        echo "  Uploading..."
        UPLOAD_OUTPUT=$(eval $DOCKER_CMD exec "$MBS2_CONTAINER" $UPLOAD_CMD 2>&1)
        UPLOAD_EXIT=$?
        
        if [ $UPLOAD_EXIT -eq 0 ] && echo "$UPLOAD_OUTPUT" | grep -q "Done\|Added file"; then
            echo "  ✓ Uploaded successfully"
        else
            echo "  ✗ Upload failed"
            echo "$UPLOAD_OUTPUT" | grep -i "error\|failed\|exception" | head -3
        fi
        
        # Cleanup container file
        $DOCKER_CMD exec "$MBS2_CONTAINER" rm -f "$CONTAINER_FILE"
        
        # Cleanup temp converted file
        if [ "$IS_TEMP" = true ] && [ -f "$UPLOAD_FILE" ]; then
            rm -f "$UPLOAD_FILE"
        fi
        
        echo ""
    fi
done

echo "✅ Migration complete! Processed $COUNT books."
echo "Access MyBookshelf2 at: http://localhost:5000"

