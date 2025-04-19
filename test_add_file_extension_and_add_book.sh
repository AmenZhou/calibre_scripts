#!/usr/bin/env bash

# Exit on error
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Test directory
TEST_DIR="test_env"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

# Function to print test results
print_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ $2${NC}"
    else
        echo -e "${RED}✗ $2${NC}"
        exit 1
    fi
}

# Create a mock calibredb command
mkdir -p "$TEST_DIR/bin"
cat > "$TEST_DIR/bin/calibredb" << 'EOF'
#!/bin/bash
echo "[MOCK] Would add books to Calibre"
exit 0
EOF
chmod +x "$TEST_DIR/bin/calibredb"
export PATH="$TEST_DIR/bin:$PATH"

# Test 1: Create test files with different formats
echo "Setting up test files..."

# Create test content
echo "test content" > "$TEST_DIR/content.txt"

# Create a RAR file with .rar extension first
if command -v rar &> /dev/null; then
    rar a "$TEST_DIR/test1.rar" "$TEST_DIR/content.txt" > /dev/null
    mv "$TEST_DIR/test1.rar" "$TEST_DIR/test1"
else
    # Create a minimal RAR file header
    {
        printf "Rar!\x1a\x07\x00"
        cat "$TEST_DIR/content.txt"
    } > "$TEST_DIR/test1"
fi

# Create a ZIP file with .zip extension first
if command -v zip &> /dev/null; then
    zip -q "$TEST_DIR/test2.zip" "$TEST_DIR/content.txt"
    mv "$TEST_DIR/test2.zip" "$TEST_DIR/test2"
else
    # Create a minimal ZIP file header
    {
        printf "PK\x03\x04"
        cat "$TEST_DIR/content.txt"
    } > "$TEST_DIR/test2"
fi

# Create a PDF file
cat > "$TEST_DIR/test3" << 'EOF'
%PDF-1.4
1 0 obj
<< /Type /Catalog
   /Pages 2 0 R
>>
endobj
2 0 obj
<< /Type /Pages
   /Kids [3 0 R]
   /Count 1
>>
endobj
EOF

# Create an EPUB file
# First create the EPUB structure
mkdir -p "$TEST_DIR/epub_temp/META-INF"
echo "application/epub+zip" > "$TEST_DIR/epub_temp/mimetype"
cat > "$TEST_DIR/epub_temp/META-INF/container.xml" << 'EOF'
<?xml version="1.0"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
EOF

# Create EPUB file using zip
if command -v zip &> /dev/null; then
    (
        cd "$TEST_DIR/epub_temp"
        zip -q -X "../test4.epub" mimetype
        zip -q -r "../test4.epub" META-INF
    )
    mv "$TEST_DIR/test4.epub" "$TEST_DIR/test4"
else
    # Create a minimal EPUB-like file
    cat "$TEST_DIR/epub_temp/mimetype" "$TEST_DIR/epub_temp/META-INF/container.xml" > "$TEST_DIR/test4"
fi
rm -rf "$TEST_DIR/epub_temp"

# Create a MOBI file
{
    # Write binary MOBI header
    printf "\x42\x4F\x4F\x4B\x4D\x4F\x42\x49" # "BOOKMOBI"
    printf "\x00\x00\x00\x01" # Version
    printf "\x00\x00\x00\x02" # Type
    printf "\x00\x00\x00\x00" # Creator
    printf "\x00\x00\x00\x00" # CreationDate
    printf "\x00\x00\x00\x00" # Backup
    printf "\x00\x00\x00\x00" # Modifiable
    printf "Test Book Content"
} > "$TEST_DIR/test5"

print_result $? "Test files created successfully"

# Test 2: Run the main script
echo "Running the main script..."
cp add_file_extension_and_add_book.sh "$TEST_DIR/"
# Copy the monitoring script to the test environment
cp "$SCRIPT_DIR/monitor_resources.sh" "$TEST_DIR/"
# Copy the rename_files script to the test environment
cp "$SCRIPT_DIR/rename_files.sh" "$TEST_DIR/"
cd "$TEST_DIR"
chmod +x add_file_extension_and_add_book.sh
chmod +x rename_files.sh

./add_file_extension_and_add_book.sh

# Test 3: Verify file extensions were changed correctly
echo "Verifying file extensions..."
check_file() {
    local name=$1
    local ext=$2
    local desc=$3
    if [ -f "$name.$ext" ] || [ -f "success/$name.$ext" ] || [ -f "failed/$name.$ext" ]; then
        print_result 0 "$desc file renamed to $ext"
    else
        print_result 1 "$desc file not renamed to $ext"
    fi
}

check_file "test1" "cbr" "RAR"
check_file "test2" "cbz" "ZIP"
check_file "test3" "pdf" "PDF"
check_file "test4" "epub" "EPUB"
check_file "test5" "mobi" "MOBI"

# Test 4: Verify directory structure
echo "Verifying directory structure..."
[ -d "success" ] && print_result 0 "Success directory created" || print_result 1 "Success directory not created"
[ -d "failed" ] && print_result 0 "Failed directory created" || print_result 1 "Failed directory not created"
[ -f "failed_additions.log" ] && print_result 0 "Log file created" || print_result 1 "Log file not created"

# Cleanup
cd ..
rm -rf "$TEST_DIR"

echo "All tests completed successfully!" 