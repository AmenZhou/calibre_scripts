#!/usr/bin/env bash

# Test script for uncompress_util.sh (batch processing mode)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counters
TESTS_RUN=0
TESTS_PASSED=0

# Get the directory of the current script and the utility script
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
UNCOMPRESS_SCRIPT_PATH="$SCRIPT_DIR/uncompress_util.sh"

_log_test_header() {
    echo -e "\n${YELLOW}--- $1 ---${NC}"
}

_assert_eq() {
    ((TESTS_RUN++))
    local actual="$1"
    local expected="$2"
    local message="$3"
    if [ "$actual" == "$expected" ]; then
        echo -e "${GREEN}✓ PASSED:${NC} $message (expected '$expected', got '$actual')"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗ FAILED:${NC} $message (expected '$expected', got '$actual')"
        return 1
    fi
}

_assert_file_exists() {
    ((TESTS_RUN++))
    local file_path="$1"
    local message="$2"
    if [ -f "$file_path" ]; then
        echo -e "${GREEN}✓ PASSED:${NC} $message (file '$file_path' exists)"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗ FAILED:${NC} $message (file '$file_path' does not exist)"
        return 1
    fi
}

_assert_dir_exists() {
    ((TESTS_RUN++))
    local dir_path="$1"
    local message="$2"
    if [ -d "$dir_path" ]; then
        echo -e "${GREEN}✓ PASSED:${NC} $message (directory '$dir_path' exists)"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗ FAILED:${NC} $message (directory '$dir_path' does not exist)"
        return 1
    fi
}

_assert_file_not_exists() {
    ((TESTS_RUN++))
    local file_path="$1"
    local message="$2"
    if [ ! -f "$file_path" ]; then
        echo -e "${GREEN}✓ PASSED:${NC} $message (file '$file_path' does not exist)"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗ FAILED:${NC} $message (file '$file_path' unexpectedly exists)"
        return 1
    fi
}

main() {
    echo "Starting tests for uncompress_util.sh (batch mode)"
    echo "Utility script path: $UNCOMPRESS_SCRIPT_PATH"
    echo "=================================================="

    if [ ! -f "$UNCOMPRESS_SCRIPT_PATH" ]; then
        echo -e "${RED}CRITICAL: uncompress_util.sh not found at $UNCOMPRESS_SCRIPT_PATH${NC}"
        exit 1
    fi

    TEST_ROOT_DIR=$(mktemp -d)
    echo "Test root directory: $TEST_ROOT_DIR"

    ORIGINAL_PWD=$(pwd)

    # --- Test Case 1: No tar files --- 
    _log_test_header "Test Case 1: No tar files in source directory"
    SOURCE_DIR_1="$TEST_ROOT_DIR/source_1_no_tars"
    mkdir -p "$SOURCE_DIR_1"
    cd "$SOURCE_DIR_1"
    echo "Current dir for test 1: $(pwd)"
    bash "$UNCOMPRESS_SCRIPT_PATH" # No arguments
    exit_code_1=$?
    _assert_eq "$exit_code_1" "0" "Script exits successfully with no tar files"
    _assert_dir_exists "./uncompressed_files" "Base 'uncompressed_files' directory created by script"
    _assert_dir_exists "./uncompressed_files/processed" "'processed' subdirectory created by script"
    _assert_eq "$(ls -A ./uncompressed_files/uncompressed_files 2>/dev/null | wc -l | xargs)" "0" "'uncompressed_files/uncompressed_files' is empty"
    cd "$ORIGINAL_PWD"

    # --- Test Case 2: Single valid tar file --- 
    _log_test_header "Test Case 2: Single valid tar file"
    SOURCE_DIR_2="$TEST_ROOT_DIR/source_2_single_tar"
    TAR_CONTENT_DIR_2="$SOURCE_DIR_2/content_for_tar1"
    mkdir -p "$SOURCE_DIR_2"
    mkdir -p "$TAR_CONTENT_DIR_2"
    echo "file1 from archive1" > "$TAR_CONTENT_DIR_2/file1.txt"
    echo "file2 from archive1" > "$TAR_CONTENT_DIR_2/file2.txt"
    cd "$TAR_CONTENT_DIR_2"
    tar -cf "../archive1.tar" . 
    cd "$SOURCE_DIR_2"
    rm -rf "$TAR_CONTENT_DIR_2"
    echo "Current dir for test 2: $(pwd)"
    bash "$UNCOMPRESS_SCRIPT_PATH"
    exit_code_2=$?
    _assert_eq "$exit_code_2" "0" "Script exits successfully for single tar"
    _assert_dir_exists "./uncompressed_files/uncompressed_files" "'uncompressed_files/uncompressed_files' dir exists"
    _assert_file_exists "./uncompressed_files/uncompressed_files/file1.txt" "file1.txt extracted"
    _assert_file_exists "./uncompressed_files/uncompressed_files/file2.txt" "file2.txt extracted"
    _assert_dir_exists "./uncompressed_files/processed" "'processed' dir exists"
    _assert_file_exists "./uncompressed_files/processed/archive1.tar" "archive1.tar moved to processed"
    _assert_file_not_exists "./archive1.tar" "archive1.tar removed from source"
    cd "$ORIGINAL_PWD"

    # --- Test Case 3: Multiple valid tar files --- 
    _log_test_header "Test Case 3: Multiple valid tar files"
    SOURCE_DIR_3="$TEST_ROOT_DIR/source_3_multiple_tars"
    mkdir -p "$SOURCE_DIR_3"
    # Tar 1
    TAR_CONTENT_DIR_3A="$SOURCE_DIR_3/content_A"
    mkdir -p "$TAR_CONTENT_DIR_3A"
    echo "alpha" > "$TAR_CONTENT_DIR_3A/alpha.txt"
    cd "$TAR_CONTENT_DIR_3A"
    tar -cf "../multi_archive1.tar" .
    cd "$SOURCE_DIR_3"
    rm -rf "$TAR_CONTENT_DIR_3A"
    # Tar 2
    TAR_CONTENT_DIR_3B="$SOURCE_DIR_3/content_B"
    mkdir -p "$TAR_CONTENT_DIR_3B"
    echo "beta" > "$TAR_CONTENT_DIR_3B/beta.txt"
    cd "$TAR_CONTENT_DIR_3B"
    tar -cf "../multi_archive2.tar" .
    cd "$SOURCE_DIR_3"
    rm -rf "$TAR_CONTENT_DIR_3B"

    echo "Current dir for test 3: $(pwd)"
    bash "$UNCOMPRESS_SCRIPT_PATH"
    exit_code_3=$?
    _assert_eq "$exit_code_3" "0" "Script exits successfully for multiple tars"
    _assert_file_exists "./uncompressed_files/uncompressed_files/alpha.txt" "alpha.txt extracted"
    _assert_file_exists "./uncompressed_files/uncompressed_files/beta.txt" "beta.txt extracted"
    _assert_file_exists "./uncompressed_files/processed/multi_archive1.tar" "multi_archive1.tar moved"
    _assert_file_exists "./uncompressed_files/processed/multi_archive2.tar" "multi_archive2.tar moved"
    _assert_file_not_exists "./multi_archive1.tar" "multi_archive1.tar removed from source"
    _assert_file_not_exists "./multi_archive2.tar" "multi_archive2.tar removed from source"
    cd "$ORIGINAL_PWD"

    # --- Test Case 4: Invalid/Corrupted tar file (plus a valid one) --- 
    _log_test_header "Test Case 4: Invalid tar file with a valid one"
    SOURCE_DIR_4="$TEST_ROOT_DIR/source_4_invalid_tar"
    mkdir -p "$SOURCE_DIR_4"
    # Valid Tar
    TAR_CONTENT_DIR_4A="$SOURCE_DIR_4/content_valid"
    mkdir -p "$TAR_CONTENT_DIR_4A"
    echo "gamma" > "$TAR_CONTENT_DIR_4A/gamma.txt"
    cd "$TAR_CONTENT_DIR_4A"
    tar -cf "../valid_for_test4.tar" .
    cd "$SOURCE_DIR_4"
    rm -rf "$TAR_CONTENT_DIR_4A"
    # Invalid Tar
    echo "This is not a tar file" > "$SOURCE_DIR_4/corrupted.tar"

    echo "Current dir for test 4: $(pwd)"
    bash "$UNCOMPRESS_SCRIPT_PATH" # Script itself should not exit with error for one bad tar if others are good
    exit_code_4=$?
    _assert_eq "$exit_code_4" "0" "Script exits successfully even with one bad tar"
    _assert_file_exists "./uncompressed_files/uncompressed_files/gamma.txt" "gamma.txt (from valid tar) extracted"
    _assert_file_exists "./uncompressed_files/processed/valid_for_test4.tar" "valid_for_test4.tar moved"
    _assert_file_exists "./corrupted.tar" "corrupted.tar remains in source (not moved)"
    _assert_file_not_exists "./uncompressed_files/processed/corrupted.tar" "corrupted.tar NOT moved to processed"
    cd "$ORIGINAL_PWD"
    
    # --- Test Case 5: Tar file that creates a subdirectory --- 
    _log_test_header "Test Case 5: Tar file with subdirectory"
    SOURCE_DIR_5="$TEST_ROOT_DIR/source_5_subdir_tar"
    TAR_CONTENT_DIR_5="$SOURCE_DIR_5/content_for_tar_subdir"
    mkdir -p "$TAR_CONTENT_DIR_5/data"
    echo "file in sub" > "$TAR_CONTENT_DIR_5/data/subfile.txt"
    echo "top level" > "$TAR_CONTENT_DIR_5/toplevel.txt"
    cd "$TAR_CONTENT_DIR_5"
    tar -cf "../subdir_archive.tar" . 
    cd "$SOURCE_DIR_5"
    rm -rf "$TAR_CONTENT_DIR_5"
    echo "Current dir for test 5: $(pwd)"
    bash "$UNCOMPRESS_SCRIPT_PATH"
    exit_code_5=$?
    _assert_eq "$exit_code_5" "0" "Script exits successfully for tar with subdir"
    _assert_file_exists "./uncompressed_files/uncompressed_files/toplevel.txt" "toplevel.txt extracted"
    _assert_dir_exists "./uncompressed_files/uncompressed_files/data" "'data' subdirectory extracted"
    _assert_file_exists "./uncompressed_files/uncompressed_files/data/subfile.txt" "subfile.txt in subdir extracted"
    _assert_file_exists "./uncompressed_files/processed/subdir_archive.tar" "subdir_archive.tar moved to processed"
    cd "$ORIGINAL_PWD"

    echo "=================================================="
    echo -e "Tests Run: $TESTS_RUN, ${GREEN}Passed: $TESTS_PASSED${NC}, ${RED}Failed: $((TESTS_RUN - TESTS_PASSED))${NC}"

    # Cleanup
    echo "Cleaning up test directory: $TEST_ROOT_DIR"
    rm -rf "$TEST_ROOT_DIR"

    if [ "$TESTS_PASSED" -eq "$TESTS_RUN" ]; then
        echo -e "${GREEN}All tests passed successfully!${NC}"
        exit 0
    else
        echo -e "${RED}Some tests failed.${NC}"
        exit 1
    fi
}

# Run the tests
main 