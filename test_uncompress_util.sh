#!/usr/bin/env bash

# Test script for uncompress_util.sh

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

# Function to run a test
run_test() {
    local test_name="$1"
    local test_command="$2"
    local expected_exit_code="${3:-0}"
    
    echo -e "${YELLOW}Running test: $test_name${NC}"
    
    # Run the test command
    eval "$test_command"
    local exit_code=$?
    
    # Check if the test passed
    if [ $exit_code -eq $expected_exit_code ]; then
        echo -e "${GREEN}✓ Test passed: $test_name${NC}"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗ Test failed: $test_name (Expected exit code: $expected_exit_code, Got: $exit_code)${NC}"
        ((TESTS_FAILED++))
        return 1
    fi
}

# Function to create test files
create_test_files() {
    local test_dir="$1"
    local num_files="$2"
    
    # Create some test files
    for i in $(seq 1 $num_files); do
        echo "Test content $i" > "$test_dir/test_file_$i.txt"
    done
}

# Function to create a test tar file
create_test_tar() {
    local source_dir="$1"
    local tar_name="$2"
    
    tar -cf "$tar_name" -C "$source_dir" .
}

# Function to verify extracted files
verify_extracted_files() {
    local dest_dir="$1"
    local expected_count="$2"
    
    local actual_count=$(find "$dest_dir" -type f | wc -l)
    if [ "$actual_count" -eq "$expected_count" ]; then
        return 0
    else
        echo "Expected $expected_count files, found $actual_count"
        return 1
    fi
}

# Main test function
main() {
    echo "Starting tests for uncompress_util.sh"
    echo "===================================="
    
    # Create test directories
    local test_root=$(mktemp -d)
    local test_source="$test_root/source"
    local test_dest="$test_root/dest"
    local test_processed="$test_root/processed"
    
    mkdir -p "$test_source" "$test_dest" "$test_processed"
    
    # Source the script to be tested
    source ./uncompress_util.sh
    
    # Test 1: Test with no tar files
    run_test "No tar files" \
        "cd '$test_source' && process_all_tars_in_current_dir '$test_dest'" \
        0
    
    # Test 2: Test with invalid destination directory
    run_test "Invalid destination directory" \
        "cd '$test_source' && process_all_tars_in_current_dir ''" \
        1
    
    # Test 3: Test with a single valid tar file
    create_test_files "$test_source" 3
    create_test_tar "$test_source" "$test_source/test.tar"
    run_test "Single valid tar file" \
        "cd '$test_source' && process_all_tars_in_current_dir '$test_dest'" \
        0
    
    # Verify the extracted files
    if verify_extracted_files "$test_dest" 3; then
        echo -e "${GREEN}✓ File extraction verification passed${NC}"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}✗ File extraction verification failed${NC}"
        ((TESTS_FAILED++))
    fi
    
    # Test 4: Test with multiple tar files
    create_test_files "$test_source" 2
    create_test_tar "$test_source" "$test_source/test2.tar"
    create_test_files "$test_source" 2
    create_test_tar "$test_source" "$test_source/test3.tar"
    run_test "Multiple tar files" \
        "cd '$test_source' && process_all_tars_in_current_dir '$test_dest'" \
        0
    
    # Verify the total number of extracted files
    if verify_extracted_files "$test_dest" 7; then
        echo -e "${GREEN}✓ Multiple file extraction verification passed${NC}"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}✗ Multiple file extraction verification failed${NC}"
        ((TESTS_FAILED++))
    fi
    
    # Test 5: Test with a corrupted tar file
    echo "This is not a valid tar file" > "$test_source/corrupted.tar"
    run_test "Corrupted tar file" \
        "cd '$test_source' && process_all_tars_in_current_dir '$test_dest'" \
        0
    
    # Test 6: Test with a non-existent tar file
    run_test "Non-existent tar file" \
        "cd '$test_source' && _uncompress_single_tar 'nonexistent.tar' '$test_dest'" \
        1
    
    # Cleanup
    rm -rf "$test_root"
    
    # Print summary
    echo -e "\nTest Summary:"
    echo "============="
    echo -e "${GREEN}Tests passed: $TESTS_PASSED${NC}"
    echo -e "${RED}Tests failed: $TESTS_FAILED${NC}"
    
    # Exit with appropriate status
    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "\n${GREEN}All tests passed successfully!${NC}"
        exit 0
    else
        echo -e "\n${RED}Some tests failed.${NC}"
        exit 1
    fi
}

# Run the tests
main 