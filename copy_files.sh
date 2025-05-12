#!/usr/bin/env bash

# Define source and destination directories
SOURCE_DIR="/media/haimengzhou/18TB034-CPF11/zlib2"
DEST_DIR="./"
PROCESSED_DIR="$SOURCE_DIR/processed"
LOG_FILE="copy_progress.log"
TEST_MODE=false

# Calculate optimal number of parallel jobs based on system resources
get_optimal_jobs() {
    # Get number of CPU cores
    local cpu_cores=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
    
    # Calculate available memory in GB
    local mem_gb=$(free -g 2>/dev/null | awk '/^Mem:/{print $2}' || sysctl -n hw.memsize 2>/dev/null | awk '{print $0/1024/1024/1024}')
    
    # Calculate optimal jobs based on resources
    # Use 1/4 of CPU cores or 2, whichever is greater, but not more than 4
    local optimal_jobs=$((cpu_cores / 4))
    optimal_jobs=$((optimal_jobs < 2 ? 2 : optimal_jobs))
    optimal_jobs=$((optimal_jobs > 4 ? 4 : optimal_jobs))
    
    echo $optimal_jobs
}

MAX_PARALLEL_JOBS=$(get_optimal_jobs)
log_progress "Using $MAX_PARALLEL_JOBS parallel jobs based on system resources"

# Create processed directory if it doesn't exist
mkdir -p "$PROCESSED_DIR"

# Initialize log file
echo "===== Copy Progress Log =====" > "$LOG_FILE"
echo "Start Time: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "===========================" >> "$LOG_FILE"

# Function to log progress
log_progress() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] $message" | tee -a "$LOG_FILE"
}

# Function to uncompress tar file
uncompress_file() {
    local source_file="$1"
    local dest_dir="$2"
    local filename=$(basename "$source_file")
    
    log_progress "Starting uncompression of $filename"
    
    # Create a temporary directory for uncompression
    local temp_dir=$(mktemp -d)
    
    # Extract the tar file with progress
    if tar -xf "$source_file" -C "$temp_dir"; then
        log_progress "Successfully uncompressed $filename"
        
        # Move all files from temp directory to destination using rsync
        if rsync -av --remove-source-files "$temp_dir/" "$dest_dir/"; then
            log_progress "Moved uncompressed files from $filename to destination"
            rm -r "$temp_dir"
            return 0
        else
            log_progress "ERROR: Failed to move uncompressed files from $filename"
            rm -r "$temp_dir"
            return 1
        fi
    else
        log_progress "ERROR: Failed to uncompress $filename"
        rm -r "$temp_dir"
        return 1
    fi
}

# Function to copy and move file
process_file() {
    local source_file="$1"
    local filename=$(basename "$source_file")
    
    log_progress "Starting processing of $filename"
    
    # Check if file is a tar file
    if [[ "$filename" == *.tar ]]; then
        # Uncompress the tar file
        if uncompress_file "$source_file" "$DEST_DIR"; then
            # Move the source file to processed directory
            if mv "$source_file" "$PROCESSED_DIR/"; then
                log_progress "Moved $filename to processed directory"
            else
                log_progress "ERROR: Failed to move $filename to processed directory"
            fi
        else
            log_progress "ERROR: Failed to process $filename"
        fi
    else
        # For non-tar files, use rsync for better performance
        if rsync -av --progress "$source_file" "$DEST_DIR/"; then
            log_progress "Successfully copied $filename"
            
            # Move the source file to processed directory
            if mv "$source_file" "$PROCESSED_DIR/"; then
                log_progress "Moved $filename to processed directory"
            else
                log_progress "ERROR: Failed to move $filename to processed directory"
            fi
        else
            log_progress "ERROR: Failed to copy $filename"
        fi
    fi
}

# Process files in parallel
process_files_parallel() {
    local -a files=("$@")
    local -a pids=()
    local running=0
    
    for file in "${files[@]}"; do
        # Wait if we've reached max parallel jobs
        while [ $running -ge $MAX_PARALLEL_JOBS ]; do
            for pid in "${pids[@]}"; do
                if ! kill -0 $pid 2>/dev/null; then
                    running=$((running - 1))
                fi
            done
            sleep 1
        done
        
        # Start new job
        process_file "$file" &
        pids+=($!)
        running=$((running + 1))
    done
    
    # Wait for all remaining jobs
    for pid in "${pids[@]}"; do
        wait $pid
    done
}

# Function to run tests
run_tests() {
    log_progress "Starting test mode..."
    
    # Create test directory
    local test_dir=$(mktemp -d)
    local test_source="$test_dir/test_source"
    local test_dest="$test_dir/test_dest"
    mkdir -p "$test_source" "$test_dest"
    
    # Create a small test tar file
    log_progress "Creating test files..."
    echo "test content" > "$test_source/test.txt"
    tar -cf "$test_source/test.tar" -C "$test_source" test.txt
    
    # Test file processing
    log_progress "Testing file processing..."
    local original_source="$SOURCE_DIR"
    local original_dest="$DEST_DIR"
    local original_processed="$PROCESSED_DIR"
    
    # Temporarily modify paths for testing
    SOURCE_DIR="$test_source"
    DEST_DIR="$test_dest"
    PROCESSED_DIR="$test_source/processed"
    mkdir -p "$PROCESSED_DIR"
    
    # Run test
    process_file "$test_source/test.tar"
    
    # Verify results
    if [ -f "$test_dest/test.txt" ] && [ -f "$PROCESSED_DIR/test.tar" ]; then
        log_progress "Test passed successfully!"
    else
        log_progress "Test failed! Please check the logs for details."
        return 1
    fi
    
    # Restore original paths
    SOURCE_DIR="$original_source"
    DEST_DIR="$original_dest"
    PROCESSED_DIR="$original_processed"
    
    # Cleanup
    rm -rf "$test_dir"
    log_progress "Test mode completed."
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --test)
            TEST_MODE=true
            shift
            ;;
        *)
            log_progress "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Run tests if in test mode
if [ "$TEST_MODE" = true ]; then
    run_tests
    exit $?
fi

# List of files to process
files=(
    "$SOURCE_DIR/pilimi-zlib2-0-14679999-extra.tar"
    "$SOURCE_DIR/pilimi-zlib2-14680000-14999999.tar"
    "$SOURCE_DIR/pilimi-zlib2-15000000-15679999.tar"
    "$SOURCE_DIR/pilimi-zlib2-15680000-16179999.tar"
    "$SOURCE_DIR/pilimi-zlib2-16180000-16379999.tar"
    "$SOURCE_DIR/pilimi-zlib2-16380000-16469999.tar"
    "$SOURCE_DIR/pilimi-zlib2-16580000-16669999.tar"
    "$SOURCE_DIR/pilimi-zlib2-16860000-16959999.tar"
    "$SOURCE_DIR/pilimi-zlib2-16960000-17059999.tar"
    "$SOURCE_DIR/pilimi-zlib2-17060000-17149999.tar"
    "$SOURCE_DIR/pilimi-zlib2-17150000-17249999.tar"
    "$SOURCE_DIR/pilimi-zlib2-17250000-17339999.tar"
)

# Process files in parallel
process_files_parallel "${files[@]}"

# Log completion
log_progress "All files processed"
echo "End Time: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "===========================" >> "$LOG_FILE"


# [2025-04-10 07:25:56] Starting copy of pilimi-zlib2-0-14679999-extra.tar
# [2025-04-10 08:25:50] Successfully copied pilimi-zlib2-0-14679999-extra.tar
# [2025-04-10 08:25:50] Moved pilimi-zlib2-0-14679999-extra.tar to processed directory
# [2025-04-10 08:25:50] Starting copy of pilimi-zlib2-14680000-14999999.tar
# [2025-04-10 09:39:28] Successfully copied pilimi-zlib2-14680000-14999999.tar
# [2025-04-10 09:39:28] Moved pilimi-zlib2-14680000-14999999.tar to processed directory
# [2025-04-10 09:39:28] Starting copy of pilimi-zlib2-15000000-15679999.tar
# [2025-04-10 16:04:12] Successfully copied pilimi-zlib2-15000000-15679999.tar
# [2025-04-10 16:04:12] Moved pilimi-zlib2-15000000-15679999.tar to processed directory
# [2025-04-10 16:04:12] Starting copy of pilimi-zlib2-15680000-16179999.tar
# [2025-04-11 00:03:27] Successfully copied pilimi-zlib2-15680000-16179999.tar
# [2025-04-11 00:03:27] Moved pilimi-zlib2-15680000-16179999.tar to processed directory
# [2025-04-11 00:03:27] Starting copy of pilimi-zlib2-16180000-16379999.tar
# [2025-04-11 04:43:01] Successfully copied pilimi-zlib2-16180000-16379999.tar
# [2025-04-11 04:43:01] Moved pilimi-zlib2-16180000-16379999.tar to processed directory
# [2025-04-11 04:43:01] Starting copy of pilimi-zlib2-16380000-16469999.tar