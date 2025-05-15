#!/usr/bin/env bash

# Define source and destination directories
SOURCE_DIR="/media/haimengzhou/18TB034-CPF11/zlib2"
DEST_DIR="./"
UNCOMPRESSED_DIR="$DEST_DIR/uncompressed_files"
PROCESSED_DIR="$SOURCE_DIR/processed"
LOG_FILE="copy_progress.log"
PROGRESS_FILE="copy_progress.json"
TEST_MODE=false
MIN_DISK_SPACE_GB=10  # Minimum required disk space in GB

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

# Create necessary directories
mkdir -p "$PROCESSED_DIR"
mkdir -p "$UNCOMPRESSED_DIR"

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

# Source utility scripts
source ./uncompress_util.sh

# Function to copy file from source to destination
copy_file() {
    local source_file="$1"
    local filename=$(basename "$source_file")
    
    log_progress "Copying $filename from source to destination"
    
    if rsync -av --progress "$source_file" "$DEST_DIR/"; then
        log_progress "Successfully copied $filename to destination"
        return 0
    else
        log_progress "ERROR: Failed to copy $filename"
        return 1
    fi
}

# Function to check available disk space
check_disk_space() {
    local dir="$1"
    local available_gb=$(df -BG "$dir" | awk 'NR==2 {print $4}' | sed 's/G//')
    if [ "$available_gb" -lt "$MIN_DISK_SPACE_GB" ]; then
        log_progress "WARNING: Low disk space on $dir. Only $available_gb GB available. Minimum required: $MIN_DISK_SPACE_GB GB"
        return 1
    fi
    return 0
}

# Function to save progress
save_progress() {
    local file="$1"
    local status="$2"
    local progress_data="{}"
    
    if [ -f "$PROGRESS_FILE" ]; then
        progress_data=$(cat "$PROGRESS_FILE")
    fi
    
    # Update progress using jq if available, otherwise use sed
    if command -v jq >/dev/null 2>&1; then
        echo "$progress_data" | jq --arg file "$file" --arg status "$status" '. + {($file): $status}' > "$PROGRESS_FILE"
    else
        # Simple sed-based fallback
        echo "$progress_data" | sed "s/}/,\"$file\":\"$status\"}/" > "$PROGRESS_FILE"
    fi
}

# Function to check if file was already processed
is_file_processed() {
    local file="$1"
    if [ -f "$PROGRESS_FILE" ]; then
        if command -v jq >/dev/null 2>&1; then
            local status=$(jq -r --arg file "$file" '.[$file]' "$PROGRESS_FILE")
            [ "$status" = "completed" ] && return 0
        else
            # Simple grep-based fallback
            grep -q "\"$file\":\"completed\"" "$PROGRESS_FILE" && return 0
        fi
    fi
    return 1
}

# Function to process file
process_file() {
    local source_file="$1"
    local filename=$(basename "$source_file")
    local dest_file="$DEST_DIR/$filename"
    
    # Check if file was already processed
    if is_file_processed "$filename"; then
        log_progress "Skipping already processed file: $filename"
        return 0
    fi
    
    # Check disk space before processing
    if ! check_disk_space "$DEST_DIR"; then
        log_progress "ERROR: Insufficient disk space to process $filename"
        save_progress "$filename" "failed_disk_space"
        return 1
    fi
    
    log_progress "Starting processing of $filename"
    
    # First, copy the file from source to destination
    if copy_file "$source_file"; then
        # Then, if it's a tar file, uncompress it
        if [[ "$filename" == *.tar ]]; then
            if uncompress_file "$dest_file" "$UNCOMPRESSED_DIR"; then
                # Move the original source file (from SOURCE_DIR) to processed directory
                if mv "$source_file" "$PROCESSED_DIR/"; then
                    log_progress "Moved original $filename from $SOURCE_DIR to $PROCESSED_DIR"
                    save_progress "$filename" "completed"
                else
                    log_progress "ERROR: Failed to move original $filename from $SOURCE_DIR to $PROCESSED_DIR"
                    save_progress "$filename" "failed_move"
                fi
                # Optionally, remove the copied tar file from DEST_DIR if it's different from UNCOMPRESSED_DIR
                if [ "$dest_file" != "$UNCOMPRESSED_DIR/$filename" ]; then
                    log_progress "Removing copied tar file $dest_file from $DEST_DIR"
                    rm -f "$dest_file"
                fi
            else
                log_progress "ERROR: Failed to uncompress $dest_file using uncompress_util.sh"
                save_progress "$filename" "failed_uncompress"
            fi
        else
            # For non-tar files, just move the source to processed
            if mv "$source_file" "$PROCESSED_DIR/"; then
                log_progress "Moved $filename to processed directory"
                save_progress "$filename" "completed"
            else
                log_progress "ERROR: Failed to move $filename to processed directory"
                save_progress "$filename" "failed_move"
            fi
        fi
    else
        log_progress "ERROR: Failed to copy $filename"
        save_progress "$filename" "failed_copy"
    fi
}

# Process files in parallel with improved error handling
process_files_parallel() {
    local -a files=("$@")
    local -a pids=()
    local -a failed_files=()
    local running=0
    
    for file in "${files[@]}"; do
        # Wait if we've reached max parallel jobs
        while [ $running -ge $MAX_PARALLEL_JOBS ]; do
            for pid in "${pids[@]}"; do
                if ! kill -0 $pid 2>/dev/null; then
                    wait $pid
                    if [ $? -ne 0 ]; then
                        failed_files+=("$file")
                    fi
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
        if [ $? -ne 0 ]; then
            failed_files+=("$file")
        fi
    done
    
    # Report failed files
    if [ ${#failed_files[@]} -gt 0 ]; then
        log_progress "The following files failed to process:"
        for file in "${failed_files[@]}"; do
            log_progress "  - $file"
        done
        return 1
    fi
    
    return 0
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
    local original_uncompressed="$UNCOMPRESSED_DIR"
    
    # Temporarily modify paths for testing
    SOURCE_DIR="$test_source"
    DEST_DIR="$test_dest"
    PROCESSED_DIR="$test_source/processed"
    UNCOMPRESSED_DIR="$test_dest/uncompressed_files"
    mkdir -p "$PROCESSED_DIR" "$UNCOMPRESSED_DIR"
    
    # Run test
    process_file "$test_source/test.tar"
    
    # Verify results
    if [ -f "$UNCOMPRESSED_DIR/test.txt" ] && [ -f "$PROCESSED_DIR/test.tar" ]; then
        log_progress "Test passed successfully!"
    else
        log_progress "Test failed! Please check the logs for details."
        return 1
    fi
    
    # Restore original paths
    SOURCE_DIR="$original_source"
    DEST_DIR="$original_dest"
    PROCESSED_DIR="$original_processed"
    UNCOMPRESSED_DIR="$original_uncompressed"
    
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

# Process files in parallel with improved error handling
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