#!/usr/bin/env bash

# Define source and destination directories
SOURCE_DIR="/media/haimengzhou/18TB034-CPF11/zlib2"
DEST_DIR="./"
UNCOMPRESSED_DIR="$DEST_DIR/uncompressed_files"
PROCESSED_DIR="$SOURCE_DIR/processed"
PROGRESS_FILE="copy_progress.json"
TEST_MODE=false
MIN_DISK_SPACE_GB=10  # Minimum required disk space in GB

# Function to log progress (modified to only print to STDOUT)
log_progress() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] $message"
}

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

# Source utility scripts
UNCOMPRESS_AVAILABLE=false
if [ -f "./uncompress_util.sh" ]; then
    source ./uncompress_util.sh
    UNCOMPRESS_AVAILABLE=true
    log_progress "Uncompress utility loaded successfully"
else
    log_progress "WARNING: uncompress_util.sh not found - tar files will be moved without uncompressing"
fi

# Wrapper function to match the expected interface
uncompress_file() {
    local source_file="$1"
    local dest_dir="$2"
    
    if [ "$UNCOMPRESS_AVAILABLE" = true ]; then
        # Call the internal function from uncompress_util.sh
        _uncompress_single_tar "$source_file" "$dest_dir"
        return $?
    else
        log_progress "Skipping uncompress for $(basename "$source_file") - uncompress utility not available"
        return 0  # Return success to continue processing
    fi
}

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
    # Use different df flags for macOS vs Linux
    local available_gb
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        available_gb=$(df -g "$dir" | awk 'NR==2 {print $4}')
    else
        # Linux
        available_gb=$(df -BG "$dir" | awk 'NR==2 {print $4}' | sed 's/G//')
    fi
    
    # Handle empty or non-numeric values
    if [[ -z "$available_gb" || ! "$available_gb" =~ ^[0-9]+$ ]]; then
        log_progress "WARNING: Could not determine disk space for $dir"
        return 0  # Assume OK if we can't check
    fi
    
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

# Function to process a tar file already in DEST_DIR
process_local_tar_file() {
    local local_tar_file="$1" # e.g., ./somefile.tar which is $DEST_DIR/somefile.tar
    local filename=$(basename "$local_tar_file")

    log_progress "Processing local tar file: $filename from $DEST_DIR"

    # Check if file was already processed
    if is_file_processed "$filename"; then
        log_progress "Skipping already processed local file: $filename"
        return 0
    fi

    # Check disk space for uncompression and move
    if ! check_disk_space "$UNCOMPRESSED_DIR"; then
        log_progress "ERROR: Insufficient disk space in $UNCOMPRESSED_DIR for $filename"
        save_progress "$filename" "failed_disk_space_local_uncompress"
        return 1
    fi
    # Check disk space in PROCESSED_DIR for the move
    if ! check_disk_space "$PROCESSED_DIR"; then
        log_progress "ERROR: Insufficient disk space in $PROCESSED_DIR for moving $filename"
        save_progress "$filename" "failed_disk_space_local_processed"
        return 1
    fi

    # "Copy" operation is uncompressing for tar files
    if [ "$UNCOMPRESS_AVAILABLE" = true ]; then
        log_progress "Uncompressing $filename from $DEST_DIR to $UNCOMPRESSED_DIR"
        if uncompress_file "$local_tar_file" "$UNCOMPRESSED_DIR"; then
            log_progress "Successfully uncompressed $filename to $UNCOMPRESSED_DIR"
        else
            log_progress "ERROR: Failed to uncompress $filename from $DEST_DIR"
            save_progress "$filename" "failed_uncompress_local"
            return 1 # Failed uncompress is critical
        fi
    else
        log_progress "Skipping uncompress for $filename - uncompress utility not available"
    fi

    # "Move" the original tar file from DEST_DIR to PROCESSED_DIR
    log_progress "Moving $filename from $DEST_DIR to $PROCESSED_DIR"
    if mv "$local_tar_file" "$PROCESSED_DIR/$filename"; then
        log_progress "Successfully moved $filename from $DEST_DIR to $PROCESSED_DIR"
        save_progress "$filename" "completed_local_processing"
    else
        log_progress "ERROR: Failed to move $filename from $DEST_DIR to $PROCESSED_DIR"
        save_progress "$filename" "failed_move_local"
        return 1 # Failed move is critical
    fi
    return 0
}

# Process local tar files in parallel
process_local_tar_files_parallel() {
    local -a files_to_process=("$@")
    local -a pids=()
    local -a failed_processing_files=() # Renamed to avoid conflict if used in same scope
    local running_jobs=0 # Renamed to avoid conflict

    for current_file_path in "${files_to_process[@]}"; do
        # Wait if we've reached max parallel jobs
        while [ $running_jobs -ge $MAX_PARALLEL_JOBS ]; do
            local new_pids=()
            for pid_val in "${pids[@]}"; do
                if ! kill -0 "$pid_val" 2>/dev/null; then
                    wait "$pid_val"
                    if [ $? -ne 0 ]; then
                        # Need to map pid back to filename for accurate failed_files logging
                        # This simplistic approach won't map pid to filename correctly here
                        # For robust error reporting, one might store pid-to-filename map
                        # For now, we add the file that was *about to be processed* or *last processed*
                        # A better way is to handle failure inside the backgrounded function or capture its specific file
                        log_progress "A background job (PID $pid_val) failed."
                        # This isn't perfect, as current_file_path might not be the one that failed
                        # failed_processing_files+=("$current_file_path") # This is potentially inaccurate
                    fi
                    running_jobs=$((running_jobs - 1))
                else
                    new_pids+=("$pid_val")
                fi
            done
            pids=("${new_pids[@]}")
            # If no jobs finished, sleep briefly
            if [ ${#pids[@]} -eq $running_jobs ] && [ $running_jobs -ge $MAX_PARALLEL_JOBS ]; then
                 sleep 1
            fi
        done

        # Start new job
        # Store PID and its corresponding file path
        process_local_tar_file "$current_file_path" &
        local job_pid=$!
        pids+=($job_pid)
        # Associate PID with file for accurate failure tracking
        # This requires a more complex setup (e.g., associative array if bash version >= 4)
        # Or temp files mapping pid to filename.
        # For now, the error reporting for *which specific file* failed in parallel is simplified.
        running_jobs=$((running_jobs + 1))
    done

    # Wait for all remaining jobs
    # More robust failure tracking:
    for pid_val in "${pids[@]}"; do
        wait "$pid_val"
        # How to get filename from pid here?
        # If we knew which file corresponds to pid_val:
        # if [ $? -ne 0 ]; then failed_processing_files+=("filename_for_$pid_val"); fi
    done

    # Wait for all pids and collect failures (alternative way)
    local final_failed_count=0
    for pid_val in "${pids[@]}"; do
        if ! wait "$pid_val"; then
            # Cannot easily get filename here without prior mapping
            log_progress "A job (PID $pid_val) completed with an error."
            final_failed_count=$((final_failed_count + 1))
        fi
    done
    
    if [ "$final_failed_count" -gt 0 ]; then
        log_progress "$final_failed_count local files failed to process. Review logs for specific PIDs/errors."
        # Note: The failed_processing_files array is not reliably populated here for specific filenames
        # A more robust method would be for process_local_tar_file to write to a unique failure log on error.
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
    if [ "$UNCOMPRESS_AVAILABLE" = true ]; then
        # If uncompress is available, check for both uncompressed content and moved tar
        if [ -f "$UNCOMPRESSED_DIR/test.txt" ] && [ -f "$PROCESSED_DIR/test.tar" ]; then
            log_progress "Test passed successfully!"
        else
            log_progress "Test failed! Please check the logs for details."
            return 1
        fi
    else
        # If uncompress is not available, only check that tar was moved to processed
        if [ -f "$PROCESSED_DIR/test.tar" ]; then
            log_progress "Test passed successfully! (uncompress utility not available, only checked file movement)"
        else
            log_progress "Test failed! tar file was not moved to processed directory."
            return 1
        fi
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

# Original logic: Process files from SOURCE_DIR
# List of files to process from source directory
log_progress "Searching for tar files in $SOURCE_DIR to process."

# Find tar files in source directory
source_tar_files=()
# Use a simple, portable approach to find tar files
if [ -d "$SOURCE_DIR" ]; then
    # Find up to 5 tar files, sorted by modification time (newest first)
    while IFS= read -r file; do
        if [ ${#source_tar_files[@]} -lt 5 ]; then
            source_tar_files+=("$file")
        fi
    done < <(find "$SOURCE_DIR" -maxdepth 1 -type f -name '*.tar' -printf '%T@ %p\n' 2>/dev/null | sort -nr | cut -d' ' -f2-)
else
    log_progress "ERROR: Source directory $SOURCE_DIR does not exist or is not accessible"
fi

if [ ${#source_tar_files[@]} -eq 0 ]; then
    log_progress "No .tar files found in $SOURCE_DIR to process."
else
    log_progress "Found ${#source_tar_files[@]} tar files in $SOURCE_DIR to process:"
    for f in "${source_tar_files[@]}"; do
        log_progress "  - $f"
    done
    process_files_parallel "${source_tar_files[@]}"
fi

# Comment out the local processing logic
# # New logic: Find up to 5 tar files in DEST_DIR and process them
# log_progress "Searching for up to 5 tar files in $DEST_DIR to process locally."
# 
# local_tar_files_to_process=()
# # Use find to get files, head to limit to 5, and mapfile to read into an array
# # Ensure files are .tar and are actual files (not directories ending in .tar)
# # -maxdepth 1 ensures we only look in DEST_DIR, not subdirectories.
# mapfile -t local_tar_files_to_process < <(find "$DEST_DIR" -maxdepth 1 -type f -name '*.tar' -print0 | xargs -0 ls -t | head -n 5)
# 
# if [ ${#local_tar_files_to_process[@]} -eq 0 ]; then
#     log_progress "No .tar files found in $DEST_DIR to process."
# else
#     log_progress "Found ${#local_tar_files_to_process[@]} tar files in $DEST_DIR to process:"
#     for f in "${local_tar_files_to_process[@]}"; do
#         log_progress "  - $f"
#     done
#     process_local_tar_files_parallel "${local_tar_files_to_process[@]}"
# fi

# Log completion
log_progress "All files processed"
log_progress "Script completed"
