#!/usr/bin/env bash

# Define source and destination directories
SOURCE_DIR="/media/haimengzhou/18TB034-CPF11/zlib2"
DEST_DIR="./"
PROCESSED_DIR="$SOURCE_DIR/processed"
LOG_FILE="copy_progress.log"

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

# Function to copy and move file
process_file() {
    local source_file="$1"
    local filename=$(basename "$source_file")
    
    log_progress "Starting copy of $filename"
    
    # Copy the file
    if cp "$source_file" "$DEST_DIR/"; then
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
}

# Process each file
process_file "$SOURCE_DIR/pilimi-zlib2-0-14679999-extra.tar"
process_file "$SOURCE_DIR/pilimi-zlib2-14680000-14999999.tar"
process_file "$SOURCE_DIR/pilimi-zlib2-15000000-15679999.tar"
process_file "$SOURCE_DIR/pilimi-zlib2-15680000-16179999.tar"
process_file "$SOURCE_DIR/pilimi-zlib2-16180000-16379999.tar"
process_file "$SOURCE_DIR/pilimi-zlib2-16380000-16469999.tar"
process_file "$SOURCE_DIR/pilimi-zlib2-16580000-16669999.tar"
process_file "$SOURCE_DIR/pilimi-zlib2-16860000-16959999.tar"
process_file "$SOURCE_DIR/pilimi-zlib2-16960000-17059999.tar"
process_file "$SOURCE_DIR/pilimi-zlib2-17060000-17149999.tar"
process_file "$SOURCE_DIR/pilimi-zlib2-17150000-17249999.tar"
process_file "$SOURCE_DIR/pilimi-zlib2-17250000-17339999.tar"

# Log completion
log_progress "All files processed"
echo "End Time: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "===========================" >> "$LOG_FILE"