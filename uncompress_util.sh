#!/usr/bin/env bash

# Internal log function for this utility
_uncompress_log() {
    echo "[UNCOMPRESS_UTIL] $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Renamed: Internal function to uncompress a single tar file
# Arg1: source_file (path to .tar file)
# Arg2: dest_dir (directory to extract contents into)
_uncompress_single_tar() {
    local source_file="$1"
    local dest_dir="$2"

    # Argument checks are now primarily handled by the calling function or direct execution block
    # But basic check for source_file existence is still good here.
    if [ ! -f "$source_file" ]; then
        _uncompress_log "ERROR (internal): Source file '$source_file' not found for _uncompress_single_tar."
        return 1
    fi

    local filename
    filename=$(basename "$source_file")
    
    _uncompress_log "Starting uncompression of $filename to $dest_dir"
    
    local temp_dir
    # Create temp directory in the current working directory
    temp_dir=$(mktemp -d ./.tmp_uncompress_XXXXXX) 
    if [ $? -ne 0 ] || [ -z "$temp_dir" ] || [ ! -d "$temp_dir" ]; then
        _uncompress_log "ERROR: Failed to create temporary directory in ./ for $filename. Check permissions and mktemp support."
        return 1
    fi
    _uncompress_log "INFO: Using temporary directory: $temp_dir"
    
    if tar -xf "$source_file" -C "$temp_dir"; then
        _uncompress_log "Successfully uncompressed $filename to temporary directory $temp_dir"
        
        if rsync -av --remove-source-files "$temp_dir/" "$dest_dir/"; then
            _uncompress_log "Moved uncompressed files from $filename to $dest_dir"
            rm -r "$temp_dir"
            return 0
        else
            _uncompress_log "ERROR: Failed to move uncompressed files from $temp_dir to $dest_dir for $filename"
            rm -r "$temp_dir" # Still attempt to clean up temp
            return 1
        fi
    else
        _uncompress_log "ERROR: Failed to uncompress $filename with tar command (source: $source_file)."
        rm -r "$temp_dir" # Still attempt to clean up temp
        return 1
    fi
}

# New main function to process all tars in current directory
# Arg1: base_uncompressed_dest_dir (e.g., ./all_my_uncompressed_stuff)
process_all_tars_in_current_dir() {
    # Hardcoded destination folder
    local base_uncompressed_dest_dir="./uncompressed_files"

    # Always process .tar files in the current working directory
    _uncompress_log "INFO: Source folder is always the current working directory: $(pwd)"
    # The script will only process .tar files in the directory where it is run.

    # Create the base destination directory if it doesn't exist
    # This will now be the direct parent for uncompressed files
    if [ ! -d "$base_uncompressed_dest_dir" ]; then
        _uncompress_log "INFO: Base uncompressed files directory '$base_uncompressed_dest_dir' does not exist. Creating it."
        mkdir -p "$base_uncompressed_dest_dir"
        if [ $? -ne 0 ]; then
            _uncompress_log "ERROR: Failed to create base uncompressed files directory '$base_uncompressed_dest_dir'."
            return 1
        fi
    fi

    # The uncompressed_dir is now the base_uncompressed_dest_dir itself
    local uncompressed_dir="$base_uncompressed_dest_dir"
    # No longer need to create a nested uncompressed_files directory,
    # as uncompressed_dir now points to base_uncompressed_dest_dir.
    # if [ ! -d "$uncompressed_dir" ]; then
    #     _uncompress_log "INFO: Uncompressed files directory '$uncompressed_dir' does not exist. Creating it."
    #     mkdir -p "$uncompressed_dir"
    #     if [ $? -ne 0 ]; then
    #         _uncompress_log "ERROR: Failed to create uncompressed files directory '$uncompressed_dir'."
    #         return 1
    #     fi
    # fi

    local processed_dir="./processed"
    _uncompress_log "INFO: Processed tar files will be moved to '$processed_dir'"
    mkdir -p "$processed_dir"
    if [ $? -ne 0 ]; then
        _uncompress_log "ERROR: Failed to create processed directory '$processed_dir'."
        return 1
    fi

    _uncompress_log "Starting batch uncompression for *.tar files in the current directory ($(pwd))."
    _uncompress_log "Uncompressed content will go into: $uncompressed_dir"

    local found_tar_files=false
    for tar_file in *.tar; do
        if [ -f "$tar_file" ]; then # Check if it's a regular file
            found_tar_files=true
            _uncompress_log "--- Processing: $tar_file ---"
            if _uncompress_single_tar "$tar_file" "$uncompressed_dir"; then
                _uncompress_log "Successfully uncompressed '$tar_file' to '$uncompressed_dir'."
                _uncompress_log "Moving '$tar_file' to '$processed_dir'"
                mv "$tar_file" "$processed_dir/"
                if [ $? -ne 0 ]; then
                    _uncompress_log "ERROR: Failed to move '$tar_file' to '$processed_dir'. It remains in the current directory."
                else
                    _uncompress_log "Successfully moved '$tar_file' to '$processed_dir'."
                fi
            else
                _uncompress_log "ERROR: Failed to uncompress '$tar_file'. It will not be moved."
            fi
            _uncompress_log "--- Finished processing: $tar_file ---"
        fi
    done

    if ! $found_tar_files; then
        _uncompress_log "INFO: No .tar files found in the current directory ($(pwd)) to process."
    fi

    _uncompress_log "Batch uncompression process completed."
    return 0
}

# If the script is executed directly (not sourced), run the batch process.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # No longer expect command line arguments, destination is hardcoded
    # in process_all_tars_in_current_dir
    # if [ "$#" -ne 1 ]; then
    #     echo "Usage: $0 <base_destination_directory_for_uncompressed_content>"
    #     echo "Example: $0 ./my_uncompressed_output"
    #     echo "This script will find all *.tar files in the current directory,"
    #     echo "uncompress them into <base_destination_directory_for_uncompressed_content>,"
    #     echo "and move the original .tar files to <base_destination_directory_for_uncompressed_content>/processed/."
    #     exit 1
    # fi
    process_all_tars_in_current_dir # Call without arguments
    exit $?
fi 