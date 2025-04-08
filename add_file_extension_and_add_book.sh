#!/usr/bin/env bash

# Define directories
DEST_DIR="./"
FAILED_DIR="./failed"
SUCCESS_DIR="./success"
LOG_FILE="failed_additions.log"
mkdir -p "$DEST_DIR" "$FAILED_DIR" "$SUCCESS_DIR"

BATCH_SIZE=100  # Number of books per batch
TIMEOUT_DURATION=300  # Timeout in seconds per batch
NUM_THREADS=10 # Number of parallel processes

# Monitoring setup
MONITOR_LOG="performance_monitor.log"
echo "Timestamp,CPU_Usage(%),Memory_Usage(%),Disk_IO(kB/s),Active_Threads" > "$MONITOR_LOG"

# Function to monitor system resources
monitor_resources() {
    while true; do
        # Get CPU usage (using ps for macOS)
        cpu_usage=$(ps -A -o %cpu | awk '{s+=$1} END {print s}')
        
        # Get memory usage (using vm_stat for macOS)
        memory_info=$(vm_stat)
        pages_active=$(echo "$memory_info" | grep "Pages active" | awk '{print $3}' | tr -d '.')
        pages_wired=$(echo "$memory_info" | grep "Pages wired down" | awk '{print $4}' | tr -d '.')
        pages_free=$(echo "$memory_info" | grep "Pages free" | awk '{print $3}' | tr -d '.')
        total_pages=$((pages_active + pages_wired + pages_free))
        memory_percent=$(((pages_active + pages_wired) * 100 / total_pages))
        
        # Get disk I/O (using iostat if available, otherwise N/A)
        if command -v iostat &> /dev/null; then
            disk_io=$(iostat -n 1 | awk 'NR==4 {print $3+$4}')
        else
            disk_io="N/A"
        fi
        
        # Get number of active threads for this process group
        active_threads=$(ps -M $$ | wc -l | tr -d ' ')
        
        # Log the metrics
        echo "$(date '+%Y-%m-%d %H:%M:%S'),$cpu_usage,$memory_percent,$disk_io,$active_threads" >> "$MONITOR_LOG"
        
        sleep 1
    done
}

# Start monitoring in the background
monitor_resources &
MONITOR_PID=$!

# Function to clean up monitoring on script exit
cleanup() {
    kill $MONITOR_PID 2>/dev/null
    # Generate performance summary
    if [ -s "$MONITOR_LOG" ] && [ $(wc -l < "$MONITOR_LOG") -gt 1 ]; then
        echo -e "\n===== Performance Summary ====="
        echo "Average CPU Usage: $(awk -F',' 'NR>1 {sum+=$2} END {if(NR>1) print sum/(NR-1); else print "N/A"}' "$MONITOR_LOG")%"
        echo "Peak CPU Usage: $(awk -F',' 'NR>1 {if($2>max)max=$2} END {print max}' "$MONITOR_LOG")%"
        echo "Average Memory Usage: $(awk -F',' 'NR>1 {sum+=$3} END {if(NR>1) print sum/(NR-1); else print "N/A"}' "$MONITOR_LOG")%"
        echo "Peak Memory Usage: $(awk -F',' 'NR>1 {if($3>max)max=$3} END {print max}' "$MONITOR_LOG")%"
        echo "Average Active Threads: $(awk -F',' 'NR>1 {sum+=$5} END {if(NR>1) print int(sum/(NR-1)); else print "N/A"}' "$MONITOR_LOG")"
        echo "Peak Active Threads: $(awk -F',' 'NR>1 {if($5>max)max=$5} END {print int(max)}' "$MONITOR_LOG")"
        echo "Total Runtime: $(awk -F',' 'NR>1{last=$1} END{print "'"$(date -j -f "%Y-%m-%d %H:%M:%S" "$(head -n2 "$MONITOR_LOG" | tail -n1 | cut -d',' -f1)" +%s)"'" - "'"$(date -j -f "%Y-%m-%d %H:%M:%S" "$last" +%s)"'"}' "$MONITOR_LOG") seconds"
    fi
}

# Set up trap to ensure cleanup runs on script exit
trap cleanup EXIT

echo "===== Processing Started: Renaming and Importing Books ====="

###############################################################################
# Step 1 & 2: Combined file processing for better performance
# First, collect all files that need processing
echo "Collecting files to process..."
files_to_process=()
while IFS= read -r -d '' f; do
    files_to_process+=("$f")
done < <(find "$DEST_DIR" -type f ! -path "$SUCCESS_DIR/*" ! -name "*.*" -print0)

echo "Processing ${#files_to_process[@]} files..."

# Create a temporary script for processing
process_script=$(mktemp)
cat > "$process_script" << 'EOF'
#!/bin/bash
f="$1"
if [[ -f "$f" ]]; then
    dir=$(dirname "$f")
    base=$(basename "$f")
    filename="$dir/${base%.*}"
    
    # Get file type once and store it
    mime_type=$(file --mime-type "$f")
    filetype=$(file -b "$f")
    
    # RAR check
    if [[ "$mime_type" =~ application/x-rar ]] || [[ "$mime_type" =~ application/vnd.rar ]]; then
        if [[ ! "$f" =~ \.(cbr|CBR)$ ]]; then
            new_name="$dir/${base}.cbr"
            if [[ ! -f "$new_name" ]]; then
                mv "$f" "$new_name"
                echo "[INFO] Renamed \"$f\" -> \"$new_name\" (RAR to CBR)"
            fi
        fi
    # ZIP check
    elif [[ "$mime_type" =~ application/zip ]]; then
        if [[ "$f" =~ \.(zip|ZIP)$ ]]; then
            new_name="$dir/${base%.*}.cbz"
        else
            new_name="$dir/${base}.cbz"
        fi
        if [[ ! "$f" =~ \.(cbz|CBZ)$ ]]; then
            mv -n "$f" "$new_name"
            echo "[INFO] Renamed \"$f\" -> \"$new_name\" (ZIP to CBZ)"
        fi
    # PDF check
    elif [[ ! "$f" =~ \.(pdf|PDF)$ && $filetype == *"PDF document"* ]]; then
        mv "$f" "$filename.pdf"
        echo "[INFO] Renamed \"$f\" -> \"$filename.pdf\" (Detected PDF)"
    # EPUB check
    elif [[ ! "$f" =~ \.(epub|EPUB)$ && $filetype == *"EPUB document"* ]]; then
        mv "$f" "$filename.epub"
        echo "[INFO] Renamed \"$f\" -> \"$filename.epub\" (Detected EPUB)"
    # MOBI check
    elif [[ $filetype == *"Mobipocket E-book"* ]]; then
        if [[ "$f" =~ \.(prc|PRC)$ ]]; then
            mv "$f" "$filename.prc"
            echo "[INFO] Renamed \"$f\" -> \"$filename.prc\" (Detected PRC)"
        elif [[ ! "$f" =~ \.(mobi|MOBI)$ ]]; then
            mv "$f" "$filename.mobi"
            echo "[INFO] Renamed \"$f\" -> \"$filename.mobi\" (Detected MOBI)"
        fi
    # Additional MOBI detection by content
    elif [[ ! "$f" =~ \.(mobi|MOBI)$ ]] && head -c 8 "$f" | grep -q "BOOKMOBI"; then
        mv "$f" "$filename.mobi"
        echo "[INFO] Renamed \"$f\" -> \"$filename.mobi\" (Detected MOBI by content)"
    fi
fi
EOF

chmod +x "$process_script"

# Process files in parallel using xargs with the temporary script
printf '%s\0' "${files_to_process[@]}" | xargs -0 -P "$NUM_THREADS" -n 1 "$process_script"

# Clean up
rm "$process_script"

echo "===== File Renaming Completed. Proceeding with Book Import. ====="

###############################################################################
# Step 3: Process books in batches and add them to Calibre
books=()
while IFS= read -r -d '' book; do
    books+=("$book")
done < <(find "$DEST_DIR" -type f \( -iname "*.pdf" -o -iname "*.epub" -o -iname "*.mobi" -o -iname "*.azw3" -o -iname "*.fb2" -o -iname "*.cbz" -o -iname "*.cbr" -o -iname "*.bbe" -o -iname "*.djvu" -o -iname "*.lit" \) -not -path "$SUCCESS_DIR/*" -print0)

echo "[INFO] Found ${#books[@]} ebook files to process."

consecutive_failures=0

for ((i=0; i<${#books[@]}; i+=BATCH_SIZE)); do
    echo "[INFO] Processing batch $((i / BATCH_SIZE + 1))..."
    batch=("${books[@]:i:BATCH_SIZE}")

    if timeout "$TIMEOUT_DURATION"s calibredb add --recurse "${batch[@]}" 2>> "$LOG_FILE"; then
        echo "[SUCCESS] Batch $((i / BATCH_SIZE + 1)) added successfully!"
        for book in "${batch[@]}"; do
            mv "$book" "$SUCCESS_DIR/" 2>/dev/null
        done
        consecutive_failures=0
    else
        echo "[ERROR] Batch $((i / BATCH_SIZE + 1)) failed. Moving files to FAILED_DIR."
        for book in "${batch[@]}"; do
            mv "$book" "$FAILED_DIR/" 2>/dev/null
        done
        ((consecutive_failures++))
        echo "[ERROR] Consecutive failures: $consecutive_failures"
        if ((consecutive_failures >= 5)); then
            echo "[ERROR] Five consecutive batch failures detected. Terminating the script."
            exit 1
        fi
    fi

    echo "[INFO] Batch $((i / BATCH_SIZE + 1)) completed."
done

echo "[INFO] All batches processed."

echo "===== Book Import Process Completed Successfully ====="
