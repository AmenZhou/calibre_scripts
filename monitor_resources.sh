#!/usr/bin/env bash

# Function to monitor system resources
monitor_resources() {
    local monitor_log="$1"
    local process_script="$2"
    local last_summary_time=$(date +%s)
    local summary_interval=60  # Print summary every 60 seconds
    
    # Initialize log file
    echo "===== Performance Monitoring Started =====" > "$monitor_log"
    echo "Timestamp,CPU_Usage(%),Memory_Usage(%),Disk_IO(kB/s),Active_Processes" >> "$monitor_log"
    
    while true; do
        # Get CPU usage (works on both Linux and macOS)
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            cpu_usage=$(ps -A -o %cpu | awk '{s+=$1} END {print s}')
        else
            # Linux
            cpu_usage=$(top -bn1 | grep "Cpu(s)" | awk '{print $2 + $4}')
        fi
        
        # Get memory usage (works on both Linux and macOS)
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            memory_info=$(vm_stat)
            pages_active=$(echo "$memory_info" | grep "Pages active" | awk '{print $3}' | tr -d '.')
            pages_wired=$(echo "$memory_info" | grep "Pages wired down" | awk '{print $4}' | tr -d '.')
            pages_free=$(echo "$memory_info" | grep "Pages free" | awk '{print $3}' | tr -d '.')
            total_pages=$((pages_active + pages_wired + pages_free))
            memory_percent=$(((pages_active + pages_wired) * 100 / total_pages))
        else
            # Linux
            memory_info=$(free | grep Mem)
            total_mem=$(echo "$memory_info" | awk '{print $2}')
            used_mem=$(echo "$memory_info" | awk '{print $3}')
            memory_percent=$((used_mem * 100 / total_mem))
        fi
        
        # Get disk I/O (works on both Linux and macOS)
        if command -v iostat &> /dev/null; then
            if [[ "$OSTYPE" == "darwin"* ]]; then
                # macOS - use iostat with disk0
                disk_io=$(iostat -d disk0 1 1 | awk 'NR==3 {print $3+$4}')
            else
                # Linux - use iostat with -x for extended stats
                disk_io=$(iostat -x 1 1 | awk 'NR==4 {print $6+$7}')
            fi
        else
            # If iostat is not available, try to get disk I/O another way
            if [[ "$OSTYPE" == "darwin"* ]]; then
                # macOS - use vm_stat for disk activity
                disk_io=$(vm_stat | grep "disk" | awk '{print $2}' | tr -d '.')
            else
                # Linux - try to read from /proc/diskstats
                disk_io=$(awk '{print $4+$8}' /proc/diskstats 2>/dev/null || echo "N/A")
            fi
        fi
        
        # Get number of active processes (including xargs workers)
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            active_processes=$(ps -ef | grep "$process_script" | grep -v grep | wc -l | tr -d ' ')
        else
            # Linux
            active_processes=$(ps -ef | grep "$process_script" | grep -v grep | wc -l)
        fi
        
        # Get current timestamp
        current_time=$(date '+%Y-%m-%d %H:%M:%S')
        
        # Log the metrics
        echo "$current_time,$cpu_usage,$memory_percent,$disk_io,$active_processes" >> "$monitor_log"
        
        # Print periodic summary
        current_time_sec=$(date +%s)
        if (( current_time_sec - last_summary_time >= summary_interval )); then
            echo -e "\n===== Performance Summary (Last 60 seconds) ====="
            echo "Time: $current_time"
            echo "CPU Usage: $cpu_usage%"
            echo "Memory Usage: $memory_percent%"
            echo "Disk I/O: $disk_io kB/s"
            echo "Active Processes: $active_processes"
            echo "============================================="
            last_summary_time=$current_time_sec
        fi
        
        sleep 1
    done
}

# Function to generate performance summary
generate_performance_summary() {
    local monitor_log="$1"
    
    if [ -s "$monitor_log" ] && [ $(wc -l < "$monitor_log") -gt 1 ]; then
        echo -e "\n===== Final Performance Summary ====="
        echo "Average CPU Usage: $(awk -F',' 'NR>1 {sum+=$2} END {if(NR>1) printf "%.1f", sum/(NR-1); else print "N/A"}' "$monitor_log")%"
        echo "Peak CPU Usage: $(awk -F',' 'NR>1 {if($2>max)max=$2} END {printf "%.1f", max}' "$monitor_log")%"
        echo "Average Memory Usage: $(awk -F',' 'NR>1 {sum+=$3} END {if(NR>1) printf "%.1f", sum/(NR-1); else print "N/A"}' "$monitor_log")%"
        echo "Peak Memory Usage: $(awk -F',' 'NR>1 {if($3>max)max=$3} END {printf "%.1f", max}' "$monitor_log")%"
        echo "Average Disk I/O: $(awk -F',' 'NR>1 {sum+=$4} END {if(NR>1) printf "%.1f", sum/(NR-1); else print "N/A"}' "$monitor_log") kB/s"
        echo "Peak Disk I/O: $(awk -F',' 'NR>1 {if($4>max)max=$4} END {printf "%.1f", max}' "$monitor_log") kB/s"
        echo "Average Active Processes: $(awk -F',' 'NR>1 {sum+=$5} END {if(NR>1) printf "%d", sum/(NR-1); else print "N/A"}' "$monitor_log")"
        echo "Peak Active Processes: $(awk -F',' 'NR>1 {if($5>max)max=$5} END {printf "%d", max}' "$monitor_log")"
        
        # Calculate runtime in a way that works on both systems
        start_time=$(head -n2 "$monitor_log" | tail -n1 | cut -d',' -f1)
        end_time=$(tail -n1 "$monitor_log" | cut -d',' -f1)
        
        # Convert timestamps to seconds since epoch
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            start_epoch=$(date -j -f "%Y-%m-%d %H:%M:%S" "$start_time" "+%s" 2>/dev/null)
            end_epoch=$(date -j -f "%Y-%m-%d %H:%M:%S" "$end_time" "+%s" 2>/dev/null)
        else
            # Linux
            start_epoch=$(date -d "$start_time" "+%s" 2>/dev/null)
            end_epoch=$(date -d "$end_time" "+%s" 2>/dev/null)
        fi
        
        # Calculate runtime only if we successfully got both timestamps
        if [ -n "$start_epoch" ] && [ -n "$end_epoch" ]; then
            runtime=$((end_epoch - start_epoch))
            echo "Total Runtime: $runtime seconds"
        else
            echo "Total Runtime: Unable to calculate (timestamp error)"
        fi
        echo "============================================="
    fi
} 