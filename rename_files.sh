#!/usr/bin/env bash

# Function to rename files based on their type
rename_file() {
    local f="$1"
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
}

# Main execution
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <file_to_rename>"
    exit 1
fi

rename_file "$1"