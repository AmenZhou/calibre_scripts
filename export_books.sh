# --- Section to export the list of books matching the condition to CSV ---
echo "[INFO] Exporting the list of books matching the condition to filtered_book_list.csv..."
calibredb list --search "(tags:children OR tags:child) AND language:English AND pubdate:<=1928 AND pubdate:*" --fields title,authors,pubdate | awk 'BEGIN{FS=":"; OFS="\t"} {gsub(/"/, "\"\"", $0); print "\""$1"\"","\""$2"\"","\""$3"\""}' >> filtered_book_list.tsv
echo "[INFO] Filtered book list exported to filtered_book_list.csv"
