#!/usr/bin/env python3

import json
import sys
import argparse
import sqlite3
from pathlib import Path
from datetime import datetime
import subprocess
import os

class CalibreExporter:
    def __init__(self, library_path):
        self.library_path = Path(library_path)
        self.db_path = self.library_path / "metadata.db"
        
        if not self.db_path.exists():
            raise FileNotFoundError(f"Calibre database not found at {self.db_path}")
    
    def export_metadata(self, output_file=None, batch_size=10000):
        """Export Calibre metadata to JSON format"""
        
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"calibre_export_{timestamp}.json"
        
        print(f"Exporting metadata from: {self.library_path}")
        print(f"Output file: {output_file}")
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        
        try:
            # Get total count for progress tracking
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM books")
            total_books = cursor.fetchone()[0]
            print(f"Total books to export: {total_books:,}")
            
            # Export in batches to handle large libraries
            exported_count = 0
            books_data = []
            
            for offset in range(0, total_books, batch_size):
                print(f"Exporting batch: {offset:,} - {min(offset + batch_size, total_books):,}")
                
                batch_books = self._export_batch(conn, offset, batch_size)
                books_data.extend(batch_books)
                exported_count += len(batch_books)
                
                # Save intermediate results for very large libraries
                if exported_count % 50000 == 0:
                    print(f"Progress: {exported_count:,}/{total_books:,} books exported")
            
            # Save final JSON
            export_data = {
                "export_timestamp": datetime.now().isoformat(),
                "library_path": str(self.library_path),
                "total_books": total_books,
                "books": books_data
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Export completed: {exported_count:,} books exported to {output_file}")
            return output_file
            
        finally:
            conn.close()
    
    def _export_batch(self, conn, offset, batch_size):
        """Export a batch of books with all metadata"""
        
        query = """
        SELECT 
            b.id,
            b.title,
            b.sort,
            b.timestamp,
            b.pubdate,
            b.series_index,
            b.author_sort,
            b.isbn,
            b.lccn,
            b.path,
            b.flags,
            b.uuid,
            b.has_cover,
            b.last_modified,
            
            -- Authors
            GROUP_CONCAT(DISTINCT a.name, ' & ') as authors,
            
            -- Series
            s.name as series_name,
            
            -- Publishers
            p.name as publisher,
            
            -- Tags
            GROUP_CONCAT(DISTINCT t.name, ', ') as tags,
            
            -- Languages
            GROUP_CONCAT(DISTINCT l.lang_code, ', ') as languages,
            
            -- Ratings
            r.rating,
            
            -- Comments
            c.text as comments,
            
            -- Identifiers
            GROUP_CONCAT(DISTINCT i.type || ':' || i.val, ', ') as identifiers
            
        FROM books b
        
        LEFT JOIN books_authors_link bal ON b.id = bal.book
        LEFT JOIN authors a ON bal.author = a.id
        
        LEFT JOIN books_series_link bsl ON b.id = bsl.book
        LEFT JOIN series s ON bsl.series = s.id
        
        LEFT JOIN books_publishers_link bpl ON b.id = bpl.book
        LEFT JOIN publishers p ON bpl.publisher = p.id
        
        LEFT JOIN books_tags_link btl ON b.id = btl.book
        LEFT JOIN tags t ON btl.tag = t.id
        
        LEFT JOIN books_languages_link bll ON b.id = bll.book
        LEFT JOIN languages l ON bll.lang_code = l.id
        
        LEFT JOIN books_ratings_link brl ON b.id = brl.book
        LEFT JOIN ratings r ON brl.rating = r.id
        
        LEFT JOIN comments c ON b.id = c.book
        
        LEFT JOIN identifiers i ON b.id = i.book
        
        GROUP BY b.id
        ORDER BY b.id
        LIMIT ? OFFSET ?
        """
        
        cursor = conn.cursor()
        cursor.execute(query, (batch_size, offset))
        
        books = []
        for row in cursor.fetchall():
            # Get file formats for this book
            formats = self._get_book_formats(conn, row['id'])
            
            # Get custom columns for this book
            custom_columns = self._get_custom_columns(conn, row['id'])
            
            book_data = {
                'id': row['id'],
                'title': row['title'],
                'sort': row['sort'],
                'timestamp': row['timestamp'],
                'pubdate': row['pubdate'],
                'series_index': row['series_index'],
                'author_sort': row['author_sort'],
                'isbn': row['isbn'],
                'lccn': row['lccn'],
                'path': row['path'],
                'uuid': row['uuid'],
                'has_cover': bool(row['has_cover']),
                'last_modified': row['last_modified'],
                'authors': row['authors'].split(' & ') if row['authors'] else [],
                'series_name': row['series_name'],
                'publisher': row['publisher'],
                'tags': row['tags'].split(', ') if row['tags'] else [],
                'languages': row['languages'].split(', ') if row['languages'] else [],
                'rating': row['rating'],
                'comments': row['comments'],
                'identifiers': self._parse_identifiers(row['identifiers']),
                'formats': formats,
                'custom_columns': custom_columns
            }
            books.append(book_data)
        
        return books
    
    def _get_book_formats(self, conn, book_id):
        """Get all file formats for a book"""
        cursor = conn.cursor()
        cursor.execute("""
            SELECT format, uncompressed_size, name
            FROM data 
            WHERE book = ?
        """, (book_id,))
        
        formats = []
        for row in cursor.fetchall():
            formats.append({
                'format': row[0],
                'size': row[1],
                'filename': row[2]
            })
        return formats
    
    def _get_custom_columns(self, conn, book_id):
        """Get custom column values for a book"""
        cursor = conn.cursor()
        
        # First, get all custom column definitions
        cursor.execute("""
            SELECT id, label, name, datatype, display
            FROM custom_columns
        """)
        
        custom_columns = {}
        for col_row in cursor.fetchall():
            col_id, label, name, datatype, display = col_row
            table_name = f"custom_column_{col_id}"
            
            try:
                # Get the value for this book from the custom column table
                cursor.execute(f"""
                    SELECT value FROM {table_name}
                    WHERE book = ?
                """, (book_id,))
                
                result = cursor.fetchone()
                if result:
                    custom_columns[label] = {
                        'name': name,
                        'value': result[0],
                        'datatype': datatype
                    }
            except sqlite3.OperationalError:
                # Table might not exist for this custom column
                continue
        
        return custom_columns
    
    def _parse_identifiers(self, identifiers_str):
        """Parse identifiers string into dictionary"""
        if not identifiers_str:
            return {}
        
        identifiers = {}
        for item in identifiers_str.split(', '):
            if ':' in item:
                key, value = item.split(':', 1)
                identifiers[key] = value
        return identifiers

def main():
    parser = argparse.ArgumentParser(description='Export Calibre metadata to JSON for PostgreSQL import')
    parser.add_argument('library_path', help='Path to Calibre library directory')
    parser.add_argument('-o', '--output', help='Output JSON file (default: calibre_export_TIMESTAMP.json)')
    parser.add_argument('-b', '--batch-size', type=int, default=10000, 
                       help='Batch size for processing (default: 10000)')
    
    args = parser.parse_args()
    
    try:
        exporter = CalibreExporter(args.library_path)
        output_file = exporter.export_metadata(args.output, args.batch_size)
        
        # Show file size
        file_size = Path(output_file).stat().st_size
        print(f"Export file size: {file_size / (1024*1024):.1f} MB")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 