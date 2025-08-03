#!/usr/bin/env python3

import os
import time
import sqlite3
import psycopg2
import psycopg2.extras
from pathlib import Path
from datetime import datetime
import argparse
import logging
import json
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CalibrePostgreSQLSync:
    def __init__(self, calibre_library_path: str, postgres_connection: str):
        self.calibre_library_path = Path(calibre_library_path)
        self.calibre_db_path = self.calibre_library_path / "metadata.db"
        self.postgres_conn_str = postgres_connection
        
        if not self.calibre_db_path.exists():
            raise FileNotFoundError(f"Calibre database not found: {self.calibre_db_path}")
    
    def get_calibre_last_modified(self) -> datetime:
        """Get the last modification time of Calibre database"""
        return datetime.fromtimestamp(self.calibre_db_path.stat().st_mtime)
    
    def get_postgres_last_sync(self) -> Optional[datetime]:
        """Get the last sync timestamp from PostgreSQL"""
        try:
            with psycopg2.connect(self.postgres_conn_str) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT export_timestamp 
                        FROM sync_status 
                        ORDER BY created_at DESC 
                        LIMIT 1
                    """)
                    result = cursor.fetchone()
                    return result['export_timestamp'] if result else None
        except Exception as e:
            logger.error(f"Failed to get last sync time: {e}")
            return None
    
    def needs_sync(self) -> bool:
        """Check if sync is needed"""
        calibre_modified = self.get_calibre_last_modified()
        postgres_last_sync = self.get_postgres_last_sync()
        
        if postgres_last_sync is None:
            logger.info("No previous sync found, full sync needed")
            return True
        
        if calibre_modified > postgres_last_sync:
            logger.info(f"Calibre DB modified at {calibre_modified}, last sync at {postgres_last_sync}")
            return True
        
        logger.info("PostgreSQL is up to date")
        return False
    
    def get_modified_books(self, since: Optional[datetime] = None) -> List[int]:
        """Get list of book IDs modified since given timestamp"""
        with sqlite3.connect(self.calibre_db_path) as conn:
            cursor = conn.cursor()
            
            if since:
                cursor.execute("""
                    SELECT id FROM books 
                    WHERE last_modified > ?
                    ORDER BY id
                """, (since.isoformat(),))
            else:
                cursor.execute("SELECT id FROM books ORDER BY id")
            
            return [row[0] for row in cursor.fetchall()]
    
    def export_book_metadata(self, book_id: int) -> Optional[Dict]:
        """Export metadata for a single book"""
        with sqlite3.connect(self.calibre_db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get basic book info
            cursor.execute("""
                SELECT * FROM books WHERE id = ?
            """, (book_id,))
            book_row = cursor.fetchone()
            
            if not book_row:
                return None
            
            # Get authors
            cursor.execute("""
                SELECT a.name 
                FROM authors a
                JOIN books_authors_link bal ON a.id = bal.author
                WHERE bal.book = ?
            """, (book_id,))
            authors = [row['name'] for row in cursor.fetchall()]
            
            # Get series
            cursor.execute("""
                SELECT s.name 
                FROM series s
                JOIN books_series_link bsl ON s.id = bsl.series
                WHERE bsl.book = ?
            """, (book_id,))
            series_row = cursor.fetchone()
            series_name = series_row['name'] if series_row else None
            
            # Get publisher
            cursor.execute("""
                SELECT p.name 
                FROM publishers p
                JOIN books_publishers_link bpl ON p.id = bpl.publisher
                WHERE bpl.book = ?
            """, (book_id,))
            publisher_row = cursor.fetchone()
            publisher = publisher_row['name'] if publisher_row else None
            
            # Get tags
            cursor.execute("""
                SELECT t.name 
                FROM tags t
                JOIN books_tags_link btl ON t.id = btl.tag
                WHERE btl.book = ?
            """, (book_id,))
            tags = [row['name'] for row in cursor.fetchall()]
            
            # Get languages
            cursor.execute("""
                SELECT l.lang_code 
                FROM languages l
                JOIN books_languages_link bll ON l.id = bll.lang_code
                WHERE bll.book = ?
            """, (book_id,))
            languages = [row['lang_code'] for row in cursor.fetchall()]
            
            # Get formats
            cursor.execute("""
                SELECT format, uncompressed_size, name
                FROM data 
                WHERE book = ?
            """, (book_id,))
            formats = [{'format': row['format'], 'size': row['uncompressed_size'], 
                       'filename': row['name']} for row in cursor.fetchall()]
            
            # Get identifiers
            cursor.execute("""
                SELECT type, val FROM identifiers WHERE book = ?
            """, (book_id,))
            identifiers = {row['type']: row['val'] for row in cursor.fetchall()}
            
            # Get rating
            cursor.execute("""
                SELECT r.rating 
                FROM ratings r
                JOIN books_ratings_link brl ON r.id = brl.rating
                WHERE brl.book = ?
            """, (book_id,))
            rating_row = cursor.fetchone()
            rating = rating_row['rating'] if rating_row else None
            
            # Get comments
            cursor.execute("""
                SELECT text FROM comments WHERE book = ?
            """, (book_id,))
            comments_row = cursor.fetchone()
            comments = comments_row['text'] if comments_row else None
            
            return {
                'id': book_row['id'],
                'title': book_row['title'],
                'sort': book_row['sort'],
                'timestamp': book_row['timestamp'],
                'pubdate': book_row['pubdate'],
                'series_index': book_row['series_index'],
                'author_sort': book_row['author_sort'],
                'isbn': book_row['isbn'],
                'lccn': book_row['lccn'],
                'path': book_row['path'],
                'uuid': book_row['uuid'],
                'has_cover': bool(book_row['has_cover']),
                'last_modified': book_row['last_modified'],
                'authors': authors,
                'series_name': series_name,
                'publisher': publisher,
                'tags': tags,
                'languages': languages,
                'rating': rating,
                'comments': comments,
                'identifiers': identifiers,
                'formats': formats,
                'custom_columns': {}  # TODO: Implement custom columns
            }
    
    def sync_incremental(self) -> int:
        """Perform incremental sync of modified books"""
        postgres_last_sync = self.get_postgres_last_sync()
        modified_books = self.get_modified_books(postgres_last_sync)
        
        if not modified_books:
            logger.info("No books to sync")
            return 0
        
        logger.info(f"Syncing {len(modified_books)} modified books")
        
        # Export modified books to temporary JSON
        export_data = {
            "export_timestamp": datetime.now().isoformat(),
            "library_path": str(self.calibre_library_path),
            "total_books": len(modified_books),
            "books": []
        }
        
        for book_id in modified_books:
            book_data = self.export_book_metadata(book_id)
            if book_data:
                export_data["books"].append(book_data)
        
        # Import to PostgreSQL
        from import_to_postgresql import PostgreSQLImporter
        
        importer = PostgreSQLImporter(self.postgres_conn_str)
        if importer.connect():
            try:
                # Create temporary JSON file
                temp_file = f"/tmp/calibre_sync_{int(time.time())}.json"
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(export_data, f, indent=2, ensure_ascii=False)
                
                # Import the data (without clearing existing data)
                imported_count = importer.import_calibre_data(temp_file, clear_existing=False)
                
                # Clean up temp file
                os.unlink(temp_file)
                
                logger.info(f"✅ Incremental sync completed: {imported_count} books updated")
                return imported_count
                
            finally:
                importer.disconnect()
        else:
            logger.error("Failed to connect to PostgreSQL")
            return 0
    
    def sync_full(self) -> int:
        """Perform full sync using the export/import scripts"""
        logger.info("Starting full sync...")
        
        # Use the existing export script
        from export_calibre_metadata import CalibreExporter
        from import_to_postgresql import PostgreSQLImporter
        
        # Export all metadata
        exporter = CalibreExporter(str(self.calibre_library_path))
        export_file = exporter.export_metadata()
        
        # Import to PostgreSQL
        importer = PostgreSQLImporter(self.postgres_conn_str)
        if importer.connect():
            try:
                imported_count = importer.import_calibre_data(export_file, clear_existing=True)
                logger.info(f"✅ Full sync completed: {imported_count} books imported")
                
                # Clean up export file
                os.unlink(export_file)
                
                return imported_count
            finally:
                importer.disconnect()
        else:
            logger.error("Failed to connect to PostgreSQL")
            return 0
    
    def watch_and_sync(self, check_interval: int = 300):
        """Watch for changes and sync automatically"""
        logger.info(f"Starting watch mode, checking every {check_interval} seconds...")
        
        while True:
            try:
                if self.needs_sync():
                    self.sync_incremental()
                else:
                    logger.debug("No sync needed")
                
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                logger.info("Watch mode stopped by user")
                break
            except Exception as e:
                logger.error(f"Error during sync: {e}")
                time.sleep(check_interval)

def main():
    parser = argparse.ArgumentParser(description='Sync Calibre library with PostgreSQL')
    parser.add_argument('library_path', help='Path to Calibre library directory')
    parser.add_argument('--connection', '-c', 
                       default='postgresql://calibre_user:calibre_pass@localhost:5432/calibre_books',
                       help='PostgreSQL connection string')
    parser.add_argument('--mode', '-m', choices=['check', 'incremental', 'full', 'watch'],
                       default='incremental', help='Sync mode')
    parser.add_argument('--watch-interval', '-w', type=int, default=300,
                       help='Watch mode check interval in seconds (default: 300)')
    
    args = parser.parse_args()
    
    try:
        sync = CalibrePostgreSQLSync(args.library_path, args.connection)
        
        if args.mode == 'check':
            needs_sync = sync.needs_sync()
            print(f"Sync needed: {needs_sync}")
            
        elif args.mode == 'incremental':
            if sync.needs_sync():
                count = sync.sync_incremental()
                print(f"Synced {count} books")
            else:
                print("No sync needed")
                
        elif args.mode == 'full':
            count = sync.sync_full()
            print(f"Full sync completed: {count} books")
            
        elif args.mode == 'watch':
            sync.watch_and_sync(args.watch_interval)
            
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 