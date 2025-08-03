#!/usr/bin/env python3

import json
import sys
import argparse
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgreSQLImporter:
    def __init__(self, connection_string):
        self.conn_string = connection_string
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(self.conn_string)
            self.conn.autocommit = False
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            logger.info("Connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from database"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Disconnected from database")
    
    def import_calibre_data(self, json_file_path, batch_size=1000, clear_existing=False):
        """Import Calibre data from JSON file into PostgreSQL"""
        
        start_time = time.time()
        
        # Load JSON data
        logger.info(f"Loading JSON data from: {json_file_path}")
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        books = data.get('books', [])
        total_books = len(books)
        logger.info(f"Found {total_books:,} books to import")
        
        if clear_existing:
            logger.info("Clearing existing data...")
            self._clear_all_data()
        
        try:
            # Import in batches for better performance
            imported_count = 0
            
            for i in range(0, total_books, batch_size):
                batch_books = books[i:i + batch_size]
                logger.info(f"Importing batch {i//batch_size + 1}: books {i+1:,} - {min(i + batch_size, total_books):,}")
                
                batch_imported = self._import_book_batch(batch_books)
                imported_count += batch_imported
                
                # Commit each batch
                self.conn.commit()
                
                # Progress update every 10 batches
                if (i // batch_size + 1) % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = imported_count / elapsed if elapsed > 0 else 0
                    logger.info(f"Progress: {imported_count:,}/{total_books:,} books ({rate:.1f} books/sec)")
            
            # Record sync status
            self._record_sync_status(data, imported_count)
            
            # Refresh materialized views
            logger.info("Refreshing materialized views...")
            self._refresh_materialized_views()
            
            self.conn.commit()
            
            elapsed = time.time() - start_time
            rate = imported_count / elapsed if elapsed > 0 else 0
            logger.info(f"✅ Import completed: {imported_count:,} books in {elapsed:.1f}s ({rate:.1f} books/sec)")
            
            return imported_count
            
        except Exception as e:
            logger.error(f"Import failed: {e}")
            self.conn.rollback()
            raise
    
    def _clear_all_data(self):
        """Clear all existing data from tables"""
        tables = [
            'custom_columns', 'book_identifiers', 'book_formats', 
            'book_languages', 'book_tags', 'book_publishers', 
            'book_series', 'book_authors', 'books',
            'authors', 'series', 'publishers', 'tags'
        ]
        
        for table in tables:
            self.cursor.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
        
        logger.info("Cleared existing data")
    
    def _import_book_batch(self, books: List[Dict]) -> int:
        """Import a batch of books with all related data"""
        
        # Collect all unique entities first
        authors_set = set()
        series_set = set()
        publishers_set = set()
        tags_set = set()
        languages_set = set()
        
        for book in books:
            if book.get('authors'):
                authors_set.update(book['authors'])
            if book.get('series_name'):
                series_set.add(book['series_name'])
            if book.get('publisher'):
                publishers_set.add(book['publisher'])
            if book.get('tags'):
                tags_set.update(book['tags'])
            if book.get('languages'):
                languages_set.update(book['languages'])
        
        # Insert unique entities in bulk
        author_id_map = self._bulk_insert_authors(list(authors_set))
        series_id_map = self._bulk_insert_series(list(series_set))
        publisher_id_map = self._bulk_insert_publishers(list(publishers_set))
        tag_id_map = self._bulk_insert_tags(list(tags_set))
        language_id_map = self._bulk_insert_languages(list(languages_set))
        
        # Insert books
        book_data = []
        for book in books:
            book_record = self._prepare_book_record(book)
            if book_record:
                book_data.append(book_record)
        
        if book_data:
            self._bulk_insert_books(book_data)
        
        # Insert relationships
        self._insert_book_relationships(books, author_id_map, series_id_map, 
                                      publisher_id_map, tag_id_map, language_id_map)
        
        # Insert formats and identifiers
        self._insert_book_formats(books)
        self._insert_book_identifiers(books)
        self._insert_custom_columns(books)
        
        return len(books)
    
    def _bulk_insert_authors(self, authors: List[str]) -> Dict[str, int]:
        """Bulk insert authors and return name->id mapping"""
        if not authors:
            return {}
        
        # Insert new authors
        insert_data = [(name,) for name in authors]
        
        self.cursor.executemany("""
            INSERT INTO authors (name) VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """, insert_data)
        
        # Get all author IDs
        format_strings = ','.join(['%s'] * len(authors))
        self.cursor.execute(f"""
            SELECT id, name FROM authors WHERE name IN ({format_strings})
        """, authors)
        
        return {row['name']: row['id'] for row in self.cursor.fetchall()}
    
    def _bulk_insert_series(self, series_names: List[str]) -> Dict[str, int]:
        """Bulk insert series and return name->id mapping"""
        if not series_names:
            return {}
        
        insert_data = [(name,) for name in series_names]
        
        self.cursor.executemany("""
            INSERT INTO series (name) VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """, insert_data)
        
        format_strings = ','.join(['%s'] * len(series_names))
        self.cursor.execute(f"""
            SELECT id, name FROM series WHERE name IN ({format_strings})
        """, series_names)
        
        return {row['name']: row['id'] for row in self.cursor.fetchall()}
    
    def _bulk_insert_publishers(self, publishers: List[str]) -> Dict[str, int]:
        """Bulk insert publishers and return name->id mapping"""
        if not publishers:
            return {}
        
        insert_data = [(name,) for name in publishers]
        
        self.cursor.executemany("""
            INSERT INTO publishers (name) VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """, insert_data)
        
        format_strings = ','.join(['%s'] * len(publishers))
        self.cursor.execute(f"""
            SELECT id, name FROM publishers WHERE name IN ({format_strings})
        """, publishers)
        
        return {row['name']: row['id'] for row in self.cursor.fetchall()}
    
    def _bulk_insert_tags(self, tags: List[str]) -> Dict[str, int]:
        """Bulk insert tags and return name->id mapping"""
        if not tags:
            return {}
        
        insert_data = [(name,) for name in tags]
        
        self.cursor.executemany("""
            INSERT INTO tags (name) VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """, insert_data)
        
        format_strings = ','.join(['%s'] * len(tags))
        self.cursor.execute(f"""
            SELECT id, name FROM tags WHERE name IN ({format_strings})
        """, tags)
        
        return {row['name']: row['id'] for row in self.cursor.fetchall()}
    
    def _bulk_insert_languages(self, language_codes: List[str]) -> Dict[str, int]:
        """Bulk insert languages and return code->id mapping"""
        if not language_codes:
            return {}
        
        # Languages should already exist from schema initialization
        format_strings = ','.join(['%s'] * len(language_codes))
        self.cursor.execute(f"""
            SELECT id, code FROM languages WHERE code IN ({format_strings})
        """, language_codes)
        
        existing = {row['code']: row['id'] for row in self.cursor.fetchall()}
        
        # Insert any missing language codes
        missing = set(language_codes) - set(existing.keys())
        if missing:
            insert_data = [(code, code.upper()) for code in missing]
            self.cursor.executemany("""
                INSERT INTO languages (code, name) VALUES (%s, %s)
                ON CONFLICT (code) DO NOTHING
            """, insert_data)
            
            # Re-fetch to get new IDs
            format_strings = ','.join(['%s'] * len(missing))
            self.cursor.execute(f"""
                SELECT id, code FROM languages WHERE code IN ({format_strings})
            """, list(missing))
            
            for row in self.cursor.fetchall():
                existing[row['code']] = row['id']
        
        return existing
    
    def _prepare_book_record(self, book: Dict) -> Optional[Tuple]:
        """Prepare book record for insertion"""
        try:
            # Convert dates
            timestamp = None
            if book.get('timestamp'):
                try:
                    timestamp = datetime.fromisoformat(book['timestamp'].replace('Z', '+00:00'))
                except:
                    pass
            
            pubdate = None
            if book.get('pubdate'):
                try:
                    pubdate = datetime.fromisoformat(book['pubdate']).date()
                except:
                    pass
            
            last_modified = None
            if book.get('last_modified'):
                try:
                    last_modified = datetime.fromisoformat(book['last_modified'].replace('Z', '+00:00'))
                except:
                    pass
            
            return (
                book['id'],  # id
                book['id'],  # calibre_id
                book.get('title', ''),
                book.get('sort'),
                book.get('uuid'),
                book.get('isbn'),
                book.get('lccn'),
                book.get('path'),
                timestamp,
                pubdate,
                last_modified,
                book.get('series_index'),
                book.get('rating'),
                book.get('comments'),
                book.get('author_sort'),
                book.get('has_cover', False)
            )
        except Exception as e:
            logger.warning(f"Failed to prepare book record for ID {book.get('id')}: {e}")
            return None
    
    def _bulk_insert_books(self, book_data: List[Tuple]):
        """Bulk insert books"""
        if not book_data:
            return
        
        self.cursor.executemany("""
            INSERT INTO books (
                id, calibre_id, title, sort_title, uuid, isbn, lccn, path,
                timestamp, pubdate, last_modified, series_index, rating,
                comments, author_sort, has_cover
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                sort_title = EXCLUDED.sort_title,
                uuid = EXCLUDED.uuid,
                isbn = EXCLUDED.isbn,
                lccn = EXCLUDED.lccn,
                path = EXCLUDED.path,
                timestamp = EXCLUDED.timestamp,
                pubdate = EXCLUDED.pubdate,
                last_modified = EXCLUDED.last_modified,
                series_index = EXCLUDED.series_index,
                rating = EXCLUDED.rating,
                comments = EXCLUDED.comments,
                author_sort = EXCLUDED.author_sort,
                has_cover = EXCLUDED.has_cover,
                updated_at = CURRENT_TIMESTAMP
        """, book_data)
    
    def _insert_book_relationships(self, books: List[Dict], author_id_map: Dict[str, int],
                                 series_id_map: Dict[str, int], publisher_id_map: Dict[str, int],
                                 tag_id_map: Dict[str, int], language_id_map: Dict[str, int]):
        """Insert all book relationships"""
        
        # Book-Author relationships
        book_author_data = []
        for book in books:
            book_id = book['id']
            for author_name in book.get('authors', []):
                if author_name in author_id_map:
                    book_author_data.append((book_id, author_id_map[author_name]))
        
        if book_author_data:
            self.cursor.executemany("""
                INSERT INTO book_authors (book_id, author_id) VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, book_author_data)
        
        # Book-Series relationships
        book_series_data = []
        for book in books:
            if book.get('series_name') and book['series_name'] in series_id_map:
                book_series_data.append((
                    book['id'], 
                    series_id_map[book['series_name']], 
                    book.get('series_index')
                ))
        
        if book_series_data:
            self.cursor.executemany("""
                INSERT INTO book_series (book_id, series_id, series_index) VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, book_series_data)
        
        # Book-Publisher relationships
        book_publisher_data = []
        for book in books:
            if book.get('publisher') and book['publisher'] in publisher_id_map:
                book_publisher_data.append((book['id'], publisher_id_map[book['publisher']]))
        
        if book_publisher_data:
            self.cursor.executemany("""
                INSERT INTO book_publishers (book_id, publisher_id) VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, book_publisher_data)
        
        # Book-Tag relationships
        book_tag_data = []
        for book in books:
            for tag_name in book.get('tags', []):
                if tag_name in tag_id_map:
                    book_tag_data.append((book['id'], tag_id_map[tag_name]))
        
        if book_tag_data:
            self.cursor.executemany("""
                INSERT INTO book_tags (book_id, tag_id) VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, book_tag_data)
        
        # Book-Language relationships
        book_language_data = []
        for book in books:
            for language_code in book.get('languages', []):
                if language_code in language_id_map:
                    book_language_data.append((book['id'], language_id_map[language_code]))
        
        if book_language_data:
            self.cursor.executemany("""
                INSERT INTO book_languages (book_id, language_id) VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, book_language_data)
    
    def _insert_book_formats(self, books: List[Dict]):
        """Insert book formats"""
        format_data = []
        for book in books:
            book_id = book['id']
            for fmt in book.get('formats', []):
                format_data.append((
                    book_id,
                    fmt.get('format'),
                    fmt.get('filename'),
                    fmt.get('size')
                ))
        
        if format_data:
            self.cursor.executemany("""
                INSERT INTO book_formats (book_id, format, filename, file_size) 
                VALUES (%s, %s, %s, %s)
            """, format_data)
    
    def _insert_book_identifiers(self, books: List[Dict]):
        """Insert book identifiers"""
        identifier_data = []
        for book in books:
            book_id = book['id']
            for id_type, id_value in book.get('identifiers', {}).items():
                identifier_data.append((book_id, id_type, id_value))
        
        if identifier_data:
            self.cursor.executemany("""
                INSERT INTO book_identifiers (book_id, identifier_type, identifier_value) 
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, identifier_data)
    
    def _insert_custom_columns(self, books: List[Dict]):
        """Insert custom column data"""
        custom_data = []
        for book in books:
            book_id = book['id']
            for col_name, col_info in book.get('custom_columns', {}).items():
                custom_data.append((
                    book_id,
                    col_name,
                    str(col_info.get('value', '')),
                    col_info.get('datatype')
                ))
        
        if custom_data:
            self.cursor.executemany("""
                INSERT INTO custom_columns (book_id, column_name, column_value, datatype) 
                VALUES (%s, %s, %s, %s)
            """, custom_data)
    
    def _record_sync_status(self, data: Dict, imported_count: int):
        """Record sync status"""
        self.cursor.execute("""
            INSERT INTO sync_status (
                export_timestamp, books_imported, calibre_library_path
            ) VALUES (%s, %s, %s)
        """, (
            datetime.fromisoformat(data['export_timestamp']),
            imported_count,
            data.get('library_path')
        ))
    
    def _refresh_materialized_views(self):
        """Refresh materialized views"""
        try:
            self.cursor.execute("SELECT refresh_all_materialized_views()")
        except Exception as e:
            logger.warning(f"Failed to refresh materialized views: {e}")

def main():
    parser = argparse.ArgumentParser(description='Import Calibre JSON data to PostgreSQL')
    parser.add_argument('json_file', help='Path to Calibre JSON export file')
    parser.add_argument('--connection', '-c', 
                       default='postgresql://calibre_user:calibre_pass@localhost:5432/calibre_books',
                       help='PostgreSQL connection string')
    parser.add_argument('--batch-size', '-b', type=int, default=1000,
                       help='Batch size for imports (default: 1000)')
    parser.add_argument('--clear', action='store_true',
                       help='Clear existing data before import')
    
    args = parser.parse_args()
    
    if not Path(args.json_file).exists():
        logger.error(f"JSON file not found: {args.json_file}")
        sys.exit(1)
    
    importer = PostgreSQLImporter(args.connection)
    
    try:
        if not importer.connect():
            sys.exit(1)
        
        imported_count = importer.import_calibre_data(
            args.json_file, 
            batch_size=args.batch_size,
            clear_existing=args.clear
        )
        
        logger.info(f"✅ Successfully imported {imported_count:,} books")
        
    except Exception as e:
        logger.error(f"Import failed: {e}")
        sys.exit(1)
        
    finally:
        importer.disconnect()

if __name__ == "__main__":
    main() 