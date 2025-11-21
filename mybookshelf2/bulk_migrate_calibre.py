#!/usr/bin/env python3
"""
Bulk migration script for Calibre to MyBookshelf2
Uses CLI tool (WebSocket) for reliable bulk uploads with FB2 to EPUB conversion
"""

import os
import sys
import json
import logging
import subprocess
import tempfile
import shutil
import hashlib
import sqlite3
import fcntl
from pathlib import Path
from typing import Optional, Dict, Tuple, List, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MyBookshelf2Migrator:
    def __init__(self, calibre_dir: str, container: str = "mybookshelf2_app", 
                 username: str = "admin", password: str = "mypassword123",
                 delete_existing: bool = False, limit: Optional[int] = None,
                 use_symlinks: bool = False, worker_id: Optional[int] = None,
                 db_offset: Optional[int] = None):
        self.calibre_dir = Path(calibre_dir)
        self.container = container
        self.username = username
        self.password = password
        # Use worker-specific progress file if worker_id is provided
        if worker_id is not None:
            self.progress_file = f"migration_progress_worker{worker_id}.json"
            self.error_file = f"migration_errors_worker{worker_id}.log"
        else:
            self.progress_file = "migration_progress.json"
            self.error_file = "migration_errors.log"
        self.temp_dir = tempfile.mkdtemp(prefix="mbs2_migration_")
        self.ebook_convert = "/usr/bin/ebook-convert"
        self.ebook_meta = "/usr/bin/ebook-meta"
        self.delete_existing = delete_existing
        self.limit = limit
        self.use_symlinks = use_symlinks
        self.worker_id = worker_id
        self.db_offset = db_offset  # Starting offset in database query
        
        # Determine docker command
        try:
            result = subprocess.run(['docker', 'ps'], capture_output=True, timeout=5)
            self.docker_cmd = "docker" if result.returncode == 0 else "sudo docker"
        except:
            self.docker_cmd = "sudo docker"
        
        logger.info(f"Using docker command: {self.docker_cmd}")
        
        # Ensure temp directory exists
        os.makedirs(self.temp_dir, exist_ok=True)
    
    def check_container_running(self) -> bool:
        """Check if MyBookshelf2 container is running"""
        try:
            result = subprocess.run(
                [self.docker_cmd, 'ps', '--filter', f'name={self.container}', '--format', '{{.Names}}'],
                capture_output=True,
                text=True,
                timeout=5
            )
            return self.container in result.stdout
        except Exception as e:
            logger.error(f"Error checking container: {e}")
            return False
    
    def delete_all_books(self):
        """Delete all existing books from MyBookshelf2 using correct order for foreign keys"""
        logger.info("Deleting all existing books from MyBookshelf2...")
        try:
            delete_script = f"""
import sys
sys.path.insert(0, '/code')
import os
os.chdir('/code')
from app import app, db
from sqlalchemy import text

with app.app_context():
    count_before = db.session.execute(text("SELECT COUNT(*) FROM ebook")).scalar()
    print(f"Found {{count_before}} ebooks to delete...")
    sys.stdout.flush()
    
    if count_before > 0:
        # Delete in correct order to handle foreign key constraints:
        # 1. Delete conversions (references source)
        # 2. Delete sources (references ebook)
        # 3. Delete ebooks
        db.session.execute(text("DELETE FROM conversion"))
        db.session.execute(text("DELETE FROM source"))
        db.session.execute(text("DELETE FROM ebook"))
        db.session.commit()
        
        count_after = db.session.execute(text("SELECT COUNT(*) FROM ebook")).scalar()
        print(f"Deleted {{count_before}} ebooks. Remaining: {{count_after}}")
        sys.stdout.flush()
    else:
        print("No books to delete.")
"""
            result = subprocess.run(
                [self.docker_cmd, 'exec', self.container, 'python3', '-c', delete_script],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                logger.info(result.stdout.strip())
            else:
                logger.error(f"Error deleting books: {result.stderr}")
        except Exception as e:
            logger.error(f"Error deleting books: {e}")
    
    def get_file_hash(self, file_path: Path) -> str:
        """Calculate SHA1 hash of file for deduplication (matches MyBookshelf2's hash algorithm)"""
        sha1 = hashlib.sha1()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha1.update(chunk)
        return sha1.hexdigest()
    
    def load_progress(self) -> Dict[str, Any]:
        """Load migration progress from file, handling corrupted files with multiple JSON objects"""
        if not os.path.exists(self.progress_file):
            return {"completed_files": {}, "errors": []}
        
        try:
            with open(self.progress_file, 'r') as f:
                content = f.read()
                # If file has multiple JSON objects, try to parse the last one
                if content.strip().count('{') > 1:
                    logger.warning("Progress file contains multiple JSON objects, attempting to parse last one")
                    # Find the last complete JSON object
                    last_brace = content.rfind('}')
                    if last_brace > 0:
                        # Find the matching opening brace
                        brace_count = 0
                        start_pos = last_brace
                        for i in range(last_brace, -1, -1):
                            if content[i] == '}':
                                brace_count += 1
                            elif content[i] == '{':
                                brace_count -= 1
                                if brace_count == 0:
                                    start_pos = i
                                    break
                        content = content[start_pos:last_brace+1]
                
                return json.loads(content)
        except Exception as e:
            logger.warning(f"Error loading progress file: {e}. Starting fresh.")
            return {"completed_files": {}, "errors": []}
    
    def save_progress(self, progress: Dict[str, Any]):
        """Save migration progress to file using atomic write with file locking"""
        try:
            # Get progress file path as string
            progress_file_str = str(self.progress_file)
            # Create temp file name
            if progress_file_str.endswith('.json'):
                temp_file_str = progress_file_str[:-5] + '.tmp'
            else:
                temp_file_str = progress_file_str + '.tmp'
            
            # Atomic write: write to temp file first, then rename
            with open(temp_file_str, 'w') as f:
                # Acquire exclusive lock
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    json.dump(progress, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())  # Ensure data is written to disk
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            
            # Atomic rename (this is atomic on POSIX systems)
            os.replace(temp_file_str, progress_file_str)
        except Exception as e:
            logger.error(f"Error saving progress file: {e}")
    
    def extract_metadata_from_file(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from ebook file using ebook-meta"""
        metadata = {}
        try:
            result = subprocess.run(
                [self.ebook_meta, str(file_path)],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                output = result.stdout
                # Parse metadata from ebook-meta output
                for line in output.split('\n'):
                    line = line.strip()
                    if line.startswith('Title:'):
                        metadata['title'] = line.split(':', 1)[1].strip()
                    elif line.startswith('Author(s):'):
                        authors = line.split(':', 1)[1].strip()
                        metadata['authors'] = [a.strip() for a in authors.split('&') if a.strip()]
                    elif line.startswith('Language:'):
                        lang = line.split(':', 1)[1].strip().lower()
                        # Fix common language code issues
                        if lang == 'rus':
                            lang = 'ru'
                        metadata['language'] = lang
                    elif line.startswith('Series:'):
                        metadata['series'] = line.split(':', 1)[1].strip()
                    elif line.startswith('Series Index:'):
                        try:
                            metadata['series_index'] = float(line.split(':', 1)[1].strip())
                        except:
                            pass
        except Exception as e:
            logger.warning(f"Error extracting metadata from {file_path}: {e}")
        
        return metadata
    
    def convert_fb2_to_epub(self, fb2_path: Path) -> Tuple[Optional[Path], Dict[str, Any]]:
        """Convert FB2 file to EPUB format"""
        # Extract metadata from FB2 first (try original file first)
        metadata = self.extract_metadata_from_file(fb2_path)
        
        # Create output path
        epub_path = Path(self.temp_dir) / f"{fb2_path.stem}.epub"
        
        try:
            logger.info(f"Converting FB2 to EPUB: {fb2_path.name}")
            result = subprocess.run(
                [self.ebook_convert, str(fb2_path), str(epub_path)],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode != 0:
                logger.error(f"Conversion failed: {result.stderr}")
                return None, metadata
            
            if not epub_path.exists():
                logger.error(f"Conversion failed: EPUB file not created")
                return None, metadata
            
            # If metadata was incomplete from FB2, try extracting from converted EPUB
            if not metadata.get('title') or not metadata.get('language'):
                epub_metadata = self.extract_metadata_from_file(epub_path)
                # Merge metadata, preferring FB2 values but filling gaps from EPUB
                for key in ['title', 'authors', 'language', 'series', 'series_index']:
                    if not metadata.get(key) and key in epub_metadata:
                        metadata[key] = epub_metadata[key]
            
            # Fix language code (rus -> ru)
            if metadata.get('language') == 'rus':
                metadata['language'] = 'ru'
            
            # If still no language, default to 'ru' for Russian books
            if not metadata.get('language'):
                metadata['language'] = 'ru'
            
            logger.info(f"Conversion successful: {epub_path.name}")
            return epub_path, metadata
            
        except subprocess.TimeoutExpired:
            logger.error(f"Conversion timeout for {fb2_path.name}")
            return None, metadata
        except Exception as e:
            logger.error(f"Error converting {fb2_path.name}: {e}")
            return None, metadata
    
    def prepare_file_for_upload(self, file_path: Path) -> Tuple[Path, bool, Dict[str, Any]]:
        """
        Prepare file for upload. Returns (file_path_to_upload, is_temp_file, metadata_dict)
        Matches the logic from quick_migrate_10.sh
        In symlink mode, uses original file format without conversion.
        """
        file_ext = file_path.suffix.lower()
        metadata = {}
        is_temp = False
        upload_file = file_path
        
        # In symlink mode, skip conversion and use original file format
        if self.use_symlinks:
            # Extract metadata from original file
            metadata = self.extract_metadata_from_file(file_path)
            # Fix language code
            if metadata.get('language') == 'rus':
                metadata['language'] = 'ru'
            if not metadata.get('language'):
                metadata['language'] = 'ru'
            return upload_file, False, metadata
        
        # Check if file needs conversion to EPUB
        if file_ext not in ['.epub']:
            # Convert to EPUB (FB2, MOBI, PDF, etc.)
            if file_ext == '.fb2':
                epub_path, metadata = self.convert_fb2_to_epub(file_path)
                if epub_path and epub_path.exists():
                    upload_file = epub_path
                    is_temp = True
                else:
                    logger.error(f"Failed to convert FB2 file: {file_path}")
                    return file_path, False, {}
            else:
                # Convert other formats (MOBI, PDF, etc.) to EPUB
                epub_path = Path(self.temp_dir) / f"{file_path.stem}.epub"
                try:
                    result = subprocess.run(
                        [self.ebook_convert, str(file_path), str(epub_path)],
                        capture_output=True,
                        text=True,
                        timeout=300
                    )
                    if result.returncode == 0 and epub_path.exists():
                        upload_file = epub_path
                        is_temp = True
                        # Extract metadata from converted EPUB
                        metadata = self.extract_metadata_from_file(epub_path)
                    else:
                        logger.error(f"Failed to convert {file_ext} file: {file_path}")
                        return file_path, False, {}
                except Exception as e:
                    logger.error(f"Error converting {file_ext} file: {e}")
                    return file_path, False, {}
        
        # Extract metadata - try original file first (if it was converted)
        if is_temp and file_path.exists():
            # Try extracting from original file first
            orig_metadata = self.extract_metadata_from_file(file_path)
            # Merge, preferring original values
            for key in ['title', 'authors', 'language', 'series', 'series_index']:
                if orig_metadata.get(key) and not metadata.get(key):
                    metadata[key] = orig_metadata[key]
        
        # If metadata is incomplete, try extracting from converted EPUB
        if not metadata.get('title') or not metadata.get('language'):
            if is_temp and upload_file.exists():
                epub_metadata = self.extract_metadata_from_file(upload_file)
                for key in ['title', 'authors', 'language', 'series', 'series_index']:
                    if not metadata.get(key) and epub_metadata.get(key):
                        metadata[key] = epub_metadata[key]
        
        # Fix language code (rus -> ru)
        if metadata.get('language') == 'rus':
            metadata['language'] = 'ru'
        
        # If still no language, default to 'ru' for Russian books
        if not metadata.get('language'):
            metadata['language'] = 'ru'
        
        # For non-converted files, extract metadata if not already done
        if not metadata and not is_temp:
            metadata = self.extract_metadata_from_file(file_path)
            # Fix language
            if metadata.get('language') == 'rus':
                metadata['language'] = 'ru'
            if not metadata.get('language'):
                metadata['language'] = 'ru'
        
        return upload_file, is_temp, metadata
    
    def upload_file(self, file_path: Path, original_file_hash: str, progress: Dict[str, Any]) -> bool:
        """Upload a single file to MyBookshelf2 using CLI"""
        # Check if already completed
        if original_file_hash in progress.get("completed_files", {}):
            logger.info(f"Skipping already uploaded file: {file_path.name}")
            return True
        
        # Prepare file (convert FB2 if needed)
        upload_path, is_temp_file, metadata = self.prepare_file_for_upload(file_path)
        
        if not upload_path.exists():
            logger.error(f"File does not exist: {upload_path}")
            return False
        
        # Store original Calibre file path for symlink creation (if in symlink mode)
        original_calibre_path = None
        calibre_container_path = None
        if self.use_symlinks and not is_temp_file:
            # Use the original file path (not converted temp file)
            original_calibre_path = file_path
            # Calculate container path for Calibre file (mounted at /calibre_library)
            try:
                calibre_rel_path = str(file_path.relative_to(self.calibre_dir))
                calibre_container_path = f"/calibre_library/{calibre_rel_path}"
            except ValueError:
                logger.warning(f"File {file_path} is not under {self.calibre_dir}, cannot use symlink mode")
                calibre_container_path = None
        
        # In symlink mode with original file, skip docker cp and use Calibre library directly
        # But we still need to upload for hash/metadata extraction, so we'll use the Calibre path
        if self.use_symlinks and calibre_container_path and not is_temp_file:
            # Verify Calibre file exists in container (with timeout handling)
            try:
                check_cmd = [self.docker_cmd, 'exec', self.container, 'test', '-f', calibre_container_path]
                check_result = subprocess.run(check_cmd, capture_output=True, timeout=5)
                if check_result.returncode == 0:
                    # File exists in container, use it directly (skip docker cp)
                    container_path = calibre_container_path
                    logger.debug(f"Using Calibre library file directly: {container_path}")
                else:
                    # File doesn't exist or check failed, fallback to copy
                    logger.debug(f"Calibre file not accessible in container, will copy: {calibre_container_path}")
            except subprocess.TimeoutExpired:
                # Timeout checking file - assume it doesn't exist or container is slow, fallback to copy
                logger.debug(f"Timeout checking Calibre file in container, will copy: {calibre_container_path}")
            except Exception as e:
                # Any other error, fallback to copy
                logger.debug(f"Error checking Calibre file in container ({e}), will copy: {calibre_container_path}")
            
            # If we didn't set container_path to calibre_container_path above, fallback to copy
            if container_path != calibre_container_path:
                # Fallback: copy file to container
                logger.warning(f"Calibre file not found in container at {calibre_container_path}, falling back to copy")
                container_path = f"/tmp/{upload_path.name}"
                try:
                    copy_cmd = [self.docker_cmd, 'cp', str(upload_path), f"{self.container}:{container_path}"]
                    subprocess.run(copy_cmd, check=True, timeout=60)
                except Exception as e:
                    logger.error(f"Failed to copy file to container: {e}")
                    return False
        else:
            # Normal mode: always copy file to container first
            container_path = f"/tmp/{upload_path.name}"
            try:
                copy_cmd = [self.docker_cmd, 'cp', str(upload_path), f"{self.container}:{container_path}"]
                subprocess.run(copy_cmd, check=True, timeout=60)
            except Exception as e:
                logger.error(f"Failed to copy file to container: {e}")
                return False
        
        # Build CLI command with container path
        upload_cmd = [
            self.docker_cmd, 'exec', self.container,
            'python3', 'cli/mbs2.py',
            '-u', self.username,
            '-p', self.password,
            '--ws-url', 'ws://mybookshelf2_backend:8080/ws',
            '--api-url', 'http://localhost:6006',
            'upload',
            '--file', container_path
        ]
        
        # Add metadata flags if available
        if metadata.get('title'):
            upload_cmd.extend(['--title', metadata['title']])
        
        if metadata.get('authors'):
            for author in metadata['authors'][:20]:  # Limit to 20 authors
                upload_cmd.extend(['--author', author])
        
        if metadata.get('language'):
            upload_cmd.extend(['--language', metadata['language']])
        
        if metadata.get('series'):
            upload_cmd.extend(['--series', metadata['series']])
            if metadata.get('series_index') is not None:
                upload_cmd.extend(['--series-index', str(metadata['series_index'])])
        
        # In symlink mode, pass the Calibre file path so API can create symlink instead of copying
        if self.use_symlinks and calibre_container_path and not is_temp_file:
            upload_cmd.extend(['--original-file-path', calibre_container_path])
        
        # Note: We don't pass genres to avoid validation errors
        # The CLI will set genres to empty array if not provided
        
        try:
            
            logger.info(f"Uploading: {file_path.name}")
            result = subprocess.run(
                upload_cmd,
                capture_output=True,
                text=True,
                timeout=600  # 10 minutes for metadata processing
            )
            
            # Clean up copied file from container (only if we copied it, not if using Calibre library directly)
            if container_path != calibre_container_path:
                try:
                    subprocess.run(
                        [self.docker_cmd, 'exec', self.container, 'rm', '-f', container_path],
                        capture_output=True,
                        timeout=10
                    )
                except:
                    pass
            
            # Clean up temp file if it was created
            if is_temp_file and upload_path.exists():
                try:
                    upload_path.unlink()
                except:
                    pass
            
            if result.returncode == 0:
                logger.info(f"Successfully uploaded: {file_path.name}")
                
                # In symlink mode:
                # - If we used Calibre library directly (calibre_container_path), API should have created symlink
                # - If we copied the file, we need to replace it with symlink
                if self.use_symlinks and original_calibre_path:
                    if calibre_container_path and container_path == calibre_container_path:
                        # We used Calibre library directly, API should have created symlink
                        logger.debug(f"Symlink should have been created by API for {file_path.name}")
                    else:
                        # We copied the file, so replace it with symlink
                        self._replace_with_symlink(original_calibre_path, original_file_hash, metadata)
                
                progress["completed_files"][original_file_hash] = {
                    "file": str(file_path),
                    "uploaded_at": str(Path(file_path).stat().st_mtime)
                }
                self.save_progress(progress)
                return True
            else:
                error_msg = result.stderr or result.stdout
                logger.error(f"Upload failed for {file_path.name}: {error_msg}")
                
                # Handle specific error cases
                if ("already exists" in error_msg.lower() or 
                    "duplicate" in error_msg.lower() or 
                    "already in db" in error_msg.lower() or
                    "SoftActionError" in error_msg):
                    logger.info(f"File already exists in MyBookshelf2: {file_path.name}")
                    progress["completed_files"][original_file_hash] = {
                        "file": str(file_path),
                        "status": "already_exists"
                    }
                    self.save_progress(progress)
                    return True
                
                if "insufficient metadata" in error_msg.lower() or "we need at least title and language" in error_msg.lower():
                    logger.warning(f"Insufficient metadata for {file_path.name}, skipping")
                    progress["completed_files"][original_file_hash] = {
                        "file": str(file_path),
                        "status": "insufficient_metadata"
                    }
                    self.save_progress(progress)
                    return True
                
                # Log error
                with open(self.error_file, 'a') as f:
                    f.write(f"{file_path}: {error_msg}\n")
                
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"Upload timeout for {file_path.name}")
            return False
        except Exception as e:
            logger.error(f"Error uploading {file_path.name}: {e}")
            return False
    
    def find_ebook_files_from_database(self, completed_hashes: set = None) -> List[Path]:
        """Find ebook files by querying Calibre database instead of filesystem scanning.
        This is MUCH faster for large libraries (milliseconds vs hours).
        
        Args:
            completed_hashes: Set of file hashes to exclude (already processed files)
        """
        db_path = self.calibre_dir / "metadata.db"
        if not db_path.exists():
            logger.error(f"Calibre metadata.db not found at {db_path}")
            logger.warning("Falling back to filesystem scanning...")
            return self._find_ebook_files_filesystem(completed_hashes)
        
        logger.info("Querying Calibre database for book files (fast method)...")
        
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            # Query for all book files with their paths
            # Calibre stores: books.path (relative path like "Author Name/Book Title (123)") 
            # and data.name (filename without extension) and data.format (uppercase extension)
            # We need to keep fetching until we have enough NEW files (not already completed)
            
            base_query = """
                SELECT b.path, d.name, d.format
                FROM books b
                JOIN data d ON b.id = d.book
                WHERE d.format IN ('EPUB', 'PDF', 'FB2', 'MOBI', 'AZW3', 'TXT')
                ORDER BY b.id
            """
            
            # Build file paths and verify they exist, filtering out completed files
            files = []
            missing_count = 0
            skipped_completed = 0
            offset = 0
            batch_size = 1000  # Fetch in batches of 1000
            max_fetched = 0
            
            # Start from db_offset if provided (for parallel workers)
            if self.db_offset is not None:
                offset = self.db_offset
                logger.info(f"Starting from database offset: {offset:,}")
            
            # Process in batches for incremental progress
            # When limit is set (e.g., 10k), process that many NEW files, not all rows
            db_batch_size = 1000  # Fetch from DB in batches of 1000
            process_batch_size = 100  # Log progress every 100 files
            last_progress_log = 0
            
            # If we have a limit, we need to fetch batches until we have enough NEW files
            # This is because many files may already be completed
            while True:
                # Calculate how many more files we need
                needed = self.limit - len(files) if self.limit else None
                if needed is not None and needed <= 0:
                    break
                
                # Fetch next batch from database
                query = base_query + f" LIMIT {db_batch_size} OFFSET {offset}"
                cursor.execute(query)
                rows = cursor.fetchall()
                
                if not rows:
                    # No more rows in database
                    break
                
                max_fetched += len(rows)
                
                # Process this batch with progress updates
                batch_new_files = 0
                for path, name, format_ext in rows:
                    file_path = self.calibre_dir / path / f"{name}.{format_ext.lower()}"
                    
                    if not file_path.exists() or not file_path.is_file():
                        missing_count += 1
                        if missing_count <= 5:
                            logger.debug(f"File not found: {file_path}")
                        continue
                    
                    # Skip if already completed (check hash)
                    if completed_hashes:
                        file_hash = self.get_file_hash(file_path)
                        if file_hash in completed_hashes:
                            skipped_completed += 1
                            continue
                    
                    files.append(file_path)
                    batch_new_files += 1
                    
                    # Log progress every process_batch_size files
                    if len(files) - last_progress_log >= process_batch_size:
                        logger.info(f"Found {len(files):,} new files so far (processed {max_fetched:,} rows, "
                                  f"skipped {skipped_completed:,} completed, {missing_count:,} missing)")
                        last_progress_log = len(files)
                    
                    # Stop if we have enough files (after filtering)
                    if self.limit is not None and self.limit > 0 and len(files) >= self.limit:
                        break
                
                # Log batch completion periodically
                if max_fetched % (db_batch_size * 10) == 0 or batch_new_files > 0:
                    logger.info(f"Processed batch: offset={offset:,}, rows={len(rows)}, "
                              f"new_files={batch_new_files}, total_new={len(files):,}, "
                              f"total_processed={max_fetched:,}, skipped={skipped_completed:,}")
                
                # Move to next batch
                offset += db_batch_size
                
                # Safety limit: don't fetch more than 10x the requested limit
                if self.limit and max_fetched >= self.limit * 10:
                    logger.warning(f"Fetched {max_fetched:,} rows but only found {len(files):,} new files. "
                                 f"Stopping to avoid excessive database queries.")
                    break
            else:
                # Original iterative approach for non-parallel or unlimited queries
                while True:
                    # Calculate how many more files we need
                    needed = self.limit - len(files) if self.limit else None
                    if needed is not None and needed <= 0:
                        break
                    
                    # Fetch next batch
                    query = base_query + f" LIMIT {batch_size} OFFSET {offset}"
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    
                    if not rows:
                        # No more rows in database
                        break
                    
                    max_fetched += len(rows)
                    logger.debug(f"Fetched batch: offset={offset}, rows={len(rows)}, total_fetched={max_fetched}, new_files={len(files)}")
                    
                    # Process this batch
                    batch_new_files = 0
                    for path, name, format_ext in rows:
                        # Calibre path format: "Author Name/Book Title (123)"
                        # File location: calibre_dir/path/name.format
                        file_path = self.calibre_dir / path / f"{name}.{format_ext.lower()}"
                        
                        if not file_path.exists() or not file_path.is_file():
                            missing_count += 1
                            if missing_count <= 5:  # Log first few missing files
                                logger.debug(f"File not found (may have been moved/deleted): {file_path}")
                            continue
                        
                        # Skip if already completed
                        if completed_hashes:
                            file_hash = self.get_file_hash(file_path)
                            if file_hash in completed_hashes:
                                skipped_completed += 1
                                continue
                        
                        files.append(file_path)
                        batch_new_files += 1
                        
                        # Stop if we have enough files (after filtering)
                        if self.limit is not None and self.limit > 0 and len(files) >= self.limit:
                            break
                    
                    # If we got no new files from this batch, we might be done
                    # But continue to next batch in case there are more new files later
                    if batch_new_files == 0 and len(files) > 0:
                        # We have some files but this batch had none - might be a gap, continue
                        pass
                    
                    # Move to next batch
                    offset += batch_size
                    
                    # Safety limit: don't fetch more than 10x the requested limit
                    if self.limit and max_fetched >= self.limit * 10:
                        logger.warning(f"Fetched {max_fetched:,} rows but only found {len(files):,} new files. "
                                     f"Stopping to avoid excessive database queries.")
                        break
            
            logger.info(f"Database query fetched {max_fetched:,} rows, found {len(files):,} new files")
            
            conn.close()
            
            if missing_count > 0:
                logger.warning(f"Found {missing_count:,} files in database that don't exist on filesystem")
            
            if skipped_completed > 0:
                logger.info(f"Skipped {skipped_completed:,} already completed files")
            
            # Final limit check (should already be satisfied, but just in case)
            if self.limit is not None and self.limit > 0:
                files = files[:self.limit]
                logger.info(f"Ready to process {len(files):,} new files (--limit {self.limit})")
            else:
                logger.info(f"Ready to process {len(files):,} ebook files from database")
            
            return files
            
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            logger.warning("Falling back to filesystem scanning...")
            return self._find_ebook_files_filesystem(completed_hashes)
        except Exception as e:
            logger.error(f"Unexpected error querying database: {e}")
            logger.warning("Falling back to filesystem scanning...")
            return self._find_ebook_files_filesystem(completed_hashes)
    
    def _find_ebook_files_filesystem(self, completed_hashes: set = None) -> List[Path]:
        """Fallback method: Find ebook files by scanning filesystem (SLOW for large libraries)
        
        Args:
            completed_hashes: Set of file hashes to exclude (already processed files)
        """
        ebook_extensions = ['.epub', '.fb2', '.pdf', '.mobi', '.azw3', '.txt']
        
        # Warn if no limit is set for large libraries
        if not self.limit or self.limit <= 0:
            logger.warning("WARNING: No --limit specified. This may be VERY SLOW for large libraries!")
            logger.warning("Consider using --limit N to process only N files at a time.")
            response = input("Continue without limit? This may take hours. (yes/no): ")
            if response.lower() != 'yes':
                logger.info("Aborted by user. Use --limit N to specify how many files to process.")
                return []
        
        logger.info("Scanning for ebook files using optimized find (with early termination)...")
        
        # Use system 'find' command with early termination via head
        # This is much faster than scanning the entire directory
        find_cmd = ['find', str(self.calibre_dir), '-type', 'f']
        
        # Build find command with -o (OR) conditions for extensions
        find_patterns = []
        for ext in ebook_extensions:
            find_patterns.extend(['-iname', f'*{ext}'])
        
        # If we have patterns, add them with -o
        if find_patterns:
            # First pattern doesn't need -o
            find_cmd.extend(['(', find_patterns[0], find_patterns[1]])
            # Remaining patterns need -o
            for i in range(2, len(find_patterns), 2):
                find_cmd.extend(['-o', find_patterns[i], find_patterns[i+1]])
            find_cmd.append(')')
        
        try:
            # ALWAYS use find | head for early termination, even without explicit limit
            # This prevents scanning the entire directory
            effective_limit = self.limit if self.limit and self.limit > 0 else 10000  # Default safety limit
            
            logger.info(f"Using find with early termination (limit: {effective_limit})...")
            
            # Use find | head to stop after finding enough files (much faster!)
            find_process = subprocess.Popen(
                find_cmd,
                stdout=subprocess.PIPE,
                text=True,
                bufsize=1  # Line buffered for faster head termination
            )
            head_process = subprocess.Popen(
                ['head', '-n', str(effective_limit)],
                stdin=find_process.stdout,
                stdout=subprocess.PIPE,
                text=True
            )
            find_process.stdout.close()
            
            # Use shorter timeout when limit is set (should be fast)
            timeout = 60 if self.limit and self.limit < 1000 else 300
            stdout, stderr = head_process.communicate(timeout=timeout)
            returncode = head_process.returncode
            
            # Terminate find process if head finished early
            try:
                find_process.terminate()
                find_process.wait(timeout=5)
            except:
                find_process.kill()
            
            if returncode != 0:
                logger.warning(f"Find command returned non-zero")
                # Fallback to Python rglob
                return self._find_ebook_files_fallback()
            
            # Parse output and filter out completed files
            files = []
            skipped_completed = 0
            for line in stdout.strip().split('\n'):
                if line.strip():
                    try:
                        file_path = Path(line.strip())
                        if file_path.exists() and file_path.is_file():
                            # Skip if already completed
                            if completed_hashes:
                                file_hash = self.get_file_hash(file_path)
                                if file_hash in completed_hashes:
                                    skipped_completed += 1
                                    continue
                            
                            files.append(file_path)
                            
                            # Stop if we have enough files (after filtering)
                            if self.limit is not None and self.limit > 0 and len(files) >= self.limit:
                                break
                    except Exception as e:
                        logger.debug(f"Error parsing file path {line}: {e}")
            
            if skipped_completed > 0:
                logger.info(f"Skipped {skipped_completed:,} already completed files")
            
            if self.limit is not None and self.limit > 0:
                files = files[:self.limit]  # Ensure we don't exceed limit
                logger.info(f"Found {len(files)} new ebook files to process (limited to {self.limit})")
            else:
                logger.info(f"Found {len(files)} ebook files (early termination at {effective_limit})")
                logger.warning("Note: This may not be all files. Use --limit for precise control.")
            
            return files
            
        except subprocess.TimeoutExpired:
            logger.error("File scanning timed out. Directory is too large.")
            logger.error("Please use --limit N to process files in smaller batches.")
            return []
        except Exception as e:
            logger.warning(f"Error using find command: {e}")
            if self.limit and self.limit > 0:
                logger.warning("Falling back to Python rglob (slower but more reliable)...")
                return self._find_ebook_files_fallback()
            else:
                logger.error("Cannot use fallback without --limit. Please specify --limit N.")
                return []
    
    def find_ebook_files(self, completed_hashes: set = None) -> List[Path]:
        """Find all ebook files in the Calibre directory.
        Uses database query (fast) with filesystem fallback (slow).
        
        Args:
            completed_hashes: Set of file hashes to exclude (already processed files)
        """
        # Try database first (much faster)
        return self.find_ebook_files_from_database(completed_hashes)
    
    def _replace_with_symlink(self, calibre_file: Path, file_hash: str, metadata: Dict[str, Any]):
        """
        Replace uploaded file with symlink to Calibre library file.
        This is called after successful upload in symlink mode.
        Uses file hash for reliable matching (no find command needed).
        """
        try:
            # Find the uploaded file location in MyBookshelf2 by querying the database
            # Use file hash for most reliable matching, fallback to most recent source
            file_name = calibre_file.name
            file_ext = calibre_file.suffix.lstrip('.')
            
            find_script = f"""
import sys
sys.path.insert(0, '/code')
import os
os.chdir('/code')
from app import app, db
from sqlalchemy import text

with app.app_context():
    # Try to find by hash first (most reliable)
    file_hash = {repr(file_hash)}
    ext = {repr(file_ext)}
    
    # First try: match by hash and extension
    query = text('''
        SELECT s.id, s.location 
        FROM source s
        JOIN format f ON s.format_id = f.id
        WHERE s.hash = :file_hash
        AND f.extension = :ext
        ORDER BY s.id DESC
        LIMIT 1
    ''')
    
    result = db.session.execute(query, {{'file_hash': file_hash, 'ext': ext}})
    row = result.fetchone()
    
    if not row:
        # Fallback: most recent source with matching extension
        query = text('''
            SELECT s.id, s.location 
            FROM source s
            JOIN format f ON s.format_id = f.id
            WHERE f.extension = :ext
            ORDER BY s.id DESC
            LIMIT 1
        ''')
        result = db.session.execute(query, {{'ext': ext}})
        row = result.fetchone()
    
    if row:
        source_id, location = row
        print(f"{{source_id}}|{{location}}")
    else:
        print("NOT_FOUND")
"""
            result = subprocess.run(
                [self.docker_cmd, 'exec', self.container, 'python3', '-c', find_script],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0 and "NOT_FOUND" not in result.stdout:
                parts = result.stdout.strip().split('|')
                if len(parts) == 2:
                    source_id, location = parts
                    # Calculate relative path from Calibre library root
                    # calibre_file is already the full absolute path to the original file
                    try:
                        calibre_rel_path = str(calibre_file.relative_to(self.calibre_dir))
                    except ValueError:
                        # If calibre_file is not relative to calibre_dir, use absolute path
                        logger.warning(f"File {calibre_file} is not under {self.calibre_dir}, using absolute path")
                        calibre_rel_path = str(calibre_file).lstrip('/')
                    
                    calibre_container_path = f"/calibre_library/{calibre_rel_path}"
                    
                    # Get MyBookshelf2 data directory
                    books_dir = "/data/books"
                    mybookshelf_file_path = f"{books_dir}/{location}"
                    
                    # Replace file with symlink (single operation, no find command)
                    replace_script = f"""
import os
import sys

calibre_path = "{calibre_container_path}"
mbs2_path = "{mybookshelf_file_path}"

# Verify Calibre file exists in container
if not os.path.exists(calibre_path):
    print(f"ERROR: Calibre file not found in container: {{calibre_path}}")
    sys.exit(1)

if os.path.exists(mbs2_path):
    # Remove the copied file
    os.remove(mbs2_path)
    # Create symlink pointing to Calibre library file (use absolute path)
    os.symlink(calibre_path, mbs2_path)
    print(f"Symlink created: {{mbs2_path}} -> {{calibre_path}}")
    sys.exit(0)
else:
    print(f"ERROR: MyBookshelf2 file not found: {{mbs2_path}}")
    sys.exit(1)
"""
                    replace_result = subprocess.run(
                        [self.docker_cmd, 'exec', self.container, 'python3', '-c', replace_script],
                        capture_output=True,
                        text=True,
                        timeout=30
                    )
                    
                    if replace_result.returncode == 0:
                        logger.info(f"✓ Replaced file with symlink: {calibre_file.name}")
                        logger.debug(f"  Symlink: {mybookshelf_file_path} -> {calibre_container_path}")
                    else:
                        error_msg = replace_result.stderr or replace_result.stdout
                        logger.warning(f"Failed to create symlink for {calibre_file.name}: {error_msg}")
                else:
                    logger.warning(f"Could not parse database result for {calibre_file.name}: {result.stdout}")
            else:
                error_msg = result.stderr or result.stdout
                logger.warning(f"Could not find uploaded file in database for {calibre_file.name}: {error_msg}")
        except Exception as e:
            logger.warning(f"Error creating symlink for {calibre_file.name}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
    
    def _find_ebook_files_fallback(self) -> List[Path]:
        """Fallback method using Python rglob (slower but more reliable)"""
        ebook_extensions = ['.epub', '.fb2', '.pdf', '.mobi', '.azw3', '.txt']
        files = []
        files_set = set()
        
        logger.info("Using Python rglob fallback method...")
        
        target_count = self.limit if self.limit else None
        
        for ext in ebook_extensions:
            if target_count and len(files) >= target_count:
                break
            
            pattern = f"*{ext}"
            found = list(self.calibre_dir.rglob(pattern))
            found.extend(self.calibre_dir.rglob(pattern.upper()))
            
            for f in found:
                if f not in files_set:
                    files_set.add(f)
                    files.append(f)
                    if target_count and len(files) >= target_count:
                        break
        
        if self.limit is not None and self.limit > 0:
            files = files[:self.limit]
            logger.info(f"Found {len(files)} ebook files (limited to {self.limit})")
        else:
            logger.info(f"Found {len(files)} ebook files")
        
        return files
    
    def migrate(self):
        """Main migration function"""
        if not self.calibre_dir.exists():
            logger.error(f"Calibre directory does not exist: {self.calibre_dir}")
            return
        
        if not self.check_container_running():
            logger.error(f"MyBookshelf2 container '{self.container}' is not running")
            return
        
        # MyBookshelf2 has built-in deduplication based on file hash + size
        # Duplicates are automatically detected and skipped during upload
        # No need to delete existing books - the migration can be safely resumed
        
        # Load progress
        progress = self.load_progress()
        completed_hashes = set(progress.get("completed_files", {}).keys())
        completed_count = len(completed_hashes)
        
        # Process in batches per plan (batch size: 10k)
        # Each worker processes its assigned range in batches, continuing until done
        batch_size = self.limit if self.limit else 10000
        total_success = 0
        total_errors = 0
        batch_num = 0
        
        while True:
            batch_num += 1
            logger.info(f"=== Processing batch {batch_num} (batch size: {batch_size:,}) ===")
            
            # Find ebook files for this batch, excluding already completed ones
            files = self.find_ebook_files(completed_hashes=completed_hashes)
            
            if not files:
                logger.info(f"No more new files to process. Migration complete.")
                break
            
            total_new = len(files)
            logger.info(f"Found {total_new:,} new files in batch {batch_num} (total completed: {completed_count:,})")
            
            # Process files in this batch
            success_count = 0
            error_count = 0
            
            for i, file_path in enumerate(files, 1):
                if i % 100 == 0:
                    logger.info(f"Batch {batch_num} progress: {i}/{total_new} files processed")
                
                # Calculate hash for deduplication
                file_hash = self.get_file_hash(file_path)
                
                # Upload file
                if self.upload_file(file_path, file_hash, progress):
                    success_count += 1
                    completed_hashes.add(file_hash)  # Update set for next batch
                else:
                    error_count += 1
            
            total_success += success_count
            total_errors += error_count
            completed_count += success_count
            
            logger.info(f"Batch {batch_num} complete. Success: {success_count}, Errors: {error_count}")
            logger.info(f"Total progress: {total_success:,} successful, {total_errors:,} errors")
            
            # Save progress after each batch (checkpoint per plan)
            self.save_progress(progress)
            
            # If we got fewer files than batch size, we're likely done
            if len(files) < batch_size:
                logger.info(f"Got fewer files than batch size ({len(files)} < {batch_size}), migration complete.")
                break
        
        # Cleanup temp directory
        try:
            shutil.rmtree(self.temp_dir)
        except:
            pass
        
        logger.info(f"Migration complete. Total: {total_success:,} successful, {total_errors:,} errors")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 bulk_migrate_calibre.py <calibre_directory> [container_name] [username] [password] [--limit N] [--use-symlinks] [--worker-id N] [--offset N]")
        print("Example: python3 bulk_migrate_calibre.py /path/to/calibre/library")
        print("         python3 bulk_migrate_calibre.py /path/to/calibre/library mybookshelf2_app admin mypassword123")
        print("         python3 bulk_migrate_calibre.py /path/to/calibre/library --limit 100")
        print("         python3 bulk_migrate_calibre.py /path/to/calibre/library --use-symlinks --limit 100")
        print("         python3 bulk_migrate_calibre.py /path/to/calibre/library --worker-id 1 --offset 0 --limit 10000")
        print("")
        print("Note: MyBookshelf2 has built-in deduplication. Duplicate files are automatically skipped.")
        sys.exit(1)
    
    calibre_dir = sys.argv[1]
    container = "mybookshelf2_app"
    username = "admin"
    password = "mypassword123"
    limit = None
    use_symlinks = False
    worker_id = None
    db_offset = None
    
    # Parse arguments - first pass: extract all --options
    # Second pass: extract positional arguments (container, username, password)
    positional_args = []
    i = 2
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--use-symlinks':
            use_symlinks = True
        elif arg == '--limit':
            if i + 1 < len(sys.argv):
                try:
                    limit = int(sys.argv[i + 1])
                    i += 1  # Skip the limit value
                except ValueError:
                    print(f"Error: --limit requires a number, got '{sys.argv[i + 1]}'")
                    sys.exit(1)
            else:
                print("Error: --limit requires a number")
                sys.exit(1)
        elif arg == '--worker-id':
            if i + 1 < len(sys.argv):
                try:
                    worker_id = int(sys.argv[i + 1])
                    i += 1
                except ValueError:
                    print(f"Error: --worker-id requires a number, got '{sys.argv[i + 1]}'")
                    sys.exit(1)
            else:
                print("Error: --worker-id requires a number")
                sys.exit(1)
        elif arg == '--offset':
            if i + 1 < len(sys.argv):
                try:
                    db_offset = int(sys.argv[i + 1])
                    i += 1
                except ValueError:
                    print(f"Error: --offset requires a number, got '{sys.argv[i + 1]}'")
                    sys.exit(1)
            else:
                print("Error: --offset requires a number")
                sys.exit(1)
        elif not arg.startswith('--'):
            # Collect positional arguments for second pass
            positional_args.append(arg)
        i += 1
    
    # Second pass: assign positional arguments
    if len(positional_args) >= 1:
        container = positional_args[0]
    if len(positional_args) >= 2:
        username = positional_args[1]
    if len(positional_args) >= 3:
        password = positional_args[2]
    
    migrator = MyBookshelf2Migrator(calibre_dir, container, username, password, False, limit, use_symlinks, worker_id, db_offset)
    migrator.migrate()


if __name__ == "__main__":
    main()
