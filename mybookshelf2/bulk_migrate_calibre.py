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
import time
import threading
import requests
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
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
                 db_offset: Optional[int] = None, parallel_uploads: int = 3,
                 batch_size: int = 1000):
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
        self.parallel_uploads = parallel_uploads  # Number of concurrent uploads per worker
        self.batch_size = batch_size  # Batch size for processing files
        self.api_url = "http://localhost:6006"  # Default API URL, can be overridden
        self.max_retries = 3  # Maximum retries for connection errors
        self.retry_delays = [2, 4, 8]  # Exponential backoff delays in seconds
        self.batch_copy_size = 5  # Number of files to copy in one batch operation
        
        # Thread-safe progress tracking for parallel uploads
        self.progress_lock = threading.Lock()
        
        # API session for file existence checks
        self.api_session = None
        self.api_token = None
        
        # Determine docker command
        try:
            result = subprocess.run(['docker', 'ps'], capture_output=True, timeout=5)
            self.docker_cmd = "docker" if result.returncode == 0 else "sudo docker"
        except:
            self.docker_cmd = "sudo docker"
        
        logger.info(f"Using docker command: {self.docker_cmd}")
        
        # Ensure temp directory exists
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Load existing file hashes from MyBookshelf2 database to avoid duplicate upload attempts
        # This prevents wasting time on files already uploaded by other workers or previous runs
        logger.info("Loading existing file hashes from MyBookshelf2 database...")
        self.existing_hashes = self.load_existing_hashes_from_database()
        logger.info(f"Loaded {len(self.existing_hashes)} existing file hashes from MyBookshelf2 database")
        
        # Track last refresh time for periodic refresh of existing_hashes
        # This prevents stale cache when other workers upload files
        self.last_hash_refresh = time.time()
        self.files_processed_since_refresh = 0
        
        # Performance monitoring
        self.upload_times = []  # Track upload times for performance analysis
        self.slow_upload_threshold = 120  # Log uploads taking more than 2 minutes
    
    def _get_api_session(self) -> Optional[requests.Session]:
        """Get authenticated API session for making HTTP requests"""
        if self.api_session is not None:
            return self.api_session
        
        try:
            # Authenticate and get token
            auth_url = f"{self.api_url}/api/auth/login"
            auth_data = {
                "username": self.username,
                "password": self.password
            }
            response = requests.post(auth_url, json=auth_data, timeout=10)
            response.raise_for_status()
            token_data = response.json()
            self.api_token = token_data.get('access_token')
            
            if not self.api_token:
                logger.warning("Failed to get API token for file checks")
                return None
            
            # Create session with token
            session = requests.Session()
            session.headers['Authorization'] = f'bearer {self.api_token}'
            self.api_session = session
            return session
        except Exception as e:
            logger.debug(f"Failed to create API session: {e}")
            return None
    
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
    
    def check_api_connectivity(self) -> bool:
        """Check if API endpoint is reachable by testing connection to backend container.
        This helps identify connection issues before attempting uploads.
        """
        try:
            # Check if backend container is running and accessible
            check_cmd = [
                self.docker_cmd, 'exec', 'mybookshelf2_backend',
                'python3', '-c', 'import socket; s=socket.socket(); s.settimeout(2); s.connect(("localhost", 9080)); s.close(); print("OK")'
            ]
            result = subprocess.run(
                check_cmd,
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.returncode == 0 and "OK" in result.stdout
        except Exception as e:
            logger.debug(f"API connectivity check failed: {e}")
            return False
    
    def retry_upload(self, upload_func, *args, **kwargs):
        """Retry upload with exponential backoff on connection errors.
        
        Args:
            upload_func: Function to retry (should be upload_file or similar)
            *args, **kwargs: Arguments to pass to upload_func
        
        Returns:
            Result from upload_func, or False if all retries failed
        """
        last_error = None
        for attempt in range(self.max_retries):
            try:
                result = upload_func(*args, **kwargs)
                if result:  # Success
                    return result
                # If upload_func returns False, it's a non-retryable error
                return False
            except subprocess.TimeoutExpired as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                    logger.warning(f"Upload timeout (attempt {attempt + 1}/{self.max_retries}), retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"Upload timeout after {self.max_retries} attempts")
                    return False
            except Exception as e:
                error_str = str(e).lower()
                # Check if it's a connection error (retryable)
                if any(keyword in error_str for keyword in ['connection', 'refused', 'timeout', 'unreachable', 'network']):
                    last_error = e
                    if attempt < self.max_retries - 1:
                        delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                        logger.warning(f"Connection error (attempt {attempt + 1}/{self.max_retries}): {e}, retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"Connection error after {self.max_retries} attempts: {e}")
                        return False
                else:
                    # Non-retryable error, return immediately
                    logger.error(f"Non-retryable error: {e}")
                    return False
        
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
    
    def load_existing_hashes_from_database(self) -> set:
        """Query MyBookshelf2 database for all existing file hashes to avoid duplicate upload attempts.
        Returns set of (hash, size) tuples for fast lookup.
        """
        script = f"""
import sys
import os
sys.path.insert(0, '/code')
os.chdir('/code')
from app import app, db
from app import model

try:
    with app.app_context():
        # Get all existing source hashes and sizes
        sources = db.session.query(model.Source.hash, model.Source.size).all()
        # Return as set of tuples (hash, size) for fast lookup
        result = []
        for hash_val, size in sources:
            result.append(f"{{hash_val}}|{{size}}")
        print('|'.join(result))
except Exception as e:
    import traceback
    print(f"ERROR: {{e}}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
"""
        try:
            result = subprocess.run(
                [self.docker_cmd, 'exec', self.container, 'python3', '-c', script],
                capture_output=True,
                text=True,
                timeout=120  # Allow up to 2 minutes for large databases
            )
            if result.returncode == 0:
                output = result.stdout.strip()
                if output:
                    existing = set()
                    # Output format: hash1|size1|hash2|size2|...
                    parts = output.split('|')
                    # Process pairs: (hash, size)
                    for i in range(0, len(parts) - 1, 2):
                        if i + 1 < len(parts):
                            hash_val = parts[i]
                            try:
                                size = int(parts[i + 1])
                                existing.add((hash_val, size))
                            except (ValueError, IndexError):
                                continue
                    logger.info(f"Successfully loaded {len(existing)} existing file hashes from MyBookshelf2 database")
                    return existing
                else:
                    logger.info("No existing files found in MyBookshelf2 database (this is normal for first migration)")
                    return set()
            else:
                error_msg = result.stderr.strip() if result.stderr else "Unknown error"
                logger.warning(f"Could not load existing hashes from database (returncode {result.returncode}): {error_msg}")
                if result.stdout:
                    logger.debug(f"stdout: {result.stdout[:200]}")
        except subprocess.TimeoutExpired:
            logger.warning("Timeout loading existing hashes from database (database may be large)")
        except Exception as e:
            logger.warning(f"Could not load existing hashes from database: {e}")
        return set()
    
    def refresh_existing_hashes(self):
        """Refresh existing_hashes from database to pick up files uploaded by other workers.
        This prevents stale cache issues when multiple workers are running in parallel.
        """
        logger.info("Refreshing existing file hashes from MyBookshelf2 database...")
        new_hashes = self.load_existing_hashes_from_database()
        old_count = len(self.existing_hashes)
        self.existing_hashes = new_hashes
        new_count = len(self.existing_hashes)
        self.last_hash_refresh = time.time()
        self.files_processed_since_refresh = 0
        logger.info(f"Refreshed existing hashes: {old_count:,} -> {new_count:,} (added {new_count - old_count:,} new hashes)")
    
    def update_existing_hashes(self, file_hash: str, file_size: int):
        """Add a newly uploaded file's hash+size to existing_hashes set.
        This keeps the cache up-to-date without needing a full database refresh.
        """
        self.existing_hashes.add((file_hash, file_size))
        self.files_processed_since_refresh += 1
    
    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to remove NUL characters and other problematic characters.
        PostgreSQL cannot handle NUL (0x00) characters in strings, causing database errors.
        """
        if not filename:
            return filename
        # Remove NUL characters (0x00) - PostgreSQL cannot handle these
        sanitized = filename.replace('\x00', '')
        # Also remove other problematic characters that might cause issues
        # Keep the sanitization minimal to preserve as much of the original filename as possible
        return sanitized
    
    def sanitize_metadata_string(self, value: str) -> str:
        """Sanitize metadata strings (title, authors, series) to remove NUL characters.
        This prevents PostgreSQL errors when storing metadata in the database.
        """
        if not value:
            return value
        # Remove NUL characters (0x00) - PostgreSQL cannot handle these
        return value.replace('\x00', '')
    
    def check_file_exists_via_api(self, file_path: Path, file_hash: str, file_size: int) -> Optional[bool]:
        """Check if file exists via API /api/upload/check.
        Returns True if file exists, False if not, None on error.
        This allows us to skip upload attempts for duplicates.
        """
        session = self._get_api_session()
        if not session:
            return None  # Can't check, proceed with normal flow
        
        try:
            # Get file extension and mime type
            extension = file_path.suffix.lower().lstrip('.')
            mime_type, _ = mimetypes.guess_type(str(file_path))
            if not mime_type:
                mime_type = ''
            
            # Prepare file info for API check
            file_info = {
                'hash': file_hash,
                'size': file_size,
                'mime_type': mime_type,
                'extension': extension
            }
            
            # Call API check endpoint
            check_url = f"{self.api_url}/api/upload/check"
            response = session.post(check_url, json=file_info, timeout=5)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('error') == 'file already exists':
                    return True  # File exists
                return False  # File doesn't exist
            else:
                logger.debug(f"API check returned status {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            logger.debug(f"API check failed for {file_path.name}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Error checking file via API: {e}")
            return None
    
    def batch_copy_files_to_container(self, file_pairs: List[Tuple[Path, str]]) -> Dict[Path, bool]:
        """Batch copy multiple files to container using tar pipe.
        Returns dict mapping file_path -> success (True/False)
        """
        if not file_pairs:
            return {}
        
        results = {}
        
        try:
            # Create tar archive in memory and pipe to docker
            tar_cmd = ['tar', 'cf', '-']
            for file_path, container_path in file_pairs:
                # Get just the filename for tar
                tar_cmd.append(str(file_path))
            
            # Extract to container using tar pipe
            docker_cmd = [
                self.docker_cmd, 'exec', '-i', self.container,
                'tar', 'xf', '-', '-C', '/tmp'
            ]
            
            # Run tar and pipe to docker
            tar_process = subprocess.Popen(
                tar_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(file_pairs[0][0].parent) if file_pairs else None
            )
            
            docker_process = subprocess.Popen(
                docker_cmd,
                stdin=tar_process.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            tar_process.stdout.close()
            
            # Wait for both processes
            docker_stdout, docker_stderr = docker_process.communicate()
            tar_process.wait()
            
            if docker_process.returncode == 0 and tar_process.returncode == 0:
                # Success - all files copied
                for file_path, container_path in file_pairs:
                    results[file_path] = True
                logger.debug(f"Batch copied {len(file_pairs)} files to container")
            else:
                # Batch failed, fallback to individual copies
                logger.warning(f"Batch copy failed, falling back to individual copies: {docker_stderr.decode()}")
                for file_path, container_path in file_pairs:
                    try:
                        copy_cmd = [self.docker_cmd, 'cp', str(file_path), f"{self.container}:{container_path}"]
                        subprocess.run(copy_cmd, check=True, timeout=60, capture_output=True)
                        results[file_path] = True
                    except Exception as e:
                        logger.error(f"Failed to copy {file_path.name} individually: {e}")
                        results[file_path] = False
        except Exception as e:
            logger.error(f"Batch copy error: {e}, falling back to individual copies")
            # Fallback to individual copies
            for file_path, container_path in file_pairs:
                try:
                    copy_cmd = [self.docker_cmd, 'cp', str(file_path), f"{self.container}:{container_path}"]
                    subprocess.run(copy_cmd, check=True, timeout=60, capture_output=True)
                    results[file_path] = True
                except Exception as e2:
                    logger.error(f"Failed to copy {file_path.name} individually: {e2}")
                    results[file_path] = False
        
        return results
    
    def get_file_hash(self, file_path: Path) -> str:
        """Calculate SHA1 hash of file for deduplication (matches MyBookshelf2's hash algorithm)"""
        sha1 = hashlib.sha1()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha1.update(chunk)
        return sha1.hexdigest()
    
    def load_progress(self) -> Dict[str, Any]:
        """Load migration progress from file, handling corrupted files with multiple JSON objects"""
        default_progress = {
            "completed_files": {},
            "errors": [],
            "last_processed_book_id": self.db_offset if self.db_offset else 0
        }
        
        if not os.path.exists(self.progress_file):
            return default_progress
        
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
                
                progress = json.loads(content)
                # Ensure last_processed_book_id exists in loaded progress
                if "last_processed_book_id" not in progress:
                    progress["last_processed_book_id"] = self.db_offset if self.db_offset else 0
                return progress
        except Exception as e:
            logger.warning(f"Error loading progress file: {e}. Starting fresh.")
            return default_progress
    
    def save_progress(self, progress: Dict[str, Any]):
        """Save migration progress to file using atomic write with file locking (thread-safe)"""
        with self.progress_lock:  # Thread-safe progress saving
            try:
                # Get progress file path as string
                progress_file_str = str(self.progress_file)
                # Ensure directory exists
                progress_dir = Path(progress_file_str).parent
                if progress_dir and not progress_dir.exists():
                    progress_dir.mkdir(parents=True, exist_ok=True)
                
                # Create temp file name
                if progress_file_str.endswith('.json'):
                    temp_file_str = progress_file_str[:-5] + '.tmp'
                else:
                    temp_file_str = progress_file_str + '.tmp'
                
                # Atomic write: write to temp file first, then rename
                try:
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
                    # Check if temp file exists before renaming
                    if os.path.exists(temp_file_str):
                        os.replace(temp_file_str, progress_file_str)
                    else:
                        logger.warning(f"Temp file {temp_file_str} was deleted before rename, writing directly")
                        with open(progress_file_str, 'w') as f:
                            json.dump(progress, f, indent=2)
                except OSError as e:
                    # If rename fails, try direct write as fallback
                    logger.warning(f"Atomic write failed ({e}), using direct write")
                    with open(progress_file_str, 'w') as f:
                        json.dump(progress, f, indent=2)
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
                        title = line.split(':', 1)[1].strip()
                        metadata['title'] = self.sanitize_metadata_string(title)
                    elif line.startswith('Author(s):'):
                        authors = line.split(':', 1)[1].strip()
                        metadata['authors'] = [self.sanitize_metadata_string(a.strip()) for a in authors.split('&') if a.strip()]
                    elif line.startswith('Language:'):
                        lang = line.split(':', 1)[1].strip().lower()
                        # Fix common language code issues
                        if lang == 'rus':
                            lang = 'ru'
                        metadata['language'] = lang
                    elif line.startswith('Series:'):
                        series = line.split(':', 1)[1].strip()
                        metadata['series'] = self.sanitize_metadata_string(series)
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
    
    def upload_file(self, file_path: Path, original_file_hash: str, progress: Dict[str, Any], container_path: Optional[str] = None) -> bool:
        """Upload a single file to MyBookshelf2 using CLI"""
        # Check if already completed in this worker's progress file
        if original_file_hash in progress.get("completed_files", {}):
            logger.info(f"Skipping already uploaded file: {file_path.name}")
            return True
        
        # Pre-check: Check if file already exists in MyBookshelf2 database (from other workers or previous runs)
        # This prevents wasting time on duplicate upload attempts
        try:
            file_size = file_path.stat().st_size
            if (original_file_hash, file_size) in self.existing_hashes:
                logger.debug(f"File already exists in MyBookshelf2 database: {file_path.name}")
                sanitized_file_path = self.sanitize_filename(str(file_path))
                with self.progress_lock:
                    progress["completed_files"][original_file_hash] = {
                        "file": sanitized_file_path,
                        "status": "already_exists_in_db"
                    }
                self.save_progress(progress)
                return True
        except Exception as e:
            logger.debug(f"Error checking existing hashes: {e}")
            # Continue with upload attempt if check fails
        
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
        
        # Use provided container_path if available (from batch copy), otherwise determine it
        if container_path is None:
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
                        container_path = f"/tmp/{upload_path.name}"
                        try:
                            copy_cmd = [self.docker_cmd, 'cp', str(upload_path), f"{self.container}:{container_path}"]
                            subprocess.run(copy_cmd, check=True, timeout=60)
                        except Exception as e:
                            logger.error(f"Failed to copy file to container: {e}")
                            return False
                except subprocess.TimeoutExpired:
                    # Timeout checking file - assume it doesn't exist or container is slow, fallback to copy
                    logger.debug(f"Timeout checking Calibre file in container, will copy: {calibre_container_path}")
                    container_path = f"/tmp/{upload_path.name}"
                    try:
                        copy_cmd = [self.docker_cmd, 'cp', str(upload_path), f"{self.container}:{container_path}"]
                        subprocess.run(copy_cmd, check=True, timeout=60)
                    except Exception as e:
                        logger.error(f"Failed to copy file to container: {e}")
                        return False
                except Exception as e:
                    # Any other error, fallback to copy
                    logger.debug(f"Error checking Calibre file in container ({e}), will copy: {calibre_container_path}")
                    container_path = f"/tmp/{upload_path.name}"
                    try:
                        copy_cmd = [self.docker_cmd, 'cp', str(upload_path), f"{self.container}:{container_path}"]
                        subprocess.run(copy_cmd, check=True, timeout=60)
                    except Exception as e2:
                        logger.error(f"Failed to copy file to container: {e2}")
                        return False
            else:
                # Normal mode: container_path should have been set by batch copy, but fallback if needed
                container_path = f"/tmp/{upload_path.name}"
                # Only copy if not already copied (batch copy should have handled this)
                # For now, we'll assume batch copy worked, but this is a fallback
                if not self.use_symlinks:
                    try:
                        # Check if file exists in container
                        check_cmd = [self.docker_cmd, 'exec', self.container, 'test', '-f', container_path]
                        check_result = subprocess.run(check_cmd, capture_output=True, timeout=5)
                        if check_result.returncode != 0:
                            # File not in container, copy it
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
            '--api-url', self.api_url,
            'upload',
            '--file', container_path
        ]
        
        # Add metadata flags if available (sanitize to prevent NUL character errors)
        if metadata.get('title'):
            sanitized_title = self.sanitize_metadata_string(metadata['title'])
            upload_cmd.extend(['--title', sanitized_title])
        
        if metadata.get('authors'):
            for author in metadata['authors'][:20]:  # Limit to 20 authors
                sanitized_author = self.sanitize_metadata_string(author)
                upload_cmd.extend(['--author', sanitized_author])
        
        if metadata.get('language'):
            sanitized_language = self.sanitize_metadata_string(metadata['language'])
            upload_cmd.extend(['--language', sanitized_language])
        
        if metadata.get('series'):
            sanitized_series = self.sanitize_metadata_string(metadata['series'])
            upload_cmd.extend(['--series', sanitized_series])
            if metadata.get('series_index') is not None:
                upload_cmd.extend(['--series-index', str(metadata['series_index'])])
        
        # In symlink mode, pass the Calibre file path so API can create symlink instead of copying
        if self.use_symlinks and calibre_container_path and not is_temp_file:
            upload_cmd.extend(['--original-file-path', calibre_container_path])
        
        # Note: We don't pass genres to avoid validation errors
        # The CLI will set genres to empty array if not provided
        
        # Retry upload on connection errors with exponential backoff
        upload_start_time = time.time()
        last_error = None
        for attempt in range(self.max_retries):
            try:
                # Reduce logging - only log retries or every 20th file
                if attempt > 0 or len(self.upload_times) % 20 == 0:
                    logger.info(f"Uploading: {file_path.name}" + (f" (attempt {attempt + 1}/{self.max_retries})" if attempt > 0 else ""))
                else:
                    logger.debug(f"Uploading: {file_path.name}")
                result = subprocess.run(
                    upload_cmd,
                    capture_output=True,
                    text=True,
                    timeout=600  # 10 minutes for metadata processing
                )
                
                # Check for API 500 errors or other errors in output
                if result.returncode != 0:
                    error_output = result.stderr or result.stdout or ""
                    # Check for WebSocket connection errors (retryable)
                    if ("ConnectionRefusedError" in error_output or 
                        "Connect call failed" in error_output or
                        "Connection refused" in error_output or
                        "Errno 111" in error_output or
                        "WebSocket" in error_output and "error" in error_output.lower()):
                        if attempt < self.max_retries - 1:
                            delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                            logger.warning(f"WebSocket connection error for {file_path.name} (attempt {attempt + 1}/{self.max_retries}), retrying in {delay}s...")
                            time.sleep(delay)
                            continue  # Retry
                        else:
                            logger.error(f"WebSocket connection error for {file_path.name} after {self.max_retries} attempts: {error_output[:300]}")
                            return False
                    # Check for 500 errors (retryable)
                    elif "500 Server Error" in error_output or "INTERNAL SERVER ERROR" in error_output:
                        if attempt < self.max_retries - 1:
                            delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                            logger.warning(f"API 500 error for {file_path.name} (attempt {attempt + 1}/{self.max_retries}), retrying in {delay}s...")
                            time.sleep(delay)
                            continue  # Retry
                        else:
                            logger.error(f"API 500 error for {file_path.name} after {self.max_retries} attempts")
                            return False
                    elif "NUL" in error_output or "0x00" in error_output:
                        # NUL character error - this shouldn't happen if sanitization works, but log it
                        logger.error(f"NUL character error for {file_path.name} (sanitization may have failed): {error_output[:200]}")
                        return False
                    else:
                        # Other error, log full output for debugging (but don't retry)
                        logger.error(f"Upload failed for {file_path.name}: {error_output[:300]}")
                        return False
                
                break  # Success, exit retry loop
            except subprocess.TimeoutExpired as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                    logger.warning(f"Upload timeout for {file_path.name} (attempt {attempt + 1}/{self.max_retries}), retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"Upload timeout for {file_path.name} after {self.max_retries} attempts")
                    return False
            except Exception as e:
                error_str = str(e).lower()
                # Check if it's a connection error (retryable)
                if any(keyword in error_str for keyword in ['connection', 'refused', 'timeout', 'unreachable', 'network']):
                    last_error = e
                    if attempt < self.max_retries - 1:
                        delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                        logger.warning(f"Connection error for {file_path.name} (attempt {attempt + 1}/{self.max_retries}): {e}, retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"Connection error for {file_path.name} after {self.max_retries} attempts: {e}")
                        return False
                else:
                    # Non-retryable error, return immediately
                    logger.error(f"Non-retryable error for {file_path.name}: {e}")
                    return False
        else:
            # All retries exhausted
            logger.error(f"Upload failed for {file_path.name} after {self.max_retries} attempts: {last_error}")
            return False
        
        try:
            
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
                upload_time = time.time() - upload_start_time
                self.upload_times.append(upload_time)
                
                # Log slow uploads for investigation
                if upload_time > self.slow_upload_threshold:
                    logger.warning(f"Slow upload detected: {file_path.name} took {upload_time:.1f}s ({upload_time/60:.1f} min)")
                
                # Calculate and log average upload rate periodically
                if len(self.upload_times) % 100 == 0:
                    avg_time = sum(self.upload_times[-100:]) / min(100, len(self.upload_times))
                    rate_per_min = 60.0 / avg_time if avg_time > 0 else 0
                    logger.info(f"Upload performance: {rate_per_min:.2f} files/min (avg {avg_time:.1f}s per file over last 100 files)")
                
                # Reduce logging frequency - only log every 10th file or slow uploads
                if len(self.upload_times) % 10 == 0 or upload_time > self.slow_upload_threshold:
                    logger.info(f"Successfully uploaded: {file_path.name} (took {upload_time:.1f}s)")
                else:
                    logger.debug(f"Successfully uploaded: {file_path.name} (took {upload_time:.1f}s)")
                
                # Update existing_hashes cache with newly uploaded file
                # This prevents other workers (or this worker in next batch) from attempting duplicate uploads
                try:
                    file_size = file_path.stat().st_size
                    self.update_existing_hashes(original_file_hash, file_size)
                except Exception as e:
                    logger.debug(f"Error updating existing_hashes cache: {e}")
                
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
                
                # Sanitize file path before storing in progress (prevent NUL character issues)
                sanitized_file_path = self.sanitize_filename(str(file_path))
                with self.progress_lock:
                    progress["completed_files"][original_file_hash] = {
                        "file": sanitized_file_path,
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
                    # Update existing_hashes cache even for files that already exist
                    # This ensures our cache is up-to-date
                    try:
                        file_size = file_path.stat().st_size
                        self.update_existing_hashes(original_file_hash, file_size)
                    except Exception as e:
                        logger.debug(f"Error updating existing_hashes cache: {e}")
                    sanitized_file_path = self.sanitize_filename(str(file_path))
                    with self.progress_lock:
                        progress["completed_files"][original_file_hash] = {
                            "file": sanitized_file_path,
                            "status": "already_exists"
                        }
                    self.save_progress(progress)
                    return True
                
                if "insufficient metadata" in error_msg.lower() or "we need at least title and language" in error_msg.lower():
                    logger.warning(f"Insufficient metadata for {file_path.name}, skipping")
                    sanitized_file_path = self.sanitize_filename(str(file_path))
                    with self.progress_lock:
                        progress["completed_files"][original_file_hash] = {
                            "file": sanitized_file_path,
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
        
        Uses indexed WHERE b.id > last_id queries instead of OFFSET for O(log n) performance.
        
        Also checks existing_hashes (from database) to avoid finding files already uploaded by any worker.
        Uses file size for quick filtering, then hash for exact matching.
        
        Args:
            completed_hashes: Set of file hashes to exclude (already processed files from this worker's progress)
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
            
            # Load progress to get last processed book ID
            progress = self.load_progress()
            last_book_id = progress.get("last_processed_book_id", 0)
            
            # Handle initial offset for parallel workers on first run
            if last_book_id == 0 and self.db_offset and self.db_offset > 0:
                # First run: find book.id at db_offset
                logger.info(f"First run: finding starting book.id at offset {self.db_offset:,}")
                offset_query = """
                    SELECT b.id
                    FROM books b
                    JOIN data d ON b.id = d.book
                    WHERE d.format IN ('EPUB', 'PDF', 'FB2', 'MOBI', 'AZW3', 'TXT')
                    ORDER BY b.id
                    LIMIT 1 OFFSET ?
                """
                cursor.execute(offset_query, (self.db_offset,))
                result = cursor.fetchone()
                if result:
                    last_book_id = result[0]
                    logger.info(f"Starting from book.id: {last_book_id:,}")
                else:
                    logger.warning(f"Could not find book at offset {self.db_offset:,}, starting from beginning")
                    last_book_id = 0
            
            # Query for all book files with their paths
            # Calibre stores: books.path (relative path like "Author Name/Book Title (123)") 
            # and data.name (filename without extension) and data.format (uppercase extension)
            # We need to keep fetching until we have enough NEW files (not already completed)
            # Use WHERE b.id > last_id instead of OFFSET for O(log n) performance (uses index)
            
            base_query = """
                SELECT b.id, b.path, d.name, d.format
                FROM books b
                JOIN data d ON b.id = d.book
                WHERE d.format IN ('EPUB', 'PDF', 'FB2', 'MOBI', 'AZW3', 'TXT')
            """
            
            # Build file paths and verify they exist, filtering out completed files
            files = []
            missing_count = 0
            skipped_completed = 0
            max_fetched = 0
            max_book_id = last_book_id  # Track maximum book.id processed in this run
            
            # Process in batches for incremental progress
            # When limit is set (e.g., 10k), process that many NEW files, not all rows
            db_batch_size = 1000  # Fetch from DB in batches of 1000
            process_batch_size = 100  # Log progress every 100 files
            last_progress_log = 0
            
            # If we have a limit, we need to fetch batches until we have enough NEW files
            # This is because many files may already be completed
            while True:
                # Calculate how many more files we need
                # If limit is set, respect it. Otherwise, use batch_size for each batch
                if self.limit:
                    needed = self.limit - len(files)
                    if needed <= 0:
                        break
                else:
                    # Continuing mode: use batch_size to limit files per batch
                    needed = self.batch_size - len(files)
                    if needed <= 0:
                        break  # Got enough files for this batch
                
                # Use WHERE b.id > last_id instead of OFFSET (uses index, O(log n) instead of O(n))
                query = base_query + f" AND b.id > {last_book_id} ORDER BY b.id LIMIT {db_batch_size}"
                cursor.execute(query)
                rows = cursor.fetchall()
                
                if not rows:
                    # No more rows in database
                    break
                
                max_fetched += len(rows)
                
                # Process this batch with progress updates
                batch_new_files = 0
                for book_id, path, name, format_ext in rows:
                    file_path = self.calibre_dir / path / f"{name}.{format_ext.lower()}"
                    
                    # Skip file existence check during discovery for speed (10-50ms per check on network mounts)
                    # We'll verify file exists during upload phase - if it doesn't exist, upload will fail gracefully
                    # This makes discovery 10-20x faster by avoiding thousands of stat() calls
                    # Only do a quick check if we're logging missing files (first few)
                    if missing_count < 5:
                        if not file_path.exists() or not file_path.is_file():
                            missing_count += 1
                            logger.debug(f"File not found: {file_path}")
                            continue
                    # For most files, assume they exist (they're in the database, so likely exist)
                    # Upload phase will handle missing files gracefully
                    
                    # Check if file is already uploaded (in existing_hashes from database)
                    # This prevents discovery from finding files already uploaded by any worker
                    try:
                        file_size = file_path.stat().st_size
                        
                        # Quick check: if file size doesn't match any existing file, it's definitely new
                        # Create set of sizes for quick lookup (only once per batch)
                        if not hasattr(self, '_existing_sizes_cache'):
                            self._existing_sizes_cache = {size for (_, size) in self.existing_hashes}
                        
                        # If size doesn't match any existing file, definitely new - add it
                        if file_size not in self._existing_sizes_cache:
                            files.append(file_path)
                            batch_new_files += 1
                        else:
                            # Size matches - need to check hash to be sure
                            # Only hash files with matching sizes to avoid unnecessary work
                            file_hash = self.get_file_hash(file_path)
                            if (file_hash, file_size) not in self.existing_hashes:
                                # Not in database, add it
                                files.append(file_path)
                                batch_new_files += 1
                            else:
                                # Already in database, skip it
                                skipped_completed += 1
                                logger.debug(f"Skipping already uploaded file: {file_path.name}")
                    except (OSError, FileNotFoundError):
                        # File doesn't exist or can't be accessed, skip it
                        missing_count += 1
                        if missing_count <= 5:
                            logger.debug(f"File not accessible: {file_path}")
                        continue
                    # Track maximum book.id from this batch
                    max_book_id = max(max_book_id, book_id)
                    
                    # Log progress every process_batch_size files
                    if len(files) - last_progress_log >= process_batch_size:
                        logger.info(f"Found {len(files):,} new files so far (processed {max_fetched:,} rows, "
                                  f"skipped {skipped_completed:,} completed, {missing_count:,} missing)")
                        last_progress_log = len(files)
                    
                    # Stop if we have enough files (after filtering)
                    # If limit is set, use it. Otherwise, use batch_size
                    if self.limit is not None and self.limit > 0 and len(files) >= self.limit:
                        break
                    elif self.limit is None and len(files) >= self.batch_size:
                        break  # Got enough files for this batch
                
                # Update last_book_id for next iteration and save progress
                if rows and max_book_id > last_book_id:
                    last_book_id = max_book_id
                    progress["last_processed_book_id"] = max_book_id
                    self.save_progress(progress)
                
                # Log batch completion periodically
                if max_fetched % (db_batch_size * 10) == 0 or batch_new_files > 0:
                    logger.info(f"Processed batch: book.id > {last_book_id - db_batch_size if last_book_id >= db_batch_size else 0}, rows={len(rows)}, "
                              f"new_files={batch_new_files}, total_new={len(files):,}, "
                              f"total_processed={max_fetched:,}, skipped={skipped_completed:,}")
                
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
            elif self.limit is None and len(files) > self.batch_size:
                # Limit to batch_size when continuing (limit is None)
                files = files[:self.batch_size]
                logger.info(f"Ready to process {len(files):,} new files (batch size: {self.batch_size})")
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
        
        # Check API connectivity (optional, logs warning if check fails but continues)
        if not self.check_api_connectivity():
            logger.warning("API connectivity check failed, but continuing with migration. Uploads may fail if API is unreachable.")
        else:
            logger.info("API connectivity check passed")
        
        # Refresh existing_hashes at start to ensure we have latest data
        # This is important for duplicate detection during discovery
        logger.info("Refreshing existing file hashes from database for accurate duplicate detection...")
        self.refresh_existing_hashes()
        
        # Clear size cache (will be rebuilt during discovery)
        if hasattr(self, '_existing_sizes_cache'):
            delattr(self, '_existing_sizes_cache')
        
        # MyBookshelf2 has built-in deduplication based on file hash + size
        # Duplicates are automatically detected and skipped during upload
        # No need to delete existing books - the migration can be safely resumed
        
        # Load progress
        progress = self.load_progress()
        completed_hashes = set(progress.get("completed_files", {}).keys())
        completed_count = len(completed_hashes)
        
        # Check if continuing from previous run
        is_continuing = progress.get("last_processed_book_id", 0) > 0
        
        # Process in batches
        # If continuing, use batch_size and process until database is exhausted
        # If starting fresh, use limit if provided, otherwise use batch_size
        if is_continuing:
            logger.info(f"Continuing from previous run - processing in batches of {self.batch_size:,} until database is exhausted")
            batch_size = self.batch_size  # Use configurable batch size
            use_limit = False  # Don't stop after one batch
        else:
            # Starting fresh: use limit if provided, otherwise use batch_size
            batch_size = self.limit if self.limit else self.batch_size
            use_limit = self.limit is not None  # Only respect limit if explicitly set
            logger.info(f"Starting fresh - processing in batches of {batch_size:,}")
        
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
            
            # Process files in this batch with parallel uploads
            success_count = 0
            error_count = 0
            processed_count = 0
            
            # Collect files that need copying (for batch operation)
            # Note: Only batch copy files that don't need conversion (EPUB files)
            # Files that need conversion will be copied individually after conversion
            files_to_copy = []  # List of (file_path, container_path) tuples for batch copy
            files_ready = []  # Files that don't need copying (symlink mode or already in container)
            files_need_conversion = []  # Files that need conversion (will be handled individually)
            
            # Pre-process files to determine which need copying
            # Add progress logging for large batches
            total_files = len(files)
            logger.info(f"Pre-processing {total_files:,} files to determine copy requirements...")
            
            for idx, file_path in enumerate(files):
                # Log progress every 1000 files
                if (idx + 1) % 1000 == 0:
                    logger.info(f"Pre-processing progress: {idx + 1:,}/{total_files:,} files ({100*(idx+1)//total_files}%)")
                
                file_ext = file_path.suffix.lower()
                needs_conversion = file_ext not in ['.epub'] and not self.use_symlinks
                
                if needs_conversion:
                    # Files that need conversion will be copied individually after conversion
                    files_need_conversion.append(file_path)
                    continue
                
                # For files that don't need conversion, check if they need copying
                needs_copy = True
                container_path = f"/tmp/{file_path.name}"
                
                if self.use_symlinks:
                    # In symlink mode, assume files are accessible (skip slow docker check)
                    # We'll verify during upload if needed
                    try:
                        calibre_rel_path = str(file_path.relative_to(self.calibre_dir))
                        calibre_container_path = f"/calibre_library/{calibre_rel_path}"
                        # Skip the slow docker exec check - assume file exists if in symlink mode
                        # This speeds up pre-processing significantly
                        needs_copy = False
                        container_path = calibre_container_path
                    except:
                        # If we can't determine path, fall back to copy
                        needs_copy = True
                
                if needs_copy:
                    files_to_copy.append((file_path, container_path))
                else:
                    files_ready.append((file_path, container_path))
            
            logger.info(f"Pre-processing complete: {len(files_ready):,} ready, {len(files_to_copy):,} need copying, {len(files_need_conversion):,} need conversion")
            
            # Batch copy files that need copying (only EPUB files that don't need conversion)
            if files_to_copy:
                logger.info(f"Batch copying {len(files_to_copy)} files to container...")
                # Copy in batches
                for i in range(0, len(files_to_copy), self.batch_copy_size):
                    batch = files_to_copy[i:i + self.batch_copy_size]
                    copy_results = self.batch_copy_files_to_container(batch)
                    # Add successfully copied files to ready list
                    for file_path, container_path in batch:
                        if copy_results.get(file_path, False):
                            files_ready.append((file_path, container_path))
                        else:
                            logger.warning(f"Skipping {file_path.name} due to copy failure")
            
            # Add files that need conversion to ready list (they'll be copied individually in upload_file)
            for file_path in files_need_conversion:
                files_ready.append((file_path, None))  # container_path will be determined in upload_file
            
            # Use ThreadPoolExecutor for parallel uploads within this worker
            with ThreadPoolExecutor(max_workers=self.parallel_uploads) as executor:
                # Submit all upload tasks
                futures = {}
                for file_path, container_path in files_ready:
                    # Periodically refresh existing_hashes to pick up files uploaded by other workers
                    # Refresh every 1000 files or every 10 minutes (600 seconds)
                    if (self.files_processed_since_refresh >= 1000 or 
                        (time.time() - self.last_hash_refresh) > 600):
                        self.refresh_existing_hashes()
                    
                    # Check existing_hashes cache first (fastest)
                    file_size = file_path.stat().st_size
                    file_hash = None
                    
                    # Calculate hash for deduplication (before submitting to thread pool)
                    file_hash = self.get_file_hash(file_path)
                    
                    # Check if file exists in cache
                    if (file_hash, file_size) in self.existing_hashes:
                        # Already exists, skip
                        logger.debug(f"File already in cache: {file_path.name}")
                        continue
                    
                    # Check API before upload (for files not in cache)
                    api_check_result = self.check_file_exists_via_api(file_path, file_hash, file_size)
                    if api_check_result is True:
                        # File exists according to API, update cache and skip
                        logger.debug(f"File exists via API check: {file_path.name}")
                        self.update_existing_hashes(file_hash, file_size)
                        continue
                    elif api_check_result is False:
                        # File doesn't exist, proceed with upload
                        pass
                    # If api_check_result is None, API check failed, proceed with normal flow
                    
                    # Submit upload task (store hash with future for later use)
                    future = executor.submit(self.upload_file, file_path, file_hash, progress, container_path)
                    futures[future] = (file_path, file_hash)
                
                # Process completed uploads as they finish
                for future in as_completed(futures):
                    file_path, file_hash = futures[future]
                    processed_count += 1
                    
                    # Log progress every 50 files (reduced from 100 for parallel operations)
                    if processed_count % 50 == 0:
                        logger.info(f"Batch {batch_num} progress: {processed_count}/{total_new} files processed")
                    
                    try:
                        result = future.result()
                        if result:
                            success_count += 1
                            # Update completed_hashes (thread-safe) - hash already calculated before submit
                            with self.progress_lock:
                                completed_hashes.add(file_hash)
                        else:
                            error_count += 1
                    except Exception as e:
                        logger.error(f"Error uploading {file_path.name}: {e}")
                        error_count += 1
            
            total_success += success_count
            total_errors += error_count
            completed_count += success_count
            
            logger.info(f"Batch {batch_num} complete. Success: {success_count}, Errors: {error_count}")
            logger.info(f"Total progress: {total_success:,} successful, {total_errors:,} errors")
            
            # Save progress after each batch (checkpoint per plan)
            self.save_progress(progress)
            
            # Only check batch size limit if starting fresh with explicit limit
            if not is_continuing and use_limit and len(files) < batch_size:
                logger.info(f"Got fewer files than batch size ({len(files)} < {batch_size}), migration complete.")
                break
            # When continuing, the loop will continue until find_ebook_files() returns no files
        
        # Cleanup temp directory
        try:
            shutil.rmtree(self.temp_dir)
        except:
            pass
        
        logger.info(f"Migration complete. Total: {total_success:,} successful, {total_errors:,} errors")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 bulk_migrate_calibre.py <calibre_directory> [container_name] [username] [password] [--limit N] [--use-symlinks] [--worker-id N] [--offset N] [--parallel-uploads N]")
        print("Example: python3 bulk_migrate_calibre.py /path/to/calibre/library")
        print("         python3 bulk_migrate_calibre.py /path/to/calibre/library mybookshelf2_app admin mypassword123")
        print("         python3 bulk_migrate_calibre.py /path/to/calibre/library --limit 100")
        print("         python3 bulk_migrate_calibre.py /path/to/calibre/library --use-symlinks --limit 100")
        print("         python3 bulk_migrate_calibre.py /path/to/calibre/library --worker-id 1 --offset 0 --limit 10000")
        print("         python3 bulk_migrate_calibre.py /path/to/calibre/library --parallel-uploads 3")
        print("")
        print("Note: MyBookshelf2 has built-in deduplication. Duplicate files are automatically skipped.")
        print("      --parallel-uploads: Number of concurrent uploads per worker (default: 3)")
        sys.exit(1)
    
    calibre_dir = sys.argv[1]
    container = "mybookshelf2_app"
    username = "admin"
    password = "mypassword123"
    limit = None
    use_symlinks = False
    worker_id = None
    db_offset = None
    parallel_uploads = 3  # Default: 3 concurrent uploads per worker
    batch_size = 1000  # Default batch size
    
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
        elif arg == '--parallel-uploads':
            if i + 1 < len(sys.argv):
                try:
                    parallel_uploads = int(sys.argv[i + 1])
                    if parallel_uploads < 1 or parallel_uploads > 10:
                        print("Error: --parallel-uploads must be between 1 and 10")
                        sys.exit(1)
                    i += 1
                except ValueError:
                    print(f"Error: --parallel-uploads requires a number, got '{sys.argv[i + 1]}'")
                    sys.exit(1)
            else:
                print("Error: --parallel-uploads requires a number")
                sys.exit(1)
        elif arg == '--batch-size':
            if i + 1 < len(sys.argv):
                try:
                    batch_size = int(sys.argv[i + 1])
                    if batch_size < 1:
                        print("Error: --batch-size must be greater than 0")
                        sys.exit(1)
                    i += 1
                except ValueError:
                    print(f"Error: --batch-size requires a number, got '{sys.argv[i + 1]}'")
                    sys.exit(1)
            else:
                print("Error: --batch-size requires a number")
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
    
    migrator = MyBookshelf2Migrator(calibre_dir, container, username, password, False, limit, use_symlinks, worker_id, db_offset, parallel_uploads, batch_size)
    migrator.migrate()


if __name__ == "__main__":
    main()
