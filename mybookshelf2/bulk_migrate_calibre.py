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

# Try to import psutil for memory monitoring (optional)
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

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
        
        # Detect if running inside Docker container
        self.running_in_container = os.path.exists('/.dockerenv') or os.environ.get('container') == 'docker'
        
        # Determine docker command (only needed if running outside container)
        if self.running_in_container:
            self.docker_cmd = None  # Not needed when running inside container
            logger.info("Running inside Docker container - will use direct CLI calls")
        else:
            try:
                result = subprocess.run(['docker', 'ps'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
                self.docker_cmd = "docker" if result.returncode == 0 else "sudo docker"
            except:
                self.docker_cmd = "sudo docker"
            logger.info(f"Running on host - using docker command: {self.docker_cmd}")
        
        # Ensure temp directory exists
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Load existing file hashes from MyBookshelf2 database to avoid duplicate upload attempts
        # This prevents wasting time on files already uploaded by other workers or previous runs
        # OPTIMIZATION: Use lazy loading - only load hashes when needed to reduce memory usage
        # During discovery, we'll use API checks instead of loading all hashes upfront
        self.existing_hashes = set()  # Start empty, load on-demand
        self._hashes_loaded = False  # Track if hashes have been loaded
        self._use_lazy_hash_loading = True  # Enable lazy loading to reduce memory
        
        # Track last refresh time for periodic refresh of existing_hashes
        # This prevents stale cache when other workers upload files
        self.last_hash_refresh = time.time()
        self.last_hash_refresh_timestamp = None  # Database timestamp of last refresh (for incremental refresh)
        self.files_processed_since_refresh = 0
        self.database_hash_count = 0  # Track database size for dynamic refresh frequency
        self.refresh_thread = None  # Background thread for non-blocking refresh
        self.refresh_lock = threading.Lock()  # Lock for thread-safe hash updates
        
        # Only load hashes if lazy loading is disabled (for backward compatibility)
        if not self._use_lazy_hash_loading:
            logger.info("Loading existing file hashes from MyBookshelf2 database...")
            self.existing_hashes, latest_timestamp = self.load_existing_hashes_from_database()
            self.database_hash_count = len(self.existing_hashes)
            if latest_timestamp:
                self.last_hash_refresh_timestamp = latest_timestamp
            logger.info(f"Loaded {len(self.existing_hashes)} existing file hashes from MyBookshelf2 database")
        else:
            logger.info("Using lazy hash loading - hashes will be loaded on-demand to reduce memory usage")
        
        # Performance monitoring
        self.upload_times = []  # Track upload times for performance analysis (limited to last 1000)
        self.max_upload_times = 1000  # Limit upload_times list to prevent memory growth
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
        if self.running_in_container:
            # If running inside container, assume it's running
            return True
        try:
            result = subprocess.run(
                [self.docker_cmd, 'ps', '--filter', f'name={self.container}', '--format', '{{.Names}}'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5
            )
            return self.container in result.stdout.decode("utf-8", errors="ignore") if isinstance(result.stdout, bytes) else result.stdout.decode('utf-8', errors='ignore')
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
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5
            )
            return result.returncode == 0 and "OK" in result.stdout.decode("utf-8", errors="ignore") if isinstance(result.stdout, bytes) else result.stdout
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
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                logger.info(result.stdout.decode("utf-8", errors="ignore") if isinstance(result.stdout, bytes) else result.stdout.strip())
            else:
                stderr_text = result.stderr.decode("utf-8", errors="ignore") if isinstance(result.stderr, bytes) else result.stderr
                logger.error(f"Error deleting books: {stderr_text}")
        except Exception as e:
            logger.error(f"Error deleting books: {e}")
    
    def load_existing_hashes_from_database(self, since_timestamp: Optional[str] = None) -> Tuple[set, Optional[str]]:
        """Query MyBookshelf2 database for existing file hashes to avoid duplicate upload attempts.
        
        Args:
            since_timestamp: If provided, only query sources created after this timestamp (ISO format)
                           for incremental refresh. If None, query all sources.
        
        Returns:
            Tuple of (set of (hash, size) tuples, latest_timestamp string or None)
        """
        if since_timestamp:
            # Incremental refresh: only get new sources since last refresh
            script = f"""
import sys
import os
from datetime import datetime
sys.path.insert(0, '/code')
os.chdir('/code')
from app import app, db
from app import model

try:
    with app.app_context():
        # Query only sources created after the timestamp
        from sqlalchemy import func
        cutoff = datetime.fromisoformat('{since_timestamp}')
        sources = db.session.query(model.Source.hash, model.Source.size, model.Source.created).filter(
            model.Source.created > cutoff
        ).order_by(model.Source.created).all()
        
        # Return as set of tuples (hash, size) for fast lookup
        result = []
        latest_timestamp = None
        for hash_val, size, created in sources:
            result.append(f"{{hash_val}}|{{size}}")
            if latest_timestamp is None or created > latest_timestamp:
                latest_timestamp = created
        
        print('|'.join(result))
        if latest_timestamp:
            print(f"\\nLATEST_TIMESTAMP:{{latest_timestamp.isoformat()}}", file=sys.stderr)
except Exception as e:
    import traceback
    print(f"ERROR: {{e}}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
"""
        else:
            # Full refresh: get all sources
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
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True,
                timeout=120  # Allow up to 2 minutes for large databases
            )
            if result.returncode == 0:
                output = result.stdout.decode("utf-8", errors="ignore") if isinstance(result.stdout, bytes) else result.stdout.strip()
                stderr_text = result.stderr.decode("utf-8", errors="ignore") if isinstance(result.stderr, bytes) else result.stderr
                
                latest_timestamp = None
                if stderr_text and "LATEST_TIMESTAMP:" in stderr_text:
                    # Extract latest timestamp from stderr
                    for line in stderr_text.split('\n'):
                        if "LATEST_TIMESTAMP:" in line:
                            latest_timestamp = line.split("LATEST_TIMESTAMP:")[1].strip()
                            break
                
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
                    
                    if since_timestamp:
                        logger.info(f"Loaded {len(existing)} new file hashes since last refresh (incremental)")
                    else:
                        logger.info(f"Successfully loaded {len(existing)} existing file hashes from MyBookshelf2 database")
                    return existing, latest_timestamp
                else:
                    logger.info("No existing files found in MyBookshelf2 database (this is normal for first migration)")
                    return set(), latest_timestamp
            else:
                stderr_text = result.stderr.decode("utf-8", errors="ignore") if isinstance(result.stderr, bytes) else result.stderr
                error_msg = stderr_text.strip() if stderr_text else "Unknown error"
                logger.warning(f"Could not load existing hashes from database (returncode {result.returncode}): {error_msg}")
                stdout_text = result.stdout.decode("utf-8", errors="ignore") if isinstance(result.stdout, bytes) else result.stdout
                if stdout_text:
                    logger.debug(f"stdout: {stdout_text[:200]}")
        except subprocess.TimeoutExpired:
            logger.warning("Timeout loading existing hashes from database (database may be large)")
        except Exception as e:
            logger.warning(f"Could not load existing hashes from database: {e}")
        return set(), None
    
    def ensure_hashes_loaded(self):
        """Lazy load hashes if not already loaded. This reduces memory usage at startup."""
        if not self._hashes_loaded and self._use_lazy_hash_loading:
            logger.info("Loading existing file hashes from MyBookshelf2 database (lazy load)...")
            self.existing_hashes, latest_timestamp = self.load_existing_hashes_from_database()
            self.database_hash_count = len(self.existing_hashes)
            if latest_timestamp:
                self.last_hash_refresh_timestamp = latest_timestamp
            self._hashes_loaded = True
            logger.info(f"Loaded {len(self.existing_hashes)} existing file hashes from MyBookshelf2 database")
    
    def _calculate_refresh_frequency(self) -> Tuple[int, int]:
        """Calculate optimal refresh frequency based on database size.
        
        Returns:
            Tuple of (files_threshold, seconds_threshold)
        """
        # Base frequency: 5000 files or 30 minutes
        base_files = 5000
        base_seconds = 1800  # 30 minutes
        
        # Adjust based on database size
        if self.database_hash_count == 0:
            return base_files, base_seconds
        
        # If database is large (>100k files), increase interval to reduce query frequency
        if self.database_hash_count > 100000:
            # Scale up: 10000 files or 60 minutes
            return base_files * 2, base_seconds * 2
        elif self.database_hash_count > 50000:
            # Medium scale: 7500 files or 45 minutes
            return int(base_files * 1.5), int(base_seconds * 1.5)
        else:
            # Small database: use base frequency
            return base_files, base_seconds
    
    def refresh_existing_hashes(self, use_incremental: bool = True, background: bool = False):
        """Refresh existing_hashes from database to pick up files uploaded by other workers.
        This prevents stale cache issues when multiple workers are running in parallel.
        
        Args:
            use_incremental: If True and last_hash_refresh_timestamp is set, only query new hashes
            background: If True, run refresh in background thread (non-blocking)
        """
        if background:
            # Run refresh in background thread to avoid blocking uploads
            if self.refresh_thread and self.refresh_thread.is_alive():
                logger.debug("Hash refresh already running in background, skipping")
                return
            
            def background_refresh():
                try:
                    self._refresh_existing_hashes_sync(use_incremental)
                except Exception as e:
                    logger.error(f"Error in background hash refresh: {e}")
            
            self.refresh_thread = threading.Thread(target=background_refresh, daemon=True)
            self.refresh_thread.start()
            logger.info("Started background hash refresh (non-blocking)")
        else:
            self._refresh_existing_hashes_sync(use_incremental)
    
    def _refresh_existing_hashes_sync(self, use_incremental: bool = True):
        """Synchronous hash refresh implementation."""
        # Ensure hashes are loaded before refreshing
        self.ensure_hashes_loaded()
        
        refresh_start = time.time()
        
        # Use incremental refresh if possible (much faster for large databases)
        if use_incremental and self.last_hash_refresh_timestamp:
            logger.info(f"Refreshing existing file hashes (incremental, since {self.last_hash_refresh_timestamp})...")
            new_hashes, latest_timestamp = self.load_existing_hashes_from_database(self.last_hash_refresh_timestamp)
            
            # Merge new hashes into existing set
            with self.refresh_lock:
                old_count = len(self.existing_hashes)
                self.existing_hashes.update(new_hashes)
                new_count = len(self.existing_hashes)
                if latest_timestamp:
                    self.last_hash_refresh_timestamp = latest_timestamp
        else:
            # Full refresh (slower, but needed for first load or if incremental fails)
            logger.info("Refreshing existing file hashes (full refresh)...")
            new_hashes, latest_timestamp = self.load_existing_hashes_from_database()
            
            with self.refresh_lock:
                old_count = len(self.existing_hashes)
                self.existing_hashes = new_hashes
                new_count = len(self.existing_hashes)
                self.database_hash_count = new_count
                if latest_timestamp:
                    self.last_hash_refresh_timestamp = latest_timestamp
        
        refresh_time = time.time() - refresh_start
        self.last_hash_refresh = time.time()
        self.files_processed_since_refresh = 0
        
        logger.info(f"Refreshed existing hashes: {old_count:,} -> {new_count:,} (added {new_count - old_count:,} new hashes) in {refresh_time:.1f}s")
        
        # Warn if refresh is taking too long (indicates database growth issue)
        if refresh_time > 30:
            logger.warning(f"Hash refresh took {refresh_time:.1f}s - database query is getting slow. Consider optimizing refresh frequency.")
    
    def update_existing_hashes(self, file_hash: str, file_size: int):
        """Add a newly uploaded file's hash+size to existing_hashes set.
        This keeps the cache up-to-date without needing a full database refresh.
        Thread-safe version.
        """
        with self.refresh_lock:
            if (file_hash, file_size) not in self.existing_hashes:
                self.existing_hashes.add((file_hash, file_size))
                self.database_hash_count += 1
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
        Also removes other problematic characters that might cause issues.
        """
        if not value:
            return value
        if not isinstance(value, str):
            value = str(value)
        # Remove NUL characters (0x00) - PostgreSQL cannot handle these
        # Also remove other control characters that might cause issues
        sanitized = value.replace('\x00', '').replace('\r', '')
        # Remove any remaining control characters except newline and tab
        sanitized = ''.join(char for char in sanitized if ord(char) >= 32 or char in '\n\t')
        return sanitized
    
    def check_file_exists_via_api(self, file_path: Path, file_hash: Optional[str], file_size: int) -> Optional[bool]:
        """Check if file exists via API /api/upload/check.
        Returns True if file exists, False if not, None on error.
        This allows us to skip upload attempts for duplicates.
        
        Args:
            file_path: Path to the file
            file_hash: Optional file hash (can be None for size-only check)
            file_size: File size in bytes
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
            # If hash is None, API can still check by size (less accurate but faster)
            file_info = {
                'size': file_size,
                'mime_type': mime_type,
                'extension': extension
            }
            if file_hash:
                file_info['hash'] = file_hash
            
            # Call API check endpoint
            check_url = f"{self.api_url}/api/upload/check"
            response = session.post(check_url, json=file_info, timeout=5)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('error') == 'file already exists':
                    return True  # File exists
                return False  # File doesn't exist
            else:
                # Enhanced error logging for API check failures
                logger.debug(f"API check returned status {response.status_code} for {file_path.name}. "
                           f"Response: {response.text[:200] if response.text else 'No response body'}")
                return None
        except requests.exceptions.Timeout as e:
            # Enhanced error logging with timeout context
            logger.warning(f"API check timeout for {file_path.name} (timeout=5s): {e}")
            return None
        except requests.exceptions.ConnectionError as e:
            # Enhanced error logging with connection context
            logger.warning(f"API check connection error for {file_path.name}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            # Enhanced error logging for other request exceptions
            logger.warning(f"API check failed for {file_path.name}: {type(e).__name__}: {e}")
            return None
        except Exception as e:
            # Enhanced error logging for unexpected exceptions
            logger.warning(f"Unexpected error checking file via API for {file_path.name}: {type(e).__name__}: {e}")
            return None
    
    def check_files_exists_via_api_batch(self, file_infos: List[Dict[str, Any]]) -> List[Optional[bool]]:
        """
        Batch check multiple files for existence via API /api/upload/check-batch.
        Much faster than individual checks.
        
        Args:
            file_infos: List of dicts with keys: file_path, file_hash (optional), file_size
            
        Returns:
            List of Optional[bool]: True if exists, False if not, None on error (same order as input)
        """
        session = self._get_api_session()
        if not session:
            return [None] * len(file_infos)  # Can't check, return None for all
        
        try:
            # Prepare batch request
            batch_request = []
            for info in file_infos:
                file_path = info['file_path']
                file_size = info['file_size']
                file_hash = info.get('file_hash')
                
                extension = file_path.suffix.lower().lstrip('.')
                mime_type, _ = mimetypes.guess_type(str(file_path))
                if not mime_type:
                    mime_type = ''
                
                file_info = {
                    'size': file_size,
                    'mime_type': mime_type,
                    'extension': extension
                }
                if file_hash:
                    file_info['hash'] = file_hash
                
                batch_request.append(file_info)
            
            # Call batch API endpoint
            check_url = f"{self.api_url}/api/upload/check-batch"
            response = session.post(check_url, json=batch_request, timeout=30)  # Longer timeout for batch
            
            if response.status_code == 200:
                results = response.json().get('results', [])
                # Convert to boolean format matching single check
                return [r.get('exists', False) if r.get('error') is None else None for r in results]
            else:
                logger.warning(f"Batch API check returned status {response.status_code}")
                return [None] * len(file_infos)
        except requests.exceptions.Timeout as e:
            logger.warning(f"Batch API check timeout (timeout=30s): {e}")
            return [None] * len(file_infos)
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Batch API check connection error: {e}")
            return [None] * len(file_infos)
        except Exception as e:
            logger.warning(f"Batch API check failed: {type(e).__name__}: {e}")
            return [None] * len(file_infos)
    
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
                        subprocess.run(copy_cmd, check=True, timeout=60, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
                    subprocess.run(copy_cmd, check=True, timeout=60, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
                
                # Check for empty or whitespace-only files
                if not content or not content.strip():
                    logger.warning(f"Progress file {self.progress_file} is empty or contains only whitespace. Using default progress.")
                    return default_progress
                
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
                
                # Validate JSON before parsing
                if not content.strip():
                    logger.warning(f"Progress file {self.progress_file} has no valid JSON content after parsing. Using default progress.")
                    return default_progress
                
                progress = json.loads(content)
                
                # Validate parsed progress structure
                if not isinstance(progress, dict):
                    logger.warning(f"Progress file {self.progress_file} does not contain a valid dictionary. Using default progress.")
                    return default_progress
                
                # Ensure last_processed_book_id exists in loaded progress
                if "last_processed_book_id" not in progress:
                    progress["last_processed_book_id"] = self.db_offset if self.db_offset else 0
                return progress
        except json.JSONDecodeError as e:
            logger.warning(f"Progress file {self.progress_file} contains invalid JSON: {e}. Starting fresh.")
            return default_progress
        except Exception as e:
            logger.warning(f"Error loading progress file {self.progress_file}: {e}. Starting fresh.")
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
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                output = result.stdout.decode("utf-8", errors="ignore") if isinstance(result.stdout, bytes) else result.stdout
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
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True,
                timeout=300
            )
            
            if result.returncode != 0:
                stderr_text = result.stderr.decode("utf-8", errors="ignore") if isinstance(result.stderr, bytes) else result.stderr
                logger.error(f"Conversion failed: {stderr_text}")
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
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
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
    
    def _run_upload_with_progress_monitoring(self, upload_cmd: List[str], file_name: str, 
                                             max_timeout: int = 600, progress_check_interval: int = 60,
                                             stuck_threshold: int = 240) -> subprocess.CompletedProcess:
        """
        Run upload command with progress monitoring to detect stuck processes.
        
        Args:
            upload_cmd: Command to run
            file_name: Name of file being uploaded (for logging)
            max_timeout: Maximum total timeout in seconds (default: 600 = 10 minutes)
            progress_check_interval: How often to check for progress in seconds (default: 60)
            stuck_threshold: Consider stuck if no progress for this many seconds (default: 240 = 4 minutes)
        
        Returns:
            subprocess.CompletedProcess with stdout, stderr, returncode
        """
        # Start the process
        process = subprocess.Popen(
            upload_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1  # Line buffered
        )
        
        stdout_lines = []
        stderr_lines = []
        start_time = time.time()
        last_progress_time = start_time
        last_output_size = 0
        
        # Track process activity
        if PSUTIL_AVAILABLE:
            try:
                proc = psutil.Process(process.pid)
                last_cpu_time = proc.cpu_times().user + proc.cpu_times().system
                last_io_read = proc.io_counters().read_bytes if hasattr(proc, 'io_counters') else 0
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                proc = None
                last_cpu_time = 0
                last_io_read = 0
        else:
            proc = None
            last_cpu_time = 0
            last_io_read = 0
        
        # Monitor process with progress checks
        while process.poll() is None:
            elapsed = time.time() - start_time
            
            # Check if exceeded maximum timeout
            if elapsed >= max_timeout:
                logger.warning(f"Upload timeout for {file_name} after {elapsed:.0f}s (max: {max_timeout}s), terminating...")
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                raise subprocess.TimeoutExpired(upload_cmd, max_timeout, output='', stderr='')
            
            # Check for progress periodically
            time_since_last_progress = time.time() - last_progress_time
            if time_since_last_progress >= progress_check_interval:
                progress_detected = False
                
                # Check 1: Process output (stdout/stderr)
                current_output_size = len(''.join(stdout_lines)) + len(''.join(stderr_lines))
                if current_output_size > last_output_size:
                    progress_detected = True
                    last_output_size = current_output_size
                    last_progress_time = time.time()
                    logger.debug(f"Progress detected for {file_name}: output size increased")
                
                # Check 2: Process CPU activity (if psutil available)
                if proc:
                    try:
                        current_cpu_time = proc.cpu_times().user + proc.cpu_times().system
                        if current_cpu_time > last_cpu_time + 0.1:  # At least 0.1s CPU time
                            progress_detected = True
                            last_cpu_time = current_cpu_time
                            last_progress_time = time.time()
                            logger.debug(f"Progress detected for {file_name}: CPU activity")
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                    
                    # Check 3: I/O activity
                    try:
                        if hasattr(proc, 'io_counters'):
                            current_io_read = proc.io_counters().read_bytes
                            if current_io_read > last_io_read:
                                progress_detected = True
                                last_io_read = current_io_read
                                last_progress_time = time.time()
                                logger.debug(f"Progress detected for {file_name}: I/O activity")
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                
                # Check 4: Process is still running (basic check)
                if process.poll() is None:
                    # Process is alive, but check if it's been stuck too long
                    if time_since_last_progress >= stuck_threshold:
                        logger.warning(f"Upload appears stuck for {file_name} (no progress for {time_since_last_progress:.0f}s, threshold: {stuck_threshold}s), terminating...")
                        process.terminate()
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                        raise subprocess.TimeoutExpired(
                            upload_cmd, 
                            elapsed, 
                            output=''.join(stdout_lines), 
                            stderr=''.join(stderr_lines)
                        )
                else:
                    # Process finished, break loop
                    break
                
                if not progress_detected:
                    logger.debug(f"No progress detected for {file_name} in last {time_since_last_progress:.0f}s (checking every {progress_check_interval}s)")
            
            # Read available output (non-blocking)
            try:
                # Use select for non-blocking read (Unix only)
                if sys.platform != 'win32':
                    import select
                    ready, _, _ = select.select([process.stdout, process.stderr], [], [], 0.1)
                    for stream in ready:
                        if stream == process.stdout:
                            line = stream.readline()
                            if line:
                                stdout_lines.append(line)
                                last_progress_time = time.time()  # Output is progress
                        elif stream == process.stderr:
                            line = stream.readline()
                            if line:
                                stderr_lines.append(line)
                                last_progress_time = time.time()  # Output is progress
                else:
                    # Windows: just sleep and check
                    time.sleep(0.1)
            except (OSError, ValueError, ImportError):
                # select failed or no data available, just sleep
                time.sleep(0.1)
        
        # Process finished, get remaining output
        remaining_stdout, remaining_stderr = process.communicate()
        if remaining_stdout:
            stdout_lines.append(remaining_stdout)
        if remaining_stderr:
            stderr_lines.append(remaining_stderr)
        
        # Create CompletedProcess-like result
        stdout_text = ''.join(stdout_lines)
        stderr_text = ''.join(stderr_lines)
        
        return subprocess.CompletedProcess(
            args=upload_cmd,
            returncode=process.returncode,
            stdout=stdout_text,
            stderr=stderr_text
        )
    
    def upload_file(self, file_path: Path, original_file_hash: str, progress: Dict[str, Any], container_path: Optional[str] = None):
        """Upload a single file to MyBookshelf2 using CLI
        Returns: (True, False) for actual new uploads, (True, True) for duplicates, or False for errors
        """
        # Check if already completed in this worker's progress file
        if original_file_hash in progress.get("completed_files", {}):
            logger.info(f"Skipping already uploaded file: {file_path.name}")
            return (True, True)  # Return (success, was_duplicate) tuple
        
        # Pre-check: Check if file already exists in MyBookshelf2 database (from other workers or previous runs)
        # This prevents wasting time on duplicate upload attempts
        try:
            file_size = file_path.stat().st_size
            # Thread-safe read (sets are generally safe for reads in CPython, but explicit is better)
            with self.refresh_lock:
                hash_exists = (original_file_hash, file_size) in self.existing_hashes
            if hash_exists:
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
        
        # CRITICAL: If metadata is empty, the backend will generate a path with spaces that's too long
        # Extract metadata from filename as fallback if extraction failed
        if not metadata.get('title') or not metadata.get('authors'):
            logger.warning(f"Metadata extraction failed or incomplete for {file_path.name}, attempting filename fallback")
            # Try to extract title from filename (remove extension, use as title)
            filename_without_ext = file_path.stem
            if filename_without_ext and not metadata.get('title'):
                metadata['title'] = filename_without_ext[:200]  # Limit title length
                logger.info(f"Using filename as title fallback: {metadata['title'][:50]}...")
            # If still no title, skip this file (backend cannot handle empty metadata)
            if not metadata.get('title'):
                logger.error(f"Cannot upload {file_path.name}: no title available (metadata extraction failed and filename unusable)")
                sanitized_file_path = self.sanitize_filename(str(file_path))
                with self.progress_lock:
                    progress["completed_files"][original_file_hash] = {
                        "file": sanitized_file_path,
                        "status": "metadata_extraction_failed"
                    }
                self.save_progress(progress)
                return False
            # Ensure we have at least one author (use "Unknown" as fallback)
            if not metadata.get('authors'):
                metadata['authors'] = ['Unknown']
                logger.info(f"Using 'Unknown' as author fallback for {file_path.name}")
            # Ensure we have a language
            if not metadata.get('language'):
                metadata['language'] = 'ru'  # Default to Russian
                logger.info(f"Using 'ru' as language fallback for {file_path.name}")
        
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
                if self.running_in_container:
                    # Running inside container - check file directly
                    if Path(calibre_container_path).exists():
                        container_path = calibre_container_path
                        logger.debug(f"Using Calibre library file directly: {container_path}")
                    else:
                        # File doesn't exist, fallback to copy
                        logger.debug(f"Calibre file not found, will copy: {calibre_container_path}")
                        container_path = f"/tmp/{upload_path.name}"
                        shutil.copy2(str(upload_path), container_path)
                else:
                    # Running on host - use docker exec to check
                    try:
                        check_cmd = [self.docker_cmd, 'exec', self.container, 'test', '-f', calibre_container_path]
                        check_result = subprocess.run(check_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
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
                    if self.running_in_container:
                        # Running inside container - check and copy directly
                        if not Path(container_path).exists():
                            shutil.copy2(str(upload_path), container_path)
                    else:
                        # Running on host - use docker exec to check
                        try:
                            # Check if file exists in container
                            check_cmd = [self.docker_cmd, 'exec', self.container, 'test', '-f', container_path]
                            check_result = subprocess.run(check_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
                            if check_result.returncode != 0:
                                # File not in container, copy it
                                copy_cmd = [self.docker_cmd, 'cp', str(upload_path), f"{self.container}:{container_path}"]
                                subprocess.run(copy_cmd, check=True, timeout=60)
                        except Exception as e:
                            logger.error(f"Failed to copy file to container: {e}")
                            return False
        
        # Build CLI command - use direct call if running inside container, otherwise use docker exec
        if self.running_in_container:
            # Running inside container - call CLI directly
            upload_cmd = [
                'python3', 'cli/mbs2.py',
                '-u', self.username,
                '-p', self.password,
                '--ws-url', 'ws://mybookshelf2_backend:8080/ws',
                '--api-url', 'http://localhost:6006',  # Use localhost when inside container
                'upload',
                '--file', container_path
            ]
        else:
            # Running on host - use docker exec
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
            # Sanitize the path to remove NUL characters and other problematic chars
            sanitized_path = self.sanitize_metadata_string(calibre_container_path)
            
            # Check if path contains NUL characters or if metadata would generate too long paths
            # The backend generates paths using schema: %(author)s/%(title)s(%(language)s)/%(author)s - %(title)s
            # This means author and title appear TWICE in the path (once in directory, once in filename)
            path_has_nul = '\x00' in calibre_container_path
            
            # Calculate estimated backend path length using actual schema format
            # Schema: %(author)s/%(title)s(%(language)s)/%(author)s - %(title)s
            # Path structure: /data/books/[author]/[title]([lang])/[author] - [title].[ext]
            estimated_length = 12  # "/data/books/"
            
            # Get authors string (backend uses ebook.authors_str which joins authors with ", ")
            if metadata.get('authors'):
                authors_str = ', '.join(metadata.get('authors', [])[:5])  # Limit to 5 authors to prevent very long strings
                estimated_length += len(authors_str)  # Author in directory
                estimated_length += 1  # "/" separator
                estimated_length += len(authors_str)  # Author in filename (duplicate)
                estimated_length += 3  # " - " separator
            else:
                # Empty author = spaces in path, add large buffer
                estimated_length += 50
            
            # Get title
            title = metadata.get('title', '')
            if title:
                estimated_length += len(title)  # Title in directory
                estimated_length += 5  # "(" + lang + ")" (e.g., "(ru)")
                estimated_length += 1  # "/" separator
                estimated_length += len(title)  # Title in filename (duplicate)
            else:
                # Empty title = spaces in path, add large buffer
                estimated_length += 50
            
            # Add language and extension
            if metadata.get('language'):
                estimated_length += 5  # Language code + parentheses (already counted above if title exists)
            else:
                estimated_length += 5  # Default language
            
            # Add file extension
            file_ext = file_path.suffix or '.fb2'
            estimated_length += len(file_ext)  # Extension (e.g., ".fb2")
            
            # Add buffer for duplicate numbering (1), (2), etc. when files already exist
            estimated_length += 50
            
            # Disable symlink if:
            # 1. Path has NUL characters
            # 2. Original path is too long (>100 chars - backend adds more)
            # 3. Estimated backend path would be too long (>180 chars - filesystem limit is 255, need buffer)
            # 4. Missing critical metadata (empty authors or title)
            # 5. Very long title (>100 chars)
            path_has_issues = (
                path_has_nul or
                len(sanitized_path) > 100 or  # Original path too long
                estimated_length > 180 or  # Backend-generated path would be too long (lowered from 200)
                not metadata.get('authors') or  # Empty authors
                not metadata.get('title') or   # Empty title
                len(metadata.get('title', '')) > 100  # Very long title
            )
            
            if path_has_issues:
                # Disable symlink for this problematic file - it will be copied instead
                logger.info(f"Disabling symlink for {file_path.name} due to path issues (NUL: {path_has_nul}, orig_len: {len(sanitized_path)}, est_backend_len: {estimated_length}, has_authors: {bool(metadata.get('authors'))}, has_title: {bool(metadata.get('title'))}, title_len: {len(metadata.get('title', ''))})")
                # Don't pass --original-file-path, so backend will copy the file instead
            else:
                # Path is safe, use it for symlink
                upload_cmd.extend(['--original-file-path', sanitized_path])
        
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
                
                # Use progress monitoring instead of simple timeout
                result = self._run_upload_with_progress_monitoring(
                    upload_cmd,
                    file_path.name,
                    max_timeout=600,  # Maximum 10 minutes total
                    progress_check_interval=60,  # Check every 60 seconds
                    stuck_threshold=240  # Consider stuck if no progress for 4 minutes
                )
                
                # Capture both stdout and stderr for error analysis (text=True means they're already strings)
                stdout_text = result.stdout or ""
                stderr_text = result.stderr or ""
                
                # Check for API 500 errors or other errors in output
                if result.returncode != 0:
                    error_output = stderr_text + stdout_text
                    if not error_output.strip():
                        error_output = f"Upload failed with return code {result.returncode} (no error message captured)"
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
                            # Enhanced error logging with more context for LLM analysis
                            logger.error(f"WebSocket connection error for {file_path.name} after {self.max_retries} attempts. "
                                       f"File: {file_path}, Size: {file_path.stat().st_size if file_path.exists() else 'N/A'}, "
                                       f"Error: {error_output[:500]}")
                            return False
                    # Check for 500 errors (retryable)
                    elif "500 Server Error" in error_output or "INTERNAL SERVER ERROR" in error_output:
                        if attempt < self.max_retries - 1:
                            delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                            logger.warning(f"API 500 error for {file_path.name} (attempt {attempt + 1}/{self.max_retries}), retrying in {delay}s...")
                            time.sleep(delay)
                            continue  # Retry
                        else:
                            # Enhanced error logging with more context for LLM analysis
                            logger.error(f"API 500 error for {file_path.name} after {self.max_retries} attempts. "
                                       f"File: {file_path}, Size: {file_path.stat().st_size if file_path.exists() else 'N/A'}, "
                                       f"Error: {error_output[:500]}")
                            return False
                    elif "NUL" in error_output or "0x00" in error_output:
                        # NUL character error - this shouldn't happen if sanitization works, but log it
                        logger.error(f"NUL character error for {file_path.name} (sanitization may have failed). "
                                   f"File: {file_path}, Error: {error_output[:500]}")
                        return False
                    elif result.returncode == 11:
                        # Return code 11 from mbs2.py means "Data error - no use in retrying" (SoftActionError)
                        # This typically means "file already exists" - treat as success
                        logger.info(f"File already exists in MyBookshelf2: {file_path.name} (return code: 11)")
                        # Update existing_hashes cache
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
                        return (True, True)  # Return (success, was_duplicate) tuple
                    else:
                        # Other error, log full output for debugging (but don't retry)
                        # Enhanced error logging with more context for LLM analysis (increased from 300 to 500 chars)
                        logger.error(f"Upload failed for {file_path.name}. "
                                   f"File: {file_path}, Size: {file_path.stat().st_size if file_path.exists() else 'N/A'}, "
                                   f"Return code: {result.returncode}, Error: {error_output[:500]}")
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
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
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
                
                # Limit upload_times list size to prevent memory growth over time
                if len(self.upload_times) > self.max_upload_times:
                    # Keep only the most recent entries
                    self.upload_times = self.upload_times[-self.max_upload_times:]
                
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
                return (True, False)  # Return (success, was_duplicate) tuple - False means actual new upload
            else:
                # This should not happen if retry logic works correctly, but handle it anyway
                error_msg = (result.stderr or "") + (result.stdout or "")
                if not error_msg.strip():
                    error_msg = f"Upload failed with return code {result.returncode} (no error message captured)"
                logger.error(f"Upload failed for {file_path.name}: {error_msg[:500]}")
                
                # Handle specific error cases
                # Return code 11 from mbs2.py means "Data error - no use in retrying" (SoftActionError)
                # This typically means "file already exists"
                if (result.returncode == 11 or
                    "already exists" in error_msg.lower() or 
                    "duplicate" in error_msg.lower() or 
                    "already in db" in error_msg.lower() or
                    "SoftActionError" in error_msg or
                    "Data error" in error_msg):
                    logger.info(f"File already exists in MyBookshelf2: {file_path.name} (return code: {result.returncode})")
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
                    return (True, True)  # Return (success, was_duplicate) tuple - duplicate
                
                if "insufficient metadata" in error_msg.lower() or "we need at least title and language" in error_msg.lower():
                    logger.warning(f"Insufficient metadata for {file_path.name}, skipping")
                    sanitized_file_path = self.sanitize_filename(str(file_path))
                    with self.progress_lock:
                        progress["completed_files"][original_file_hash] = {
                            "file": sanitized_file_path,
                            "status": "insufficient_metadata"
                        }
                    self.save_progress(progress)
                    return (True, True)  # Return (success, was_duplicate) tuple - skipped, treat as duplicate
                
                # Log error
                with open(self.error_file, 'a') as f:
                    f.write(f"{file_path}: {error_msg}\n")
                
                return False  # Return False for errors (not a tuple, will be handled as error)
                
        except subprocess.TimeoutExpired:
            logger.error(f"Upload timeout for {file_path.name}")
            return False  # Return False for errors (not a tuple, will be handled as error)
        except Exception as e:
            logger.error(f"Error uploading {file_path.name}: {e}")
            return False  # Return False for errors (not a tuple, will be handled as error)
    
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
        
        # Enhanced logging: Log phase transition and book ID range
        progress = self.load_progress()
        last_book_id = progress.get("last_processed_book_id", 0)
        logger.info(f"[DISCOVERY PHASE] Querying Calibre database for book files (fast method). "
                   f"Starting from book.id > {last_book_id:,}")
        
        try:
            # Use read-only mode and timeout to prevent database locking conflicts between workers
            # timeout=30 allows other workers to wait up to 30 seconds if database is locked
            # uri=True enables additional connection options
            # Add retry logic for database locked errors
            max_db_retries = 3
            db_retry_delays = [2, 4, 8]  # Exponential backoff in seconds
            conn = None
            
            for db_attempt in range(max_db_retries):
                try:
                    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30.0)
                    break  # Success, exit retry loop
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e).lower() and db_attempt < max_db_retries - 1:
                        delay = db_retry_delays[min(db_attempt, len(db_retry_delays) - 1)]
                        logger.warning(f"Database locked (attempt {db_attempt + 1}/{max_db_retries}), retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"Database connection failed after {db_attempt + 1} attempts: {e}")
                        raise
                except Exception as e:
                    logger.error(f"Database connection error: {e}")
                    raise
            
            if conn is None:
                raise sqlite3.OperationalError("Failed to connect to database after retries")
            
            cursor = conn.cursor()
            
            # Load progress to get last processed book ID (already loaded above, but reload to ensure consistency)
            progress = self.load_progress()
            last_book_id = progress.get("last_processed_book_id", 0)
            
            # Enhanced logging: Log memory usage if psutil is available
            if PSUTIL_AVAILABLE:
                try:
                    process = psutil.Process()
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    logger.debug(f"[DISCOVERY] Memory usage: {memory_mb:.1f} MB")
                except Exception:
                    pass  # Ignore memory monitoring errors
            
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
            
            # Maximum number of database rows to check per discovery batch
            # This prevents spending too much time checking files that are already uploaded
            # Process whatever new files we find after checking this many rows
            max_rows_to_check = db_batch_size  # Check one batch of 1000 rows, then process whatever we found
            
            # If we have a limit, we need to fetch batches until we have enough NEW files
            # This is because many files may already be completed
            while True:
                # Calculate how many more files we need
                if self.limit:
                    # If limit is set, respect it and keep checking until we have enough
                    needed = self.limit - len(files)
                    if needed <= 0:
                        break
                else:
                    # Continuing mode: try to find batch_size files, but process whatever we find
                    # Stop after checking max_rows_to_check rows to avoid spending too much time
                    # on files that are already uploaded
                    if max_fetched >= max_rows_to_check:
                        # We've checked enough rows (one batch), process whatever new files we found
                        if len(files) > 0:
                            logger.info(f"Found {len(files):,} new files after checking {max_fetched:,} rows. Processing these files.")
                            break
                        # If no new files found yet, check one more batch before giving up
                        if max_fetched >= max_rows_to_check * 2:
                            logger.info(f"Checked {max_fetched:,} rows with no new files. Moving to next batch.")
                            break
                    # If we found batch_size files, we can stop early
                    if len(files) >= self.batch_size:
                        break  # Got enough files for this batch
                
                # Use WHERE b.id > last_id instead of OFFSET (uses index, O(log n) instead of O(n))
                query = base_query + f" AND b.id > {last_book_id} ORDER BY b.id LIMIT {db_batch_size}"
                cursor.execute(query)
                rows = cursor.fetchall()
                
                if not rows:
                    # No more rows in database
                    # CRITICAL: Update progress even when no rows found (if we've processed any rows)
                    # This ensures we don't query the same range repeatedly
                    if max_fetched > 0 and max_book_id > last_book_id:
                        last_book_id = max_book_id
                        progress["last_processed_book_id"] = max_book_id
                        self.save_progress(progress)
                        logger.info(f"Database exhausted at book.id {max_book_id}. Updated last_processed_book_id.")
                    elif max_fetched > 0:
                        # We've processed rows but max_book_id didn't advance (shouldn't happen, but handle it)
                        logger.warning(f"Database exhausted but max_book_id ({max_book_id}) <= last_book_id ({last_book_id}). No progress update.")
                    else:
                        logger.info("Database query returned no rows. No rows processed in this batch.")
                    break
                
                max_fetched += len(rows)
                
                # Process this batch with progress updates
                batch_new_files = 0
                
                # OPTIMIZATION: Batch API checks for better performance
                # Collect file info first, then check in batches
                file_info_batch = []
                file_paths_batch = []
                
                for book_id, path, name, format_ext in rows:
                    # CRITICAL: Track max_book_id FIRST, before any file checks
                    # This ensures we advance even if files are missing or skipped
                    max_book_id = max(max_book_id, book_id)
                    
                    file_path = self.calibre_dir / path / f"{name}.{format_ext.lower()}"
                    
                    # Quick file existence check (only for first few to avoid slowdown)
                    if missing_count < 5:
                        if not file_path.exists() or not file_path.is_file():
                            missing_count += 1
                            logger.debug(f"File not found: {file_path}")
                            continue
                    
                    # Collect file info for batch API check
                    try:
                        file_size = file_path.stat().st_size
                        file_info_batch.append({
                            'file_path': file_path,
                            'file_size': file_size,
                            'file_hash': None  # Size-only check during discovery
                        })
                        file_paths_batch.append(file_path)
                    except (OSError, FileNotFoundError):
                        missing_count += 1
                        if missing_count <= 5:
                            logger.debug(f"File not accessible: {file_path}")
                        continue
                
                # Perform batch API check if we have files to check (in batches of 100)
                if file_info_batch:
                    batch_size = 100
                    for i in range(0, len(file_info_batch), batch_size):
                        batch_chunk = file_info_batch[i:i + batch_size]
                        batch_paths = file_paths_batch[i:i + batch_size]
                        
                        batch_results = self.check_files_exists_via_api_batch(batch_chunk)
                        
                        # Process results
                        for file_path, api_result in zip(batch_paths, batch_results):
                            if api_result is True:
                                # File already exists, skip it during discovery
                                skipped_completed += 1
                            elif api_result is False:
                                # File doesn't exist, add it for processing
                                files.append(file_path)
                                batch_new_files += 1
                            else:
                                # API check failed/unavailable, add file anyway (will be checked during upload)
                                files.append(file_path)
                                batch_new_files += 1
                            
                            # Log progress every process_batch_size files
                            if len(files) - last_progress_log >= process_batch_size:
                                logger.info(f"Found {len(files):,} new files so far (processed {max_fetched:,} rows, "
                                          f"skipped {skipped_completed:,} completed, {missing_count:,} missing)")
                                last_progress_log = len(files)
                            
                            # Stop if we have enough files (after filtering)
                            # If limit is set, use it. Otherwise, use batch_size as target
                            if self.limit is not None and self.limit > 0 and len(files) >= self.limit:
                                break
                            elif self.limit is None and len(files) >= self.batch_size:
                                break  # Got enough files for this batch
                        
                        # Break from batch loop if we have enough files
                        if self.limit is not None and self.limit > 0 and len(files) >= self.limit:
                            break
                        elif self.limit is None and len(files) >= self.batch_size:
                            break
                    # Note: We'll also stop after checking max_rows_to_check rows (handled in outer loop)
                
                # CRITICAL FIX: Update last_book_id even if no files were added (all were duplicates)
                # This prevents infinite loops when all files in a range are already uploaded
                # max_book_id tracks the highest book.id from the database query, regardless of file processing
                # Also update if we've processed rows but max_book_id didn't advance (edge case)
                if rows:
                    if max_book_id > last_book_id:
                        last_book_id = max_book_id
                        progress["last_processed_book_id"] = max_book_id
                        self.save_progress(progress)
                        logger.debug(f"Updated last_processed_book_id to {max_book_id} (processed {len(rows)} rows, found {len(files)} new files)")
                    elif max_book_id == last_book_id and len(rows) > 0:
                        # Edge case: all rows had same book_id (shouldn't happen, but handle it)
                        # Still update to ensure progress is saved
                        progress["last_processed_book_id"] = max_book_id
                        self.save_progress(progress)
                        logger.debug(f"Updated last_processed_book_id to {max_book_id} (all rows had same book_id)")
                elif max_fetched > 0:
                    # No rows in this batch, but we've processed rows before - ensure progress is saved
                    if max_book_id > last_book_id:
                        last_book_id = max_book_id
                        progress["last_processed_book_id"] = max_book_id
                        self.save_progress(progress)
                        logger.debug(f"Updated last_processed_book_id to {max_book_id} (no rows in this batch, but processed {max_fetched} rows total)")
                
                # Log batch completion periodically with enhanced context for LLM
                if max_fetched % (db_batch_size * 10) == 0 or batch_new_files > 0:
                    # Enhanced logging: Include book ID range for LLM analysis
                    book_id_range = f"book.id > {last_book_id - db_batch_size if last_book_id >= db_batch_size else 0}"
                    if max_book_id > last_book_id:
                        book_id_range += f" to {max_book_id:,}"
                    logger.info(f"[DISCOVERY] Processed batch: {book_id_range}, rows={len(rows)}, "
                              f"new_files={batch_new_files}, total_new={len(files):,}, "
                              f"total_processed={max_fetched:,}, skipped={skipped_completed:,}")
                    
                    # Enhanced logging: Periodic memory usage
                    if PSUTIL_AVAILABLE and max_fetched % (db_batch_size * 20) == 0:
                        try:
                            process = psutil.Process()
                            memory_mb = process.memory_info().rss / 1024 / 1024
                            logger.info(f"[DISCOVERY] Memory usage: {memory_mb:.1f} MB (processed {max_fetched:,} rows)")
                        except Exception:
                            pass
                
                # Safety limit: don't fetch more than 10x the requested limit
                if self.limit and max_fetched >= self.limit * 10:
                    logger.warning(f"Fetched {max_fetched:,} rows but only found {len(files):,} new files. "
                                 f"Stopping to avoid excessive database queries.")
                    break
            
            # Enhanced logging: Include book ID range in final summary
            final_book_id_range = f"book.id > {progress.get('last_processed_book_id', 0):,}"
            if max_book_id > progress.get('last_processed_book_id', 0):
                final_book_id_range += f" to {max_book_id:,}"
            logger.info(f"[DISCOVERY] Database query complete: {final_book_id_range}, "
                       f"fetched {max_fetched:,} rows, found {len(files):,} new files")
            
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
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0 and "NOT_FOUND" not in result.stdout.decode("utf-8", errors="ignore") if isinstance(result.stdout, bytes) else result.stdout:
                parts = result.stdout.decode("utf-8", errors="ignore") if isinstance(result.stdout, bytes) else result.stdout.strip().split('|')
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
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        text=True,
                        timeout=30
                    )
                    
                    if replace_result.returncode == 0:
                        logger.info(f"✓ Replaced file with symlink: {calibre_file.name}")
                        logger.debug(f"  Symlink: {mybookshelf_file_path} -> {calibre_container_path}")
                    else:
                        error_msg = replace_result.stderr.decode("utf-8", errors="ignore") if isinstance(result.stderr, bytes) else result.stderr or replace_result.stdout.decode("utf-8", errors="ignore") if isinstance(result.stdout, bytes) else result.stdout
                        logger.warning(f"Failed to create symlink for {calibre_file.name}: {error_msg}")
                else:
                    stdout_text = result.stdout.decode("utf-8", errors="ignore") if isinstance(result.stdout, bytes) else result.stdout
                    logger.warning(f"Could not parse database result for {calibre_file.name}: {stdout_text}")
            else:
                error_msg = result.stderr.decode("utf-8", errors="ignore") if isinstance(result.stderr, bytes) else result.stderr or result.stdout.decode("utf-8", errors="ignore") if isinstance(result.stdout, bytes) else result.stdout
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
        
        # OPTIMIZATION: Don't load hashes at startup - use lazy loading instead
        # Hashes will be loaded on-demand when needed (during file discovery/upload)
        # This reduces memory usage when multiple workers start simultaneously
        # The ensure_hashes_loaded() method will be called automatically when hashes are first needed
        logger.info("Using lazy hash loading - hashes will be loaded on-demand to reduce memory usage")
        
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
            # Enhanced logging: Phase transition and book ID range
            progress_current = self.load_progress()
            current_book_id = progress_current.get("last_processed_book_id", 0)
            logger.info(f"[UPLOAD PHASE] Starting batch {batch_num}: {total_new:,} new files "
                       f"(total completed: {completed_count:,}, current book.id: {current_book_id:,})")
            
            # Enhanced logging: Memory usage at start of upload phase
            if PSUTIL_AVAILABLE:
                try:
                    process = psutil.Process()
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    logger.debug(f"[UPLOAD] Memory usage at batch start: {memory_mb:.1f} MB")
                except Exception:
                    pass
            
            # Process files in this batch with parallel uploads
            success_count = 0
            error_count = 0
            processed_count = 0
            actual_upload_count = 0  # Track actual new uploads (not duplicates)
            
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
            
            # OPTIMIZATION: Batch API check before upload phase
            # Check all files in batches to filter duplicates before processing
            files_to_upload = []
            file_info_batch = []
            file_paths_batch = []
            container_paths_batch = []
            
            # Periodically refresh existing_hashes to pick up files uploaded by other workers
            # Dynamic frequency based on database size, non-blocking background refresh
            files_threshold, seconds_threshold = self._calculate_refresh_frequency()
            if (self.files_processed_since_refresh >= files_threshold or 
                (time.time() - self.last_hash_refresh) > seconds_threshold):
                # Use background refresh to avoid blocking uploads
                self.refresh_existing_hashes(use_incremental=True, background=True)
            
            # Collect file info for batch check
            for file_path, container_path in files_ready:
                try:
                    file_size = file_path.stat().st_size
                    file_info_batch.append({
                        'file_path': file_path,
                        'file_size': file_size,
                        'file_hash': None  # Size-only check first
                    })
                    file_paths_batch.append(file_path)
                    container_paths_batch.append(container_path)
                except (OSError, FileNotFoundError):
                    logger.warning(f"Could not stat file: {file_path.name}")
                    continue
            
            # Perform batch API check (in chunks of 100)
            if file_info_batch:
                batch_size = 100
                for i in range(0, len(file_info_batch), batch_size):
                    batch_chunk = file_info_batch[i:i + batch_size]
                    batch_paths = file_paths_batch[i:i + batch_size]
                    batch_containers = container_paths_batch[i:i + batch_size]
                    
                    batch_results = self.check_files_exists_via_api_batch(batch_chunk)
                    
                    # Process results - only add files that don't exist
                    for file_path, container_path, api_result in zip(batch_paths, batch_containers, batch_results):
                        if api_result is True:
                            # File exists, skip
                            logger.debug(f"File exists via batch API check: {file_path.name}")
                            continue
                        elif api_result is False:
                            # File doesn't exist, add to upload list
                            files_to_upload.append((file_path, container_path))
                        else:
                            # API check failed, add anyway (will check again during upload)
                            files_to_upload.append((file_path, container_path))
            
            # Use ThreadPoolExecutor for parallel uploads within this worker
            with ThreadPoolExecutor(max_workers=self.parallel_uploads) as executor:
                # Submit all upload tasks
                futures = {}
                for file_path, container_path in files_to_upload:
                    # For files that passed batch check, calculate hash if needed
                    file_size = file_path.stat().st_size
                    file_hash = None
                    
                    # If API check was unavailable, fall back to hash-based check
                    # Only load hashes if we really need them (when API is unavailable)
                    if not self._hashes_loaded:
                        # Check if we need hashes (only if API is consistently failing)
                        # For now, proceed with upload - hash will be calculated in upload_file if needed
                        pass
                    
                    # Submit upload task (hash will be calculated in upload_file if needed)
                    future = executor.submit(self.upload_file, file_path, file_hash, progress, container_path)
                    futures[future] = (file_path, file_hash)
                
                # Process completed uploads as they finish
                for future in as_completed(futures):
                    file_path, file_hash = futures[future]
                    processed_count += 1
                    
                    # Enhanced logging: Periodic status updates with book ID range and memory usage
                    if processed_count % 50 == 0:
                        progress_current = self.load_progress()
                        current_book_id = progress_current.get("last_processed_book_id", 0)
                        memory_info = ""
                        if PSUTIL_AVAILABLE:
                            try:
                                process = psutil.Process()
                                memory_mb = process.memory_info().rss / 1024 / 1024
                                memory_info = f", Memory: {memory_mb:.1f} MB"
                            except Exception:
                                pass
                        logger.info(f"[UPLOAD] Batch {batch_num} progress: {processed_count}/{total_new} files processed "
                                  f"(Success: {success_count}, Errors: {error_count}), "
                                  f"book.id: {current_book_id:,}{memory_info}")
                    
                    try:
                        result = future.result()
                        if result:
                            success_count += 1
                            # Check if this was an actual upload or a duplicate
                            # upload_file returns a tuple (success, was_duplicate) or just True/False
                            was_duplicate = False
                            if isinstance(result, tuple):
                                was_duplicate = result[1] if len(result) > 1 else False
                            else:
                                # For backward compatibility, check progress file for status
                                try:
                                    progress_check = self.load_progress()
                                    file_entry = progress_check.get("completed_files", {}).get(file_hash, {})
                                    if file_entry.get("status") == "already_exists":
                                        was_duplicate = True
                                except:
                                    pass
                            
                            if not was_duplicate:
                                actual_upload_count += 1
                            
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
            
            # Enhanced logging: Batch completion with book ID range and memory usage
            progress_final = self.load_progress()
            final_book_id = progress_final.get("last_processed_book_id", 0)
            memory_info = ""
            if PSUTIL_AVAILABLE:
                try:
                    process = psutil.Process()
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    memory_info = f", Memory: {memory_mb:.1f} MB"
                except Exception:
                    pass
            logger.info(f"[UPLOAD] Batch {batch_num} complete. Success: {success_count}, Actual uploads: {actual_upload_count}, Errors: {error_count}, "
                       f"book.id: {final_book_id:,}{memory_info}")
            logger.info(f"[UPLOAD] Total progress: {total_success:,} successful, {total_errors:,} errors")
            
            # Track consecutive batches with all duplicates to detect when worker needs to skip ahead
            # If worker has processed 5+ consecutive batches with 0 actual uploads (all duplicates), skip ahead
            # Check if all successes were duplicates (no actual new uploads)
            all_duplicates = (success_count > 0 and actual_upload_count == 0) or (success_count == 0 and error_count == 0)
            
            if all_duplicates:
                # All files in this batch were duplicates (either filtered before upload or all returned "already exists")
                if not hasattr(self, '_consecutive_duplicate_batches'):
                    self._consecutive_duplicate_batches = 0
                self._consecutive_duplicate_batches += 1
                
                if self._consecutive_duplicate_batches >= 5:
                    # Worker has processed 5+ batches with all duplicates
                    # Skip ahead to avoid wasting time on fully-migrated ranges
                    skip_ahead_amount = 10000  # Skip ahead by 10,000 book.id
                    new_book_id = final_book_id + skip_ahead_amount
                    
                    logger.warning(f"⚠️  Worker has processed {self._consecutive_duplicate_batches} consecutive batches with all duplicates "
                                f"(Success: {success_count}, Actual uploads: {actual_upload_count}). Current book.id: {final_book_id:,}. "
                                f"Skipping ahead to book.id: {new_book_id:,} to avoid fully-migrated range.")
                    
                    # Update progress to skip ahead
                    progress["last_processed_book_id"] = new_book_id
                    self.save_progress(progress)
                    
                    # Reset counter after skip
                    self._consecutive_duplicate_batches = 0
                    
                    # Continue to next iteration of main while loop (will start from new position)
                    logger.info(f"🔄 Skipped ahead to book.id: {new_book_id:,}. Will resume from new position.")
                    continue  # Continue to next batch iteration (will use new_book_id)
            else:
                # Reset counter if batch had any actual uploads
                if hasattr(self, '_consecutive_duplicate_batches'):
                    self._consecutive_duplicate_batches = 0
            
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
