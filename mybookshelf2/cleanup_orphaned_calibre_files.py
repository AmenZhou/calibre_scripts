#!/usr/bin/env python3
"""
Cleanup script to identify orphaned files in Calibre library.

This script:
1. Scans all files in the calibre library folder
2. Checks each file against calibre's metadata.db to see if it's tracked
3. For files tracked by calibre, checks mybookshelf2 database:
   - Calculates file hash (SHA1)
   - Checks if hash exists in Source.hash
   - If hash exists, checks if the specific file path is referenced via symlinks
   - If hash does NOT exist, automatically uploads the file to MyBookshelf2 (if enabled)
4. Generates a detailed report categorizing files:
   - No hash match: Files not in mybookshelf2 (orphaned, or uploaded if upload enabled)
   - Hash exists but no path reference: Duplicate files where hash exists but this specific file path isn't referenced

Usage:
    # Dry-run (report only, no deletion, no upload)
    python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --container mybookshelf2_app
    
    # Actually delete orphaned files (missing files will be uploaded first)
    python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --container mybookshelf2_app --delete
    
    # Disable automatic upload of missing books
    python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --no-upload-missing
    
    # With worker ID for progress tracking
    python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --worker-id 1 --batch-size 1000
    
    # With custom username/password for uploads
    python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --username admin --password mypass
"""

import os
import sys
import json
import logging
import subprocess
import hashlib
import sqlite3
import argparse
import time
from pathlib import Path
from typing import Optional, Dict, Tuple, List, Any, Set
from datetime import datetime
from collections import defaultdict

# Configure logging - will be set up after worker_id is known
logger = logging.getLogger(__name__)

def setup_logging(worker_id: Optional[int] = None):
    """Setup logging with worker-specific log file if worker_id is provided"""
    log_file = f'calibre_cleanup_worker{worker_id}.log' if worker_id is not None else 'calibre_cleanup.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ],
        force=True  # Force reconfiguration if called multiple times
    )


class CalibreCleanup:
    def __init__(self, calibre_dir: str, container: str = "mybookshelf2_app",
                 dry_run: bool = True, worker_id: Optional[int] = None,
                 batch_size: int = 1000, limit: Optional[int] = None,
                 skip_verification: bool = False, require_symlink_check: bool = True,
                 confirm_threshold: int = 1000, backup_dir: Optional[str] = None,
                 username: str = "admin", password: str = "mypassword123",
                 upload_missing: bool = True):
        self.calibre_dir = Path(calibre_dir)
        self.container = container
        self.dry_run = dry_run
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.limit = limit  # Maximum number of files to process
        self.skip_verification = skip_verification
        self.require_symlink_check = require_symlink_check
        self.confirm_threshold = confirm_threshold
        self.backup_dir = backup_dir
        
        # Upload configuration
        self.username = username
        self.password = password
        self.upload_missing = upload_missing
        
        # Progress and report files
        if worker_id is not None:
            self.progress_file = f"calibre_cleanup_progress_worker{worker_id}.json"
            self.report_json = f"calibre_cleanup_report_worker{worker_id}.json"
            self.report_txt = f"calibre_cleanup_report_worker{worker_id}.txt"
            self.backup_file = f"calibre_cleanup_backup_worker{worker_id}.json"
        else:
            self.progress_file = "calibre_cleanup_progress.json"
            self.report_json = "calibre_cleanup_report.json"
            self.report_txt = "calibre_cleanup_report.txt"
            self.backup_file = "calibre_cleanup_backup.json"
        
        # Determine docker command
        try:
            result = subprocess.run(['docker', 'ps'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
            self.docker_cmd = "docker" if result.returncode == 0 else "sudo docker"
        except:
            self.docker_cmd = "sudo docker"
        
        # Statistics
        self.stats = {
            "total_files_scanned": 0,
            "files_in_calibre_db": 0,
            "files_not_in_calibre_db": 0,
            "files_no_hash_match": 0,
            "files_hash_match_no_path": 0,
            "files_hash_match_with_path": 0,
            "errors": 0,
            "files_uploaded": 0,
            "upload_errors": 0
        }
        
        # Deletion statistics (only populated if deletion was performed)
        self.deletion_stats = None
        self.backup_file_path = None
        
        # Categorized files
        self.files_not_in_calibre = []
        self.files_no_hash_match = []
        self.files_hash_match_no_path = []
        self.files_uploaded = []  # Track successfully uploaded files
        
        # Initialize migrator for uploads (use symlinks mode to keep original files)
        self.migrator = None
        if self.upload_missing:
            try:
                from bulk_migrate_calibre import MyBookshelf2Migrator
                self.migrator = MyBookshelf2Migrator(
                    calibre_dir=str(self.calibre_dir),
                    container=self.container,
                    username=self.username,
                    password=self.password,
                    use_symlinks=True,  # Keep original files, use symlinks
                    worker_id=self.worker_id
                )
                logger.info("Upload functionality enabled - missing books will be uploaded to MyBookshelf2")
            except Exception as e:
                logger.warning(f"Failed to initialize migrator: {e}. Upload functionality disabled.")
                self.upload_missing = False
        
        # Cache for database queries
        self.calibre_tracked_files = set()
        self.mybookshelf2_hashes = set()
        self.symlink_paths = set()
        self.symlink_check_succeeded = True  # Will be set to False if check fails/times out
        # Note: symlink_check_succeeded is set in load_symlink_paths() method
        
    def get_file_hash(self, file_path: Path) -> str:
        """Calculate SHA1 hash of file (matches MyBookshelf2's hash algorithm)"""
        sha1 = hashlib.sha1()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    sha1.update(chunk)
            return sha1.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating hash for {file_path}: {e}")
            return ""
    
    def load_calibre_tracked_files(self) -> Set[Path]:
        """Query calibre metadata.db to get all tracked files"""
        db_path = self.calibre_dir / "metadata.db"
        if not db_path.exists():
            logger.error(f"Calibre metadata.db not found at {db_path}")
            return set()
        
        tracked_files = set()
        try:
            # Use read-only mode to prevent database locking
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30.0)
            cursor = conn.cursor()
            
            # Query from plan: SELECT b.id, b.path, d.name, d.format
            query = """
                SELECT b.id, b.path, d.name, d.format
                FROM books b
                JOIN data d ON b.id = d.book
                WHERE d.format IN ('EPUB', 'PDF', 'FB2', 'MOBI', 'AZW3', 'TXT')
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            for book_id, path, name, format_ext in rows:
                # Build file path: calibre_dir / path / name.format
                file_path = self.calibre_dir / path / f"{name}.{format_ext.lower()}"
                tracked_files.add(file_path)
            
            conn.close()
            logger.info(f"Loaded {len(tracked_files):,} tracked files from Calibre database")
            return tracked_files
            
        except Exception as e:
            logger.error(f"Error querying Calibre database: {e}")
            return set()
    
    def get_book_metadata_from_calibre_db(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from Calibre metadata.db for a specific book file"""
        db_path = self.calibre_dir / "metadata.db"
        if not db_path.exists():
            return {}
        
        try:
            # Find book_id from file path
            # File path format: calibre_dir / path / name.format
            # We need to match path and name to find book_id
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30.0)
            cursor = conn.cursor()
            
            # Get relative path and filename from absolute path
            rel_path = file_path.relative_to(self.calibre_dir)
            path_dir = str(rel_path.parent)
            filename_without_ext = file_path.stem
            
            # Query to find book_id
            query = """
                SELECT b.id
                FROM books b
                JOIN data d ON b.id = d.book
                WHERE b.path = ? AND d.name = ?
            """
            cursor.execute(query, (path_dir, filename_without_ext))
            result = cursor.fetchone()
            
            if not result:
                conn.close()
                return {}
            
            book_id = result[0]
            
            # Get book metadata
            # Try with language join first, fallback to direct lang_code if join fails
            book_query = """
                SELECT b.title, b.pubdate, b.series_index, 
                       COALESCE(l.lang_code, b.lang_code) as lang_code, s.name
                FROM books b
                LEFT JOIN languages l ON b.lang_code = l.id
                LEFT JOIN series s ON b.series = s.id
                WHERE b.id = ?
            """
            try:
                cursor.execute(book_query, (book_id,))
            except sqlite3.OperationalError:
                # Fallback: lang_code might be a direct column
                book_query = """
                    SELECT b.title, b.pubdate, b.series_index, b.lang_code, s.name
                    FROM books b
                    LEFT JOIN series s ON b.series = s.id
                    WHERE b.id = ?
                """
                cursor.execute(book_query, (book_id,))
            
            book_result = cursor.fetchone()
            
            metadata = {}
            if book_result:
                title, pubdate, series_index, lang_code, series_name = book_result
                if title:
                    metadata['title'] = title
                if lang_code:
                    # Fix language code (rus -> ru)
                    lang_code = str(lang_code).lower()
                    if lang_code == 'rus':
                        lang_code = 'ru'
                    metadata['language'] = lang_code
                if series_name:
                    metadata['series'] = series_name
                if series_index:
                    metadata['series_index'] = series_index
            
            # Get authors
            authors_query = """
                SELECT a.name
                FROM authors a
                JOIN books_authors_link bal ON a.id = bal.author
                WHERE bal.book = ?
                ORDER BY bal.id
            """
            cursor.execute(authors_query, (book_id,))
            authors = [row[0] for row in cursor.fetchall()]
            if authors:
                metadata['authors'] = authors
            
            conn.close()
            return metadata
        except Exception as e:
            logger.warning(f"Error extracting metadata from Calibre DB for {file_path}: {e}")
            return {}
    
    def upload_file_to_mybookshelf2(self, file_path: Path, file_hash: str) -> Tuple[bool, str]:
        """Upload file to MyBookshelf2 using the migrator's upload_file method.
        Returns (success, message) tuple.
        
        This method delegates to MyBookshelf2Migrator.upload_file() which handles:
        - File preparation (conversion if needed)
        - Metadata extraction from file
        - Container path handling
        - Symlink creation (if use_symlinks=True)
        - CLI upload command execution
        """
        if not self.migrator or not self.upload_missing:
            return (False, "Upload disabled or migrator not initialized")
        
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would upload: {file_path.name}")
            return (True, "dry-run")
        
        try:
            # Create progress dict for upload tracking (same format as bulk migration script)
            progress = {
                "completed_files": {},
                "stats": {}
            }
            
            # Upload file using migrator's upload_file method
            # This method handles all the complexity:
            # - Calls prepare_file_for_upload() for file conversion and metadata extraction
            # - Handles container paths and symlinks
            # - Executes the CLI upload command
            # Returns: (True, False) for new uploads, (True, True) for duplicates, or False for errors
            result = self.migrator.upload_file(file_path, file_hash, progress)
            
            if result:
                # Check return format - migrator returns tuple (success, was_duplicate) or just True
                if isinstance(result, tuple):
                    success, was_duplicate = result
                    if success:
                        if was_duplicate:
                            logger.debug(f"File already exists in MyBookshelf2: {file_path.name}")
                            return (True, "already_exists")
                        else:
                            # New upload successful
                            self.stats["files_uploaded"] += 1
                            self.files_uploaded.append({
                                "path": str(file_path),
                                "hash": file_hash
                            })
                            logger.info(f"Successfully uploaded: {file_path.name}")
                            return (True, "uploaded")
                    else:
                        # Upload failed
                        self.stats["upload_errors"] += 1
                        return (False, "upload_failed")
                else:
                    # Legacy return format (just True) - treat as successful new upload
                    self.stats["files_uploaded"] += 1
                    self.files_uploaded.append({
                        "path": str(file_path),
                        "hash": file_hash
                    })
                    logger.info(f"Successfully uploaded: {file_path.name}")
                    return (True, "uploaded")
            else:
                # Upload failed
                self.stats["upload_errors"] += 1
                logger.warning(f"Upload failed for {file_path.name}")
                return (False, "upload_failed")
        except Exception as e:
            logger.error(f"Error uploading {file_path}: {e}", exc_info=True)
            self.stats["upload_errors"] += 1
            return (False, f"error: {str(e)}")
    
    def load_mybookshelf2_hashes(self) -> Set[str]:
        """Query MyBookshelf2 database for all existing file hashes"""
        script = """
import sys
import os
sys.path.insert(0, '/code')
os.chdir('/code')
from app import app, db
from app import model

try:
    with app.app_context():
        # Get all existing source hashes
        sources = db.session.query(model.Source.hash).all()
        # Return as set of hashes
        result = []
        for (hash_val,) in sources:
            result.append(hash_val)
        print('|'.join(result))
except Exception as e:
    import traceback
    print(f"ERROR: {e}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
"""
        try:
            result = subprocess.run(
                [self.docker_cmd, 'exec', self.container, 'python3', '-c', script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=120  # Allow up to 2 minutes for large databases
            )
            if result.returncode == 0:
                output = result.stdout.strip()
                if output:
                    hashes = set(output.split('|'))
                    logger.info(f"Loaded {len(hashes):,} file hashes from MyBookshelf2 database")
                    return hashes
                else:
                    logger.info("No hashes found in MyBookshelf2 database")
                    return set()
            else:
                stderr_text = result.stderr.strip() if result.stderr else "Unknown error"
                logger.warning(f"Could not load hashes from database: {stderr_text}")
                return set()
        except subprocess.TimeoutExpired:
            logger.warning("Timeout loading hashes from database")
            return set()
        except Exception as e:
            logger.warning(f"Error loading hashes from database: {e}")
            return set()
    
    def load_symlink_paths(self, timeout: int = 300, max_retries: int = 3) -> Set[str]:
        """Get all symlink target paths from MyBookshelf2 container with retry logic"""
        logger.info("Loading symlink paths from MyBookshelf2 container...")
        
        for attempt in range(max_retries):
            try:
                # Find all symlinks and get their resolved paths
                # Use readlink -f to get absolute resolved paths
                logger.info(f"Attempting to load symlink paths (attempt {attempt + 1}/{max_retries})...")
                result = subprocess.run(
                    [self.docker_cmd, 'exec', self.container, 'bash', '-c',
                     'find /data/books -type l -exec readlink -f {} \\;'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=timeout
                )
                if result.returncode == 0:
                    paths = set()
                    output_lines = result.stdout.strip().split('\n')
                    for line in output_lines:
                        if line.strip():
                            paths.add(line.strip())
                    logger.info(f"Loaded {len(paths):,} symlink target paths from MyBookshelf2")
                    return paths
                else:
                    stderr_text = result.stderr.strip() if result.stderr else "Unknown error"
                    logger.warning(f"Error getting symlink paths (attempt {attempt + 1}/{max_retries}): {stderr_text}")
                    if attempt < max_retries - 1:
                        delay = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                        logger.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        logger.error("Failed to load symlink paths after all retries")
                        self.symlink_check_succeeded = False
                        return set()
            except subprocess.TimeoutExpired:
                logger.warning(f"Timeout getting symlink paths (attempt {attempt + 1}/{max_retries}, timeout: {timeout}s)")
                if attempt < max_retries - 1:
                    delay = 2 ** attempt  # Exponential backoff
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error("Timeout getting symlink paths after all retries - symlink verification will be skipped")
                    # Return empty set and mark as failed
                    self.symlink_check_succeeded = False
                    return set()
            except Exception as e:
                logger.warning(f"Error getting symlink paths (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"Failed to load symlink paths after all retries: {e}")
                    self.symlink_check_succeeded = False
                    return set()
        
        # If we get here, all retries failed (shouldn't happen, but safety check)
        logger.error("Unexpected: All retries exhausted but no return statement reached")
        self.symlink_check_succeeded = False
        return set()
    
    def normalize_path_for_matching(self, file_path: Path) -> List[str]:
        """Normalize file path for matching against symlink targets.
        Returns list of possible path representations (host path, container path, etc.)
        """
        # Get absolute path
        abs_path = str(file_path.resolve())
        
        # Possible container paths
        # Common mount points: /calibre_library, /calibre-library, etc.
        container_paths = [
            abs_path,
            abs_path.replace(str(self.calibre_dir.resolve()), '/calibre_library'),
            abs_path.replace(str(self.calibre_dir.resolve()), '/calibre-library'),
            # Also try with spaces in "calibre library"
            abs_path.replace(str(self.calibre_dir.resolve()), '/calibre library'),
        ]
        
        return container_paths
    
    def check_file_path_referenced(self, file_path: Path) -> bool:
        """Check if file path is referenced by any symlink in MyBookshelf2"""
        normalized_paths = self.normalize_path_for_matching(file_path)
        
        for normalized_path in normalized_paths:
            if normalized_path in self.symlink_paths:
                return True
        
        # Also check if any symlink path contains this file path (for relative paths)
        abs_path = str(file_path.resolve())
        for symlink_path in self.symlink_paths:
            if abs_path in symlink_path or symlink_path in abs_path:
                return True
        
        return False
    
    def verify_file_safe_to_delete(self, file_path: Path, file_hash: str) -> Tuple[bool, str]:
        """Verify file is safe to delete. Returns (safe, reason)"""
        # Check 1: Hash must exist in MyBookshelf2
        if file_hash not in self.mybookshelf2_hashes:
            return (False, "Hash no longer exists in MyBookshelf2")
        
        # Check 2: Symlink check must have succeeded (not timed out)
        if not self.symlink_check_succeeded:
            return (False, "Symlink check failed or timed out - cannot verify safety")
        
        # Check 3: Re-verify path is not referenced
        if self.check_file_path_referenced(file_path):
            return (False, "Path is referenced by symlink")
        
        # Check 4: File must exist
        if not file_path.exists():
            return (False, "File does not exist")
        
        return (True, "Safe to delete")
    
    def create_backup_list(self, file_list: List[Dict[str, str]], backup_dir: Optional[str] = None) -> str:
        """Create backup list of files to be deleted"""
        if backup_dir:
            backup_path = Path(backup_dir)
            backup_path.mkdir(parents=True, exist_ok=True)
            backup_file = backup_path / (f"calibre_cleanup_backup_worker{self.worker_id}.json" if self.worker_id else "calibre_cleanup_backup.json")
        else:
            backup_file = Path(f"calibre_cleanup_backup_worker{self.worker_id}.json" if self.worker_id else "calibre_cleanup_backup.json")
        
        backup_data = {
            "timestamp": datetime.now().isoformat(),
            "worker_id": self.worker_id,
            "calibre_library": str(self.calibre_dir),
            "container": self.container,
            "files": []
        }
        
        logger.info(f"Creating backup list for {len(file_list):,} files...")
        for idx, item in enumerate(file_list):
            file_path = Path(item['path'])
            if file_path.exists():
                try:
                    stat = file_path.stat()
                    backup_data["files"].append({
                        "path": str(file_path),
                        "hash": item['hash'],
                        "size": stat.st_size,
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })
                except Exception as e:
                    logger.warning(f"Error getting file stats for {file_path}: {e}")
                    backup_data["files"].append({
                        "path": str(file_path),
                        "hash": item['hash'],
                        "size": None,
                        "modified": None,
                        "error": str(e)
                    })
            else:
                backup_data["files"].append({
                    "path": str(file_path),
                    "hash": item['hash'],
                    "size": None,
                    "modified": None,
                    "error": "File does not exist"
                })
            
            # Log progress every 1000 files
            if (idx + 1) % 1000 == 0:
                logger.info(f"Backed up metadata for {idx + 1:,} files...")
        
        with open(backup_file, 'w') as f:
            json.dump(backup_data, f, indent=2)
        logger.info(f"Backup list saved to {backup_file} ({len(backup_data['files']):,} files)")
        return str(backup_file)
    
    def load_progress(self) -> Dict[str, Any]:
        """Load cleanup progress from file"""
        default_progress = {
            "processed_files": set(),
            "last_processed_file": None,
            "stats": self.stats.copy()
        }
        
        if not os.path.exists(self.progress_file):
            return default_progress
        
        try:
            with open(self.progress_file, 'r') as f:
                progress = json.load(f)
                # Convert processed_files list back to set
                if "processed_files" in progress:
                    progress["processed_files"] = set(progress["processed_files"])
                return progress
        except Exception as e:
            logger.warning(f"Error loading progress file: {e}")
            return default_progress
    
    def save_progress(self, progress: Dict[str, Any]):
        """Save cleanup progress to file"""
        try:
            # Convert set to list for JSON serialization
            progress_copy = progress.copy()
            if "processed_files" in progress_copy:
                progress_copy["processed_files"] = list(progress_copy["processed_files"])
            
            with open(self.progress_file, 'w') as f:
                json.dump(progress_copy, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving progress: {e}")
    
    def scan_calibre_files(self) -> List[Path]:
        """Scan all files in calibre library directory using database query (fast)"""
        logger.info(f"Scanning calibre library directory: {self.calibre_dir}")
        
        # Try to use database query first (much faster for large libraries)
        db_path = self.calibre_dir / "metadata.db"
        if db_path.exists():
            try:
                logger.info("Using Calibre database to find files (fast method)...")
                conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30.0)
                cursor = conn.cursor()
                
                # Query from database (same as migration script)
                query = """
                    SELECT b.id, b.path, d.name, d.format
                    FROM books b
                    JOIN data d ON b.id = d.book
                    WHERE d.format IN ('EPUB', 'PDF', 'FB2', 'MOBI', 'AZW3', 'TXT')
                """
                logger.info("Executing database query...")
                cursor.execute(query)
                logger.info("Fetching results from database...")
                rows = cursor.fetchall()
                logger.info(f"Fetched {len(rows):,} records from database, building file paths...")
                
                files = []
                for idx, (book_id, path, name, format_ext) in enumerate(rows):
                    file_path = self.calibre_dir / path / f"{name}.{format_ext.lower()}"
                    files.append(file_path)
                    # Log progress every 100k files
                    if (idx + 1) % 100000 == 0:
                        logger.info(f"Processed {idx + 1:,} file paths...")
                
                conn.close()
                logger.info(f"Found {len(files):,} book files from Calibre database")
                return files
            except Exception as e:
                logger.warning(f"Error querying Calibre database: {e}, falling back to filesystem scan...")
        
        # Fallback to filesystem scanning (slow but works if database unavailable)
        logger.info("Falling back to filesystem scanning (this may take a while for large libraries)...")
        files = []
        extensions = {'.epub', '.pdf', '.fb2', '.mobi', '.azw3', '.txt'}
        
        try:
            # Walk through directory tree
            for root, dirs, filenames in os.walk(self.calibre_dir):
                # Skip metadata.db and other non-book files
                for filename in filenames:
                    file_path = Path(root) / filename
                    if file_path.suffix.lower() in extensions:
                        files.append(file_path)
                        # Log progress every 10000 files
                        if len(files) % 10000 == 0:
                            logger.info(f"Scanned {len(files):,} files so far...")
            
            logger.info(f"Found {len(files):,} potential book files in calibre library")
            return files
        except Exception as e:
            logger.error(f"Error scanning calibre library: {e}")
            return []
    
    def process_files(self, files: List[Path], progress: Dict[str, Any]):
        """Process files in batches and categorize them"""
        processed_files = progress.get("processed_files", set())
        last_processed = progress.get("last_processed_file", None)
        
        # Load caches
        logger.info("Loading Calibre tracked files...")
        self.calibre_tracked_files = self.load_calibre_tracked_files()
        
        logger.info("Loading MyBookshelf2 hashes...")
        self.mybookshelf2_hashes = self.load_mybookshelf2_hashes()
        
        logger.info("Loading symlink paths...")
        # Initialize to True, load_symlink_paths will set to False on failure
        self.symlink_check_succeeded = True
        self.symlink_paths = self.load_symlink_paths()
        # symlink_check_succeeded is set by load_symlink_paths() - True if successful, False if failed/timed out
        if not self.symlink_check_succeeded:
            logger.warning("Symlink check failed or timed out - files with 'hash match but no path' will require verification")
        
        logger.info("Starting file processing...")
        
        # Process files in batches
        batch_count = 0
        for i in range(0, len(files), self.batch_size):
            batch = files[i:i + self.batch_size]
            batch_count += 1
            
            logger.info(f"Processing batch {batch_count} ({len(batch):,} files)...")
            
            for file_path in batch:
                # Check limit
                if self.limit and self.stats["total_files_scanned"] >= self.limit:
                    logger.info(f"Reached limit of {self.limit} files. Stopping processing.")
                    break
                
                # Skip if already processed
                file_str = str(file_path)
                if file_str in processed_files:
                    continue
                
                try:
                    self.stats["total_files_scanned"] += 1
                    
                    # Check if file is tracked in Calibre
                    if file_path not in self.calibre_tracked_files:
                        self.stats["files_not_in_calibre_db"] += 1
                        self.files_not_in_calibre.append(str(file_path))
                        processed_files.add(file_str)
                        continue
                    
                    self.stats["files_in_calibre_db"] += 1
                    
                    # Calculate hash
                    file_hash = self.get_file_hash(file_path)
                    if not file_hash:
                        self.stats["errors"] += 1
                        processed_files.add(file_str)
                        continue
                    
                    # Check if hash exists in MyBookshelf2
                    if file_hash not in self.mybookshelf2_hashes:
                        # File not in MyBookshelf2 - upload it if enabled
                        if self.upload_missing:
                            logger.info(f"File not in MyBookshelf2, uploading: {file_path.name}")
                            upload_success, upload_msg = self.upload_file_to_mybookshelf2(file_path, file_hash)
                            
                            if upload_success:
                                if upload_msg == "already_exists":
                                    logger.info(f"File already exists in MyBookshelf2: {file_path.name}")
                                    # Re-check hash after upload (might have been added by another worker)
                                    # For now, treat as successfully handled
                                    self.stats["files_hash_match_with_path"] += 1
                                elif upload_msg == "uploaded":
                                    logger.info(f"Successfully uploaded: {file_path.name}")
                                    # File uploaded, keep it (don't mark for deletion)
                                    self.stats["files_hash_match_with_path"] += 1
                                else:
                                    # Upload failed or dry-run
                                    self.stats["files_no_hash_match"] += 1
                                    self.files_no_hash_match.append({
                                        "path": str(file_path),
                                        "hash": file_hash,
                                        "upload_attempted": True,
                                        "upload_status": upload_msg
                                    })
                            else:
                                # Upload failed
                                logger.warning(f"Failed to upload {file_path.name}: {upload_msg}")
                                self.stats["files_no_hash_match"] += 1
                                self.files_no_hash_match.append({
                                    "path": str(file_path),
                                    "hash": file_hash,
                                    "upload_attempted": True,
                                    "upload_status": upload_msg
                                })
                        else:
                            # Upload disabled, mark for deletion
                            self.stats["files_no_hash_match"] += 1
                            self.files_no_hash_match.append({
                                "path": str(file_path),
                                "hash": file_hash
                            })
                        
                        processed_files.add(file_str)
                        continue
                    
                    # Hash exists, check if path is referenced
                    if self.check_file_path_referenced(file_path):
                        self.stats["files_hash_match_with_path"] += 1
                    else:
                        self.stats["files_hash_match_no_path"] += 1
                        self.files_hash_match_no_path.append({
                            "path": str(file_path),
                            "hash": file_hash
                        })
                    
                    processed_files.add(file_str)
                    last_processed = file_str
                    
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
                    self.stats["errors"] += 1
                    processed_files.add(file_str)
            
            # Save progress after each batch
            progress["processed_files"] = processed_files
            progress["last_processed_file"] = last_processed
            progress["stats"] = self.stats.copy()
            self.save_progress(progress)
            
            logger.info(f"Batch {batch_count} complete. Stats: {self.stats}")
            
            # Check if we've reached the limit
            if self.limit and self.stats["total_files_scanned"] >= self.limit:
                logger.info(f"Reached limit of {self.limit} files. Stopping processing.")
                break
    
    def generate_reports(self):
        """Generate JSON and text reports"""
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "calibre_library": str(self.calibre_dir),
            "container": self.container,
            "dry_run": self.dry_run,
            "worker_id": self.worker_id,
            "statistics": self.stats.copy(),
            "files_not_in_calibre_db": self.files_not_in_calibre,
            "files_no_hash_match": self.files_no_hash_match,
            "files_hash_match_no_path": self.files_hash_match_no_path,
            "symlink_check_succeeded": self.symlink_check_succeeded,
            "backup_file": self.backup_file_path,
            "deletion_stats": self.deletion_stats,
            "files_uploaded": self.files_uploaded,
            "upload_statistics": {
                "files_uploaded": self.stats["files_uploaded"],
                "upload_errors": self.stats["upload_errors"]
            }
        }
        
        # Save JSON report
        with open(self.report_json, 'w') as f:
            json.dump(report_data, f, indent=2)
        logger.info(f"JSON report saved to {self.report_json}")
        
        # Generate text report
        with open(self.report_txt, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("  Calibre Library Cleanup Report\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Calibre Library: {self.calibre_dir}\n")
            f.write(f"Container: {self.container}\n")
            f.write(f"Mode: {'DRY-RUN (no files deleted)' if self.dry_run else 'DELETE (files will be removed)'}\n")
            if self.worker_id:
                f.write(f"Worker ID: {self.worker_id}\n")
            f.write("\n")
            
            # Statistics
            f.write("=" * 80 + "\n")
            f.write("  Statistics\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Total files scanned: {self.stats['total_files_scanned']:,}\n")
            f.write(f"Files in Calibre DB: {self.stats['files_in_calibre_db']:,}\n")
            f.write(f"Files not in Calibre DB: {self.stats['files_not_in_calibre_db']:,}\n")
            f.write(f"Files with no hash match (orphaned from MyBookshelf2): {self.stats['files_no_hash_match']:,}\n")
            f.write(f"Files with hash match but no path reference (duplicates): {self.stats['files_hash_match_no_path']:,}\n")
            f.write(f"Files with hash match and path reference (in use): {self.stats['files_hash_match_with_path']:,}\n")
            f.write(f"Errors: {self.stats['errors']:,}\n")
            f.write("\n")
            
            # Upload statistics
            if self.stats["files_uploaded"] > 0 or self.stats["upload_errors"] > 0:
                f.write("=" * 80 + "\n")
                f.write("  Upload Statistics\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Files uploaded to MyBookshelf2: {self.stats['files_uploaded']:,}\n")
                f.write(f"Upload errors: {self.stats['upload_errors']:,}\n")
                f.write("\n")
            
            # Warnings
            if not self.symlink_check_succeeded:
                f.write("=" * 80 + "\n")
                f.write("  WARNING\n")
                f.write("=" * 80 + "\n\n")
                f.write("⚠️  Symlink check failed or timed out!\n")
                f.write("Files with 'hash match but no path' could not be verified as safe to delete.\n")
                f.write("These files may still be in use by MyBookshelf2 via symlinks.\n")
                f.write("Deletion of these files was skipped for safety.\n\n")
            
            # Deletion statistics (if deletion was performed)
            if self.deletion_stats:
                f.write("=" * 80 + "\n")
                f.write("  Deletion Statistics\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Files deleted: {self.deletion_stats['deleted']:,}\n")
                f.write(f"Files skipped: {self.deletion_stats['skipped']:,}\n")
                f.write(f"Files failed: {self.deletion_stats['failed']:,}\n")
                if 'verified' in self.deletion_stats:
                    f.write(f"Files verified: {self.deletion_stats['verified']:,}\n")
                if self.backup_file_path:
                    f.write(f"Backup file: {self.backup_file_path}\n")
                f.write("\n")
            
            # Files not in Calibre DB
            if self.files_not_in_calibre:
                f.write("=" * 80 + "\n")
                f.write(f"  Files Not in Calibre DB ({len(self.files_not_in_calibre):,})\n")
                f.write("=" * 80 + "\n\n")
                for file_path in self.files_not_in_calibre[:100]:  # Show first 100
                    f.write(f"{file_path}\n")
                if len(self.files_not_in_calibre) > 100:
                    f.write(f"... and {len(self.files_not_in_calibre) - 100:,} more\n")
                f.write("\n")
            
            # Files with no hash match
            if self.files_no_hash_match:
                f.write("=" * 80 + "\n")
                f.write(f"  Files with No Hash Match - Orphaned from MyBookshelf2 ({len(self.files_no_hash_match):,})\n")
                f.write("=" * 80 + "\n\n")
                f.write("These files are tracked by Calibre but not in MyBookshelf2.\n")
                f.write("They can be safely deleted if not needed.\n\n")
                for item in self.files_no_hash_match[:100]:  # Show first 100
                    f.write(f"{item['path']} (hash: {item['hash']})\n")
                if len(self.files_no_hash_match) > 100:
                    f.write(f"... and {len(self.files_no_hash_match) - 100:,} more\n")
                f.write("\n")
            
            # Files with hash match but no path reference
            if self.files_hash_match_no_path:
                f.write("=" * 80 + "\n")
                f.write(f"  Files with Hash Match but No Path Reference - Duplicates ({len(self.files_hash_match_no_path):,})\n")
                f.write("=" * 80 + "\n\n")
                f.write("These files have the same hash as files in MyBookshelf2, but this specific path is not referenced.\n")
                f.write("They are likely duplicate files that can be safely deleted.\n\n")
                for item in self.files_hash_match_no_path[:100]:  # Show first 100
                    f.write(f"{item['path']} (hash: {item['hash']})\n")
                if len(self.files_hash_match_no_path) > 100:
                    f.write(f"... and {len(self.files_hash_match_no_path) - 100:,} more\n")
                f.write("\n")
        
        logger.info(f"Text report saved to {self.report_txt}")
    
    def delete_files(self, file_list: List[Dict[str, str]], verify: bool = True, batch_size: int = 100) -> Dict[str, int]:
        """Delete files from the list with verification"""
        stats = {"deleted": 0, "failed": 0, "skipped": 0, "verified": 0}
        
        total_files = len(file_list)
        logger.info(f"Deleting {total_files:,} files (verification: {'enabled' if verify else 'disabled'})...")
        
        # Delete in batches for progress tracking
        for batch_idx in range(0, total_files, batch_size):
            batch = file_list[batch_idx:batch_idx + batch_size]
            batch_num = (batch_idx // batch_size) + 1
            total_batches = (total_files + batch_size - 1) // batch_size
            
            logger.info(f"Processing deletion batch {batch_num}/{total_batches} ({len(batch):,} files)...")
            
            for item in batch:
                file_path = Path(item['path'])
                
                # Verify before deletion
                if verify:
                    safe, reason = self.verify_file_safe_to_delete(file_path, item['hash'])
                    if not safe:
                        logger.warning(f"Skipping {file_path.name}: {reason}")
                        stats["skipped"] += 1
                        continue
                    stats["verified"] += 1
                
                # Delete file
                try:
                    if file_path.exists():
                        file_path.unlink()
                        stats["deleted"] += 1
                        logger.debug(f"Deleted: {file_path}")
                    else:
                        logger.warning(f"File not found (may have been deleted already): {file_path}")
                        stats["skipped"] += 1
                except Exception as e:
                    logger.error(f"Error deleting {file_path}: {e}")
                    stats["failed"] += 1
            
            logger.info(f"Batch {batch_num} complete: {stats['deleted']:,} deleted, {stats['skipped']:,} skipped, {stats['failed']:,} failed")
        
        return stats
    
    def run(self):
        """Main cleanup process"""
        logger.info("=" * 80)
        logger.info("  Calibre Library Cleanup")
        logger.info("=" * 80)
        logger.info(f"Calibre Library: {self.calibre_dir}")
        logger.info(f"Container: {self.container}")
        logger.info(f"Mode: {'DRY-RUN' if self.dry_run else 'DELETE'}")
        if self.worker_id:
            logger.info(f"Worker ID: {self.worker_id}")
        logger.info("")
        
        # Load progress
        progress = self.load_progress()
        
        # Scan files
        logger.info("Scanning calibre library files...")
        all_files = self.scan_calibre_files()
        
        # Process files
        self.process_files(all_files, progress)
        
        # Generate reports
        logger.info("Generating reports...")
        self.generate_reports()
        
        # Delete files if not dry-run
        if not self.dry_run:
            logger.info("=" * 80)
            logger.info("DELETING FILES")
            logger.info("=" * 80)
            
            # Check if symlink check failed and require_symlink_check is True
            if not self.symlink_check_succeeded and self.require_symlink_check:
                logger.error("=" * 80)
                logger.error("DELETION ABORTED: Symlink check failed or timed out")
                logger.error("=" * 80)
                logger.error("Cannot safely verify which files are duplicates without symlink information.")
                logger.error("Files with 'hash match but no path' cannot be verified as safe to delete.")
                logger.error("")
                logger.error("Options:")
                logger.error("  1. Wait and retry (symlink check may succeed on retry)")
                logger.error("  2. Run with --require-symlink-check=false to proceed anyway (NOT RECOMMENDED)")
                logger.error("  3. Fix the symlink timeout issue and rerun")
                return
            
            # Collect all files to delete
            files_to_delete = []
            
            # Add files with no hash match
            if self.files_no_hash_match:
                files_to_delete.extend(self.files_no_hash_match)
            
            # Add files with hash match but no path reference
            if self.files_hash_match_no_path:
                files_to_delete.extend(self.files_hash_match_no_path)
            
            if not files_to_delete:
                logger.info("No files to delete.")
                return
            
            total_to_delete = len(files_to_delete)
            
            # Require confirmation for large deletions
            if total_to_delete >= self.confirm_threshold:
                logger.warning("=" * 80)
                logger.warning(f"WARNING: About to delete {total_to_delete:,} files!")
                logger.warning("=" * 80)
                response = input(f"Type 'DELETE {total_to_delete}' to confirm deletion: ")
                if response != f'DELETE {total_to_delete}':
                    logger.info("Deletion cancelled by user.")
                    return
            
            # Create backup before deletion
            logger.info("Creating backup list before deletion...")
            self.backup_file_path = self.create_backup_list(files_to_delete, self.backup_dir)
            logger.info(f"Backup list saved to: {self.backup_file_path}")
            
            # Delete files with verification
            verify = not self.skip_verification
            logger.info(f"Starting deletion (verification: {'enabled' if verify else 'disabled'})...")
            
            deletion_stats = self.delete_files(files_to_delete, verify=verify)
            self.deletion_stats = deletion_stats
            
            logger.info("=" * 80)
            logger.info("DELETION COMPLETE")
            logger.info("=" * 80)
            logger.info(f"Deleted: {deletion_stats['deleted']:,} files")
            logger.info(f"Skipped: {deletion_stats['skipped']:,} files")
            logger.info(f"Failed: {deletion_stats['failed']:,} files")
            if verify:
                logger.info(f"Verified: {deletion_stats['verified']:,} files")
            logger.info(f"Backup saved to: {self.backup_file_path}")
            logger.info("=" * 80)
        else:
            logger.info("=" * 80)
            logger.info("DRY-RUN: No files were deleted")
            logger.info(f"To actually delete files, run with --delete flag")
            logger.info("=" * 80)
        
        logger.info("Cleanup complete!")


def main():
    parser = argparse.ArgumentParser(
        description='Cleanup orphaned files in Calibre library',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry-run (report only)
  python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library
  
  # Actually delete orphaned files
  python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --delete
  
  # With worker ID and custom batch size
  python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --worker-id 1 --batch-size 500
        """
    )
    parser.add_argument(
        'calibre_dir',
        help='Path to Calibre library directory'
    )
    parser.add_argument(
        '--container',
        default='mybookshelf2_app',
        help='Docker container name (default: mybookshelf2_app)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=True,
        help='Report only, do not delete files (default)'
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Actually delete orphaned files (overrides --dry-run)'
    )
    parser.add_argument(
        '--worker-id',
        type=int,
        help='Worker ID for progress tracking (optional)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of files to process per batch (default: 1000)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of files to process (for testing, default: no limit)'
    )
    parser.add_argument(
        '--skip-verification',
        action='store_true',
        help='Skip verification before deletion (NOT RECOMMENDED, default: False)'
    )
    parser.add_argument(
        '--require-symlink-check',
        action='store_true',
        default=True,
        help='Require successful symlink check before deletion (default: True). Use --no-require-symlink-check to disable.'
    )
    parser.add_argument(
        '--no-require-symlink-check',
        dest='require_symlink_check',
        action='store_false',
        help='Allow deletion even if symlink check failed (NOT RECOMMENDED)'
    )
    parser.add_argument(
        '--confirm-threshold',
        type=int,
        default=1000,
        help='Require confirmation prompt for deletions above this count (default: 1000)'
    )
    parser.add_argument(
        '--backup-dir',
        type=str,
        help='Directory to save backup file (default: current directory)'
    )
    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Run verification only, do not delete files (overrides --delete)'
    )
    parser.add_argument(
        '--username',
        type=str,
        default='admin',
        help='MyBookshelf2 username for uploads (default: admin)'
    )
    parser.add_argument(
        '--password',
        type=str,
        default='mypassword123',
        help='MyBookshelf2 password for uploads (default: mypassword123)'
    )
    parser.add_argument(
        '--no-upload-missing',
        action='store_true',
        help='Disable automatic upload of missing books (default: upload enabled)'
    )
    
    args = parser.parse_args()
    
    # Setup logging first (needs worker_id)
    setup_logging(args.worker_id)
    
    # Determine dry-run mode (verify-only overrides delete)
    dry_run = not args.delete or args.verify_only
    
    # Create cleanup instance
    cleanup = CalibreCleanup(
        calibre_dir=args.calibre_dir,
        container=args.container,
        dry_run=dry_run,
        worker_id=args.worker_id,
        batch_size=args.batch_size,
        limit=args.limit,
        skip_verification=args.skip_verification,
        require_symlink_check=args.require_symlink_check,
        confirm_threshold=args.confirm_threshold,
        backup_dir=args.backup_dir,
        username=args.username,
        password=args.password,
        upload_missing=not args.no_upload_missing
    )
    
    # If verify-only mode, run verification and exit
    if args.verify_only:
        logger.info("=" * 80)
        logger.info("  VERIFICATION MODE")
        logger.info("=" * 80)
        logger.info("Running verification only - no files will be deleted")
        logger.info("")
        
        # Load progress and run processing to get file lists
        progress = cleanup.load_progress()
        all_files = cleanup.scan_calibre_files()
        cleanup.process_files(all_files, progress)
        
        # Run verification on files marked for deletion
        logger.info("=" * 80)
        logger.info("  VERIFICATION RESULTS")
        logger.info("=" * 80)
        
        verify_results = {"safe": 0, "unsafe": 0, "errors": 0}
        unsafe_files = []
        
        # Verify files with no hash match
        if cleanup.files_no_hash_match:
            logger.info(f"Verifying {len(cleanup.files_no_hash_match):,} files with no hash match...")
            for item in cleanup.files_no_hash_match:
                file_path = Path(item['path'])
                file_hash = item['hash']
                safe, reason = cleanup.verify_file_safe_to_delete(file_path, file_hash)
                if safe:
                    verify_results["safe"] += 1
                else:
                    verify_results["unsafe"] += 1
                    unsafe_files.append({"path": item['path'], "reason": reason})
        
        # Verify files with hash match but no path
        if cleanup.files_hash_match_no_path:
            logger.info(f"Verifying {len(cleanup.files_hash_match_no_path):,} files with hash match but no path...")
            for item in cleanup.files_hash_match_no_path:
                file_path = Path(item['path'])
                file_hash = item['hash']
                safe, reason = cleanup.verify_file_safe_to_delete(file_path, file_hash)
                if safe:
                    verify_results["safe"] += 1
                else:
                    verify_results["unsafe"] += 1
                    unsafe_files.append({"path": item['path'], "reason": reason})
        
        logger.info("")
        logger.info(f"Verification complete:")
        logger.info(f"  Safe to delete: {verify_results['safe']:,}")
        logger.info(f"  Unsafe to delete: {verify_results['unsafe']:,}")
        
        if unsafe_files:
            logger.warning(f"\nUnsafe files (first 10):")
            for item in unsafe_files[:10]:
                logger.warning(f"  {item['path']}: {item['reason']}")
            if len(unsafe_files) > 10:
                logger.warning(f"  ... and {len(unsafe_files) - 10:,} more")
        
        cleanup.generate_reports()
        return 0
    
    # Run cleanup
    try:
        cleanup.run()
        return 0
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Error during cleanup: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
