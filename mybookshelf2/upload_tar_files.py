#!/usr/bin/env python3
"""
Tar file upload script for MyBookshelf2
Processes tar files containing books and uploads them to MyBookshelf2
Supports multiple workers and auto-monitor supervision
"""

import os
import sys
import json
import logging
import subprocess
import tempfile
import shutil
import hashlib
import fcntl
import time
import threading
import requests
import mimetypes
import tarfile
import glob
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, Dict, Tuple, List, Any, Iterator

# Import upload logic from bulk_migrate_calibre.py
# We'll reuse the MyBookshelf2Migrator class for actual uploads
sys.path.insert(0, str(Path(__file__).parent))
from bulk_migrate_calibre import MyBookshelf2Migrator

# Try to import psutil for memory monitoring (optional)
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# Configure logging - will be set up after worker_id is known
logger = logging.getLogger(__name__)


class TarFileUploader:
    """Upload books from tar files to MyBookshelf2"""
    
    def __init__(self, tar_source_dir: str, container: str = "mybookshelf2_app",
                 username: str = "admin", password: str = "mypassword123",
                 worker_id: Optional[int] = None, tar_list: Optional[List[str]] = None,
                 parallel_uploads: int = 1, batch_size: int = 1000,
                 temp_dir: Optional[str] = None):
        self.tar_source_dir = Path(tar_source_dir)
        self.container = container
        self.username = username
        self.password = password
        self.worker_id = worker_id
        self.tar_list = tar_list  # List of tar files to process (for worker mode)
        self.parallel_uploads = parallel_uploads
        self.batch_size = batch_size
        
        # Use worker-specific progress file if worker_id is provided
        if worker_id is not None:
            self.progress_file = f"migration_progress_worker{worker_id}.json"
            self.error_file = f"migration_errors_worker{worker_id}.log"
            log_file = f"migration_worker{worker_id}.log"
        else:
            self.progress_file = "tar_upload_progress.json"
            self.error_file = "tar_upload_errors.log"
            log_file = "tar_upload.log"
        
        # Configure logging with worker-specific log file
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ],
            force=True  # Override any existing configuration
        )
        
        # Temp directory for tar extraction (on same partition for performance)
        if temp_dir is None:
            partition_base = "/media/haimengzhou/78613a5d-17be-413e-8691-908154970815"
            temp_dir = str(Path(partition_base) / "tar_extraction_temp")
        self.temp_extraction_base = Path(temp_dir)
        self.temp_extraction_base.mkdir(parents=True, exist_ok=True)
        
        # Processed directory for completed tar files (same directory as tar_source_dir)
        self.processed_dir = self.tar_source_dir / "processed"
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Thread-safe progress tracking
        self.progress_lock = threading.Lock()
        
        # Detect if running inside Docker container
        self.running_in_container = os.path.exists('/.dockerenv') or os.environ.get('container') == 'docker'
        
        # Determine docker command (only needed if running outside container)
        if self.running_in_container:
            self.docker_cmd = None
            logger.info("Running inside Docker container - will use direct CLI calls")
        else:
            try:
                result = subprocess.run(['docker', 'ps'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
                self.docker_cmd = "docker" if result.returncode == 0 else "sudo docker"
            except:
                self.docker_cmd = "sudo docker"
            logger.info(f"Running on host - using docker command: {self.docker_cmd}")
        
        # Create migrator instance for actual uploads (reuse existing logic)
        # We'll use a dummy calibre_dir since we're processing extracted files
        dummy_calibre_dir = str(self.temp_extraction_base)
        self.migrator = MyBookshelf2Migrator(
            dummy_calibre_dir, container, username, password,
            delete_existing=False, limit=None, use_symlinks=False,
            worker_id=worker_id, db_offset=None,
            parallel_uploads=parallel_uploads, batch_size=batch_size
        )
        
        # Override progress file to use our tar-specific one
        self.migrator.progress_file = self.progress_file
        self.migrator.error_file = self.error_file
    
    def detect_file_type(self, file_path: Path) -> Optional[str]:
        """Detect ebook file type by content (for files without extensions)"""
        try:
            # Try 'file' command first (most reliable)
            result = subprocess.run(
                ['file', str(file_path)],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                output = result.stdout.lower()
                if 'mobipocket' in output or 'mobi' in output:
                    return '.mobi'
                elif 'epub' in output or 'zip archive' in output:
                    return '.epub'
                elif 'pdf' in output:
                    return '.pdf'
                elif 'xml' in output and 'fb2' in output:
                    return '.fb2'
            
            # Fallback to magic bytes
            with open(file_path, 'rb') as f:
                # Read first 64 bytes for magic bytes
                header = f.read(64)
                
                # MOBI: Check for "BOOKMOBI" at offset 0x3C (60)
                if len(header) > 60:
                    if header[60:68] == b'BOOKMOBI':
                        return '.mobi'
                
                # EPUB: ZIP signature "PK" at start
                if header[:2] == b'PK':
                    return '.epub'
                
                # PDF: "%PDF" at start
                if header[:4] == b'%PDF':
                    return '.pdf'
                
                # FB2: XML declaration
                if header[:5] == b'<?xml' or header[:6] == b'<FictionBook':
                    return '.fb2'
        
        except Exception as e:
            logger.debug(f"Error detecting file type for {file_path.name}: {e}")
        
        return None
    
    def find_tar_files(self) -> List[Path]:
        """Find all tar files in source directory"""
        tar_files = []
        for item in self.tar_source_dir.iterdir():
            if item.is_file() and item.suffix.lower() == '.tar':
                tar_files.append(item)
        return sorted(tar_files)
    
    def find_existing_extraction_folder(self, tar_name: str) -> Optional[Path]:
        """Find existing extraction folder for a tar file (most recent one)"""
        if not self.temp_extraction_base.exists():
            return None
        
        # Look for extraction folders matching this tar name
        pattern = f"{tar_name}_*"
        matching_dirs = []
        
        for item in self.temp_extraction_base.iterdir():
            if item.is_dir() and item.name.startswith(f"{tar_name}_"):
                matching_dirs.append(item)
        
        if not matching_dirs:
            return None
        
        # Find folders that actually have extracted files (not just empty directories)
        # Prefer folders with files over empty ones, even if they're older
        valid_folders = []
        for dir_path in matching_dirs:
            # Find the actual extracted folder (may be nested)
            extracted_folder = None
            
            # Check if there's a subdirectory with the tar name
            for subdir in dir_path.iterdir():
                if subdir.is_dir() and subdir.name == tar_name:
                    extracted_folder = subdir
                    break
            
            # If no subdirectory found, check if the directory itself has files
            if not extracted_folder:
                # Check if dir_path itself contains files
                files_in_dir = list(dir_path.rglob('*'))
                if any(f.is_file() for f in files_in_dir):
                    extracted_folder = dir_path
                else:
                    # Check nested directories recursively
                    for subdir in dir_path.rglob('*'):
                        if subdir.is_dir() and subdir.name == tar_name:
                            extracted_folder = subdir
                            break
            
            # Verify it has files
            if extracted_folder and extracted_folder.exists():
                file_count = sum(1 for _ in extracted_folder.rglob('*') if _.is_file())
                if file_count > 0:
                    valid_folders.append((extracted_folder, file_count, dir_path.stat().st_mtime))
        
        if not valid_folders:
            return None
        
        # Sort by file count (most files first), then by modification time (most recent first)
        valid_folders.sort(key=lambda x: (x[1], x[2]), reverse=True)
        best_folder = valid_folders[0][0]
        
        logger.info(f"Found existing extraction folder: {best_folder} ({valid_folders[0][1]} files)")
        return best_folder
    
    def find_orphaned_extraction_folders(self, completed_tars: set, all_assigned_tars: set) -> List[Tuple[Path, str]]:
        """
        Find extraction folders that don't have corresponding assigned tar files.
        Returns list of (extracted_folder, tar_name) tuples.
        
        Args:
            completed_tars: Set of tar file names that are already completed
            all_assigned_tars: Set of tar file names assigned to all workers
        """
        if not self.temp_extraction_base.exists():
            return []
        
        orphaned_folders = []
        all_tar_files = {f.stem: f for f in self.find_tar_files()}  # tar_name (without .tar) -> tar_path
        
        # Scan all extraction folders in temp_extraction_base
        for item in self.temp_extraction_base.iterdir():
            if not item.is_dir():
                continue
            
            # Extract tar name from folder name (format: {tar_name}_{timestamp})
            # Example: pilimi-zlib2-16470000-16579999_1766592979
            if '_' not in item.name:
                continue
            
            # Try to match folder name to tar file name
            # Split by last underscore to get tar_name and timestamp
            parts = item.name.rsplit('_', 1)
            if len(parts) != 2:
                continue
            
            potential_tar_name = parts[0]
            tar_file_name = f"{potential_tar_name}.tar"
            
            # Check if this tar file exists in source directory
            if potential_tar_name not in all_tar_files:
                continue
            
            # Check if this tar file is assigned to any worker or already completed
            if tar_file_name in all_assigned_tars or tar_file_name in completed_tars:
                continue
            
            # This is an orphaned folder - find the actual extracted folder
            extracted_folder = None
            
            # Check if there's a subdirectory with the tar name
            for subdir in item.iterdir():
                if subdir.is_dir() and subdir.name == potential_tar_name:
                    extracted_folder = subdir
                    break
            
            # If no subdirectory found, check if the directory itself has files
            if not extracted_folder:
                files_in_dir = list(item.rglob('*'))
                if any(f.is_file() for f in files_in_dir):
                    extracted_folder = item
                else:
                    # Check nested directories recursively
                    for subdir in item.rglob('*'):
                        if subdir.is_dir() and subdir.name == potential_tar_name:
                            extracted_folder = subdir
                            break
            
            # Verify it has files
            if extracted_folder and extracted_folder.exists():
                file_count = sum(1 for _ in extracted_folder.rglob('*') if _.is_file())
                if file_count > 0:
                    orphaned_folders.append((extracted_folder, tar_file_name, file_count))
        
        # Sort by file count (most files first) to prioritize larger folders
        orphaned_folders.sort(key=lambda x: x[2], reverse=True)
        
        if orphaned_folders:
            logger.info(f"Found {len(orphaned_folders)} orphaned extraction folder(s):")
            for extracted_folder, tar_name, file_count in orphaned_folders[:5]:  # Log first 5
                logger.info(f"  - {tar_name}: {extracted_folder} ({file_count:,} files)")
            if len(orphaned_folders) > 5:
                logger.info(f"  ... and {len(orphaned_folders) - 5} more")
        
        return [(folder, tar_name) for folder, tar_name, _ in orphaned_folders]
    
    def process_orphaned_extraction_folder(self, extracted_folder: Path, tar_name: str) -> Dict[str, Any]:
        """
        Process an orphaned extraction folder (similar to process_tar_file but without extraction).
        This handles folders that were extracted but their tar files weren't assigned to any worker.
        """
        result = {
            "tar_file": tar_name,
            "status": "failed",
            "files_processed": 0,
            "files_uploaded": 0,
            "errors": 0,
            "started_at": time.time(),
            "completed_at": None,
            "is_orphaned": True  # Mark as orphaned for tracking
        }
        
        logger.info(f"=== Processing orphaned extraction folder: {tar_name} ===")
        logger.info(f"Extracted folder: {extracted_folder}")
        
        try:
            # Load progress first
            progress = self.load_progress()
            completed_hashes = set(progress.get("completed_files", {}).keys())
            
            # Also load all workers' progress to avoid processing files already done by other workers
            all_workers_completed = self.load_all_workers_completed_files()
            completed_hashes.update(all_workers_completed)
            if all_workers_completed:
                logger.info(f"Loaded {len(all_workers_completed)} completed files from all workers' progress files")
            
            # Process files incrementally as they're found (upload immediately after detection)
            total_success = 0
            total_errors = 0
            batch_num = 0
            batch_files = []
            total_files_found = 0
            
            # Use incremental file finder that processes files as they're discovered
            for file_path in self.find_and_process_ebook_files_incremental(extracted_folder, progress, completed_hashes, extracted_folder):
                total_files_found += 1
                batch_files.append(file_path)
                
                # When batch is full, process it immediately
                if len(batch_files) >= self.batch_size:
                    batch_num += 1
                    logger.info(f"[UPLOAD PHASE] Processing batch {batch_num} from orphaned {tar_name}: {len(batch_files):,} files (found {total_files_found:,} total so far)")
                    
                    # Process batch (same logic as process_tar_file)
                    batch_result = self._process_batch(batch_files, progress, completed_hashes, extracted_folder)
                    total_success += batch_result["success"]
                    total_errors += batch_result["errors"]
                    
                    # Save progress after each batch
                    self.save_progress(progress)
                    batch_files = []
            
            # Process final batch if any files remain
            if batch_files:
                batch_num += 1
                logger.info(f"[UPLOAD PHASE] Processing final batch {batch_num} from orphaned {tar_name}: {len(batch_files):,} files")
                batch_result = self._process_batch(batch_files, progress, completed_hashes, extracted_folder)
                total_success += batch_result["success"]
                total_errors += batch_result["errors"]
                self.save_progress(progress)
            
            result["files_processed"] = total_files_found
            result["files_uploaded"] = total_success
            result["errors"] = total_errors
            result["status"] = "completed" if total_errors == 0 else "completed_with_errors"
            result["completed_at"] = time.time()
            
            logger.info(f"Orphaned folder {tar_name} processing complete: {result['status']}, {total_success:,} uploaded, {total_errors:,} errors")
            
        except Exception as e:
            logger.error(f"Error processing orphaned extraction folder {tar_name}: {e}", exc_info=True)
            result["status"] = "failed"
            result["completed_at"] = time.time()
        
        return result
    
    def _process_batch(self, batch_files: List[Path], progress: Dict[str, Any], 
                      completed_hashes: set, extracted_folder: Path) -> Dict[str, int]:
        """
        Process a batch of files: API check first (fast), then calculate hashes only for new files, check duplicates, upload.
        Returns dict with 'success' and 'errors' counts.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # OPTIMIZATION: Check files via API first (by size only - no hash needed, much faster)
        # This allows us to skip expensive hash calculation for duplicates
        logger.debug(f"[BATCH] Checking {len(batch_files):,} files via API (size-only check, no hash needed)...")
        api_checked_files = []
        files_to_hash = []
        api_skipped = 0
        
        # Prepare file info for batch API check (size-only, no hash)
        file_infos = []
        for file_path in batch_files:
            try:
                file_size = file_path.stat().st_size
                file_infos.append({
                    'file_path': file_path,
                    'file_size': file_size,
                    'file_hash': None  # No hash needed for initial API check
                })
            except Exception as e:
                logger.debug(f"Error getting file size for {file_path.name}: {e}")
                # If we can't get size, we'll need to hash it
                files_to_hash.append(file_path)
        
        # Batch API check (up to 200 files per request, so split if needed)
        if file_infos:
            batch_size = 200  # API limit
            for i in range(0, len(file_infos), batch_size):
                batch_chunk = file_infos[i:i + batch_size]
                try:
                    api_results = self.migrator.check_files_exists_via_api_batch(batch_chunk)
                    for info, api_result in zip(batch_chunk, api_results):
                        file_path = info['file_path']
                        if api_result is True:
                            # File exists in database - skip hash calculation
                            api_skipped += 1
                            # Mark as completed (we'll use a placeholder hash for tracking)
                            # We'll get the real hash later if needed, but for now just mark as duplicate
                            logger.debug(f"File already exists (API check): {file_path.name}")
                        elif api_result is False:
                            # File doesn't exist - need to calculate hash
                            files_to_hash.append(file_path)
                        else:
                            # API check failed (None) - calculate hash to be safe
                            files_to_hash.append(file_path)
                except Exception as e:
                    logger.warning(f"Batch API check failed, will calculate hashes for all files: {e}")
                    # If API check fails, fall back to hashing all files
                    files_to_hash.extend([info['file_path'] for info in batch_chunk])
        
        if api_skipped > 0:
            logger.info(f"[BATCH] API check: {api_skipped:,} files already exist (skipped hash calculation)")
        
        # Only calculate hashes for files that API says don't exist (or API check failed)
        logger.debug(f"[BATCH] Calculating hashes for {len(files_to_hash):,} files (API check passed)...")
        file_hash_map = {}
        if files_to_hash:
            with ThreadPoolExecutor(max_workers=self.parallel_uploads) as hash_executor:
                hash_futures = {hash_executor.submit(self.migrator.get_file_hash, fp): fp 
                              for fp in files_to_hash}
                for future in as_completed(hash_futures):
                    file_path = hash_futures[future]
                    try:
                        file_hash = future.result()
                        file_hash_map[file_path] = file_hash
                    except Exception as e:
                        logger.error(f"Error calculating hash for {file_path.name}: {e}")
                        continue
        
        # Check duplicates against progress files and database cache
        # Note: We don't need to load database hashes if API check already filtered duplicates
        # But we still check progress files for files we hashed
        files_to_upload = []
        skipped_duplicates = api_skipped  # Start with API-skipped count
        
        for file_path, file_hash in file_hash_map.items():
            # Check progress file first (fastest)
            if file_hash in completed_hashes:
                skipped_duplicates += 1
                continue
            
            # Check database cache (only if not already checked via API)
            # For files that passed API check, we know they're not in DB, so skip this
            try:
                file_size = file_path.stat().st_size
                # Only check existing_hashes if we have it loaded (lazy loading)
                # But since API already checked, this is mostly redundant - but keep for safety
                if hasattr(self.migrator, 'existing_hashes') and self.migrator._hashes_loaded:
                    # Thread-safe read
                    with self.migrator.refresh_lock:
                        hash_exists = (file_hash, file_size) in self.migrator.existing_hashes
                    if hash_exists:
                        skipped_duplicates += 1
                        with self.progress_lock:
                            progress["completed_files"][file_hash] = {
                                "file": self.migrator.sanitize_filename(str(file_path)),
                                "status": "already_exists_in_db"
                            }
                        continue
            except Exception as e:
                logger.debug(f"Error checking existing_hashes for {file_path.name}: {e}")
            
            # File is new - add to upload queue
            files_to_upload.append((file_path, file_hash))
        
        if skipped_duplicates > 0:
            logger.info(f"[BATCH] Skipped {skipped_duplicates:,} duplicate files (already uploaded)")
        
        if not files_to_upload:
            logger.info(f"[BATCH] All files in batch are duplicates, skipping upload")
            return {"success": 0, "errors": 0}
        
        # Upload files in parallel
        logger.info(f"[BATCH] Uploading {len(files_to_upload):,} new files (skipped {skipped_duplicates:,} duplicates)")
        success_count = 0
        error_count = 0
        
        with ThreadPoolExecutor(max_workers=self.parallel_uploads) as executor:
            futures = {}
            for file_path, file_hash in files_to_upload:
                future = executor.submit(
                    self.upload_file_from_tar,
                    file_path, file_hash, progress, extracted_folder
                )
                futures[future] = file_path
            
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    upload_result = future.result()
                    if upload_result:
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    logger.error(f"Error uploading {file_path.name}: {e}")
                    error_count += 1
        
        return {"success": success_count, "errors": error_count}
    
    def extract_tar_file(self, tar_path: Path, extract_dir: Path) -> Optional[Path]:
        """Extract tar file to directory, return path to extracted folder"""
        try:
            logger.info(f"Extracting tar file: {tar_path.name}")
            
            # Extract tar file
            with tarfile.open(tar_path, 'r') as tar:
                # Get first directory name (usually the root folder in tar)
                members = tar.getmembers()
                if not members:
                    logger.error(f"Tar file {tar_path.name} is empty")
                    return None
                
                # Extract all files
                tar.extractall(path=extract_dir)
                
                # Find the extracted folder (first directory in tar)
                extracted_folder = None
                for member in members:
                    if member.isdir() and '/' not in member.name.rstrip('/'):
                        # This is a root-level directory
                        extracted_folder = extract_dir / member.name.rstrip('/')
                        break
                
                if extracted_folder and extracted_folder.exists():
                    logger.info(f"Extracted to: {extracted_folder}")
                    return extracted_folder
                else:
                    # Fallback: use extract_dir itself if no subdirectory
                    logger.warning(f"No subdirectory found in tar, using extract_dir: {extract_dir}")
                    return extract_dir
        
        except Exception as e:
            logger.error(f"Error extracting tar file {tar_path.name}: {e}")
            return None
    
    def find_ebook_files_in_directory(self, directory: Path) -> List[Path]:
        """Find ebook files in directory, including those without extensions"""
        files = []
        ebook_extensions = ['.mobi', '.epub', '.fb2', '.pdf', '.azw3', '.txt']
        
        logger.info(f"Scanning directory for ebook files: {directory}")
        
        for item in directory.rglob('*'):
            if not item.is_file():
                continue
            
            # Check if file has extension
            if item.suffix.lower() in ebook_extensions:
                files.append(item)
            else:
                # No extension - detect type
                file_type = self.detect_file_type(item)
                if file_type in ebook_extensions:
                    files.append(item)
                    logger.debug(f"Detected {file_type} file without extension: {item.name}")
        
        logger.info(f"Found {len(files)} ebook files in directory")
        return files
    
    def find_and_process_ebook_files_incremental(self, directory: Path, progress: Dict[str, Any], 
                                                 completed_hashes: set, extracted_folder: Path) -> Iterator[Path]:
        """
        Generator that finds ebook files incrementally and yields them for immediate processing.
        This allows uploads to start while scanning is still in progress.
        """
        ebook_extensions = ['.mobi', '.epub', '.fb2', '.pdf', '.azw3', '.txt']
        files_found = 0
        files_yielded = 0
        
        logger.info(f"Scanning directory for ebook files (incremental mode): {directory}")
        
        for item in directory.rglob('*'):
            if not item.is_file():
                continue
            
            files_found += 1
            is_ebook = False
            file_type = None
            
            # Check if file has extension
            if item.suffix.lower() in ebook_extensions:
                is_ebook = True
                file_type = item.suffix.lower()
            else:
                # No extension - detect type
                file_type = self.detect_file_type(item)
                if file_type in ebook_extensions:
                    is_ebook = True
                    logger.debug(f"Detected {file_type} file without extension: {item.name}")
            
            if is_ebook:
                # For files with extensions: defer hash calculation to batch processing
                # This is much faster - we can parallelize hash calculations later
                # For files without extensions: we already detected type, so just yield
                files_yielded += 1
                # Log progress every 1000 files
                if files_yielded % 1000 == 0:
                    logger.info(f"[SCAN PROGRESS] Found {files_yielded:,} ebook files so far (scanned {files_found:,} total files)")
                yield item
        
        logger.info(f"[SCAN COMPLETE] Found {files_yielded:,} new ebook files out of {files_found:,} total files scanned")
    
    def prepare_file_for_upload_no_conversion(self, file_path: Path) -> Tuple[Path, bool, Dict[str, Any]]:
        """Prepare file for upload WITHOUT conversion. Returns (file_path, is_temp, metadata)"""
        # Detect file type if no extension
        detected_type = None
        if not file_path.suffix:
            detected_type = self.detect_file_type(file_path)
            if detected_type:
                logger.debug(f"Detected type {detected_type} for file without extension: {file_path.name}")
        
        # Extract metadata from original file
        metadata = self.migrator.extract_metadata_from_file(file_path)
        
        # Fix language code
        if metadata.get('language') == 'rus':
            metadata['language'] = 'ru'
        if not metadata.get('language'):
            metadata['language'] = 'ru'
        
        # Return original file (no conversion, no temp files)
        return file_path, False, metadata
    
    def process_tar_file(self, tar_path: Path) -> Dict[str, Any]:
        """Process a single tar file: extract, find files, upload"""
        result = {
            "tar_file": tar_path.name,
            "status": "failed",
            "files_processed": 0,
            "files_uploaded": 0,
            "errors": 0,
            "started_at": time.time(),
            "completed_at": None
        }
        
        # Check for existing extraction folder first (to reuse if available)
        tar_name = tar_path.stem
        extracted_folder = self.find_existing_extraction_folder(tar_name)
        is_reused_extraction = False
        
        if extracted_folder:
            logger.info(f"Using existing extraction folder for {tar_path.name}: {extracted_folder}")
            extract_dir = extracted_folder.parent  # Keep reference for cleanup
            is_reused_extraction = True  # Don't delete this folder - it may be used by other workers
        else:
            # No existing folder found - extract tar file
            timestamp = int(time.time())
            extract_dir = self.temp_extraction_base / f"{tar_name}_{timestamp}"
            extract_dir.mkdir(exist_ok=True)
            
            try:
                extracted_folder = self.extract_tar_file(tar_path, extract_dir)
                if not extracted_folder or not extracted_folder.exists():
                    logger.error(f"Failed to extract tar file: {tar_path.name}")
                    result["status"] = "extraction_failed"
                    return result
            except Exception as e:
                logger.error(f"Error extracting tar file {tar_path.name}: {e}")
                result["status"] = "extraction_failed"
                return result
        
        try:
            
            # Load progress first
            progress = self.load_progress()
            completed_hashes = set(progress.get("completed_files", {}).keys())
            
            # Also load all workers' progress to avoid processing files already done by other workers
            # This is especially important when reusing existing extraction folders
            all_workers_completed = self.load_all_workers_progress()
            completed_hashes.update(all_workers_completed)
            if all_workers_completed:
                logger.info(f"Loaded {len(all_workers_completed)} completed files from all workers' progress files")
            
            # Process files incrementally as they're found (upload immediately after detection)
            total_success = 0
            total_errors = 0
            batch_num = 0
            batch_files = []
            total_files_found = 0
            
            # Use incremental file finder that processes files as they're discovered
            for file_path in self.find_and_process_ebook_files_incremental(extracted_folder, progress, completed_hashes, extracted_folder):
                total_files_found += 1
                batch_files.append(file_path)
                
                # When batch is full, process it immediately
                if len(batch_files) >= self.batch_size:
                    batch_num += 1
                    logger.info(f"[UPLOAD PHASE] Processing batch {batch_num} from {tar_path.name}: {len(batch_files):,} files (found {total_files_found:,} total so far)")
                    
                    # Process batch using optimized _process_batch (API checks first, then hash only for new files)
                    batch_result = self._process_batch(batch_files, progress, completed_hashes, extracted_folder)
                    total_success += batch_result["success"]
                    total_errors += batch_result["errors"]
                    
                    # Save progress after each batch
                    self.save_progress(progress)
                    
                    logger.info(f"[UPLOAD] Batch {batch_num} complete. Success: {total_success:,}, Errors: {total_errors:,}, Total found: {total_files_found:,}")
                    
                    # Clear batch for next iteration
                    batch_files = []
            
            # Process remaining files in final batch (use _process_batch for consistency)
            if batch_files:
                batch_num += 1
                logger.info(f"[UPLOAD PHASE] Processing final batch {batch_num} from {tar_path.name}: {len(batch_files):,} files")
                batch_result = self._process_batch(batch_files, progress, completed_hashes, extracted_folder)
                total_success += batch_result["success"]
                total_errors += batch_result["errors"]
                self.save_progress(progress)
                logger.info(f"[UPLOAD] Final batch {batch_num} complete. Success: {total_success:,}, Errors: {total_errors:,}")
            
            if total_files_found == 0:
                logger.warning(f"No new ebook files found in {tar_path.name} (all may already be processed)")
                result["status"] = "no_new_files"
                return result
            
            result["status"] = "completed"
            result["files_processed"] = total_files_found
            result["files_uploaded"] = total_success
            result["errors"] = total_errors
            result["completed_at"] = time.time()
            
            logger.info(f"Completed processing {tar_path.name}: {total_files_found:,} files found, {total_success:,} uploaded, {total_errors:,} errors")
        
        except Exception as e:
            logger.error(f"Error processing tar file {tar_path.name}: {e}", exc_info=True)
            result["status"] = "error"
            result["error_message"] = str(e)
        
        finally:
            # Clean up extraction directory only if we created it (not reused)
            if not is_reused_extraction and extract_dir.exists():
                try:
                    shutil.rmtree(extract_dir, ignore_errors=True)
                    logger.info(f"Cleaned up extraction directory: {extract_dir}")
                except Exception as e:
                    logger.warning(f"Error cleaning up extraction directory: {e}")
            elif is_reused_extraction:
                logger.info(f"Keeping reused extraction directory: {extract_dir} (may be used by other workers)")
        
        return result
    
    def move_tar_to_processed(self, tar_path: Path) -> bool:
        """Move completed tar file to processed folder"""
        try:
            if not tar_path.exists():
                logger.warning(f"Tar file {tar_path.name} does not exist, cannot move to processed folder")
                return False
            
            processed_path = self.processed_dir / tar_path.name
            
            # Check if file already exists in processed folder
            if processed_path.exists():
                logger.info(f"Tar file {tar_path.name} already exists in processed folder, removing original")
                tar_path.unlink()
                return True
            
            # Move tar file to processed folder
            shutil.move(str(tar_path), str(processed_path))
            logger.info(f"Moved completed tar file {tar_path.name} to processed folder: {processed_path}")
            return True
        except Exception as e:
            logger.error(f"Error moving tar file {tar_path.name} to processed folder: {e}", exc_info=True)
            return False
    
    def upload_file_from_tar(self, file_path: Path, file_hash: str, progress: Dict[str, Any], 
                            extracted_folder: Path) -> bool:
        """Upload a single file from tar extraction (no conversion)"""
        # Prepare file metadata (no conversion)
        upload_path, is_temp, metadata = self.prepare_file_for_upload_no_conversion(file_path)
        
        # Detect file type if no extension (needed for CLI tool to guess mime_type)
        detected_extension = None
        if not file_path.suffix:
            detected_extension = self.detect_file_type(file_path)  # Returns .mobi, .epub, etc.
        
        # Temporarily set calibre_dir to extracted folder for migrator
        # This is needed for some internal path calculations
        original_calibre_dir = self.migrator.calibre_dir
        self.migrator.calibre_dir = extracted_folder
        
        try:
            # Check if already completed
            if file_hash in progress.get("completed_files", {}):
                logger.debug(f"Skipping already uploaded file: {file_path.name}")
                return True
            
            # Check if file exists in database
            try:
                file_size = file_path.stat().st_size
                self.migrator.ensure_hashes_loaded()
                # Thread-safe read
                with self.migrator.refresh_lock:
                    hash_exists = (file_hash, file_size) in self.migrator.existing_hashes
                if hash_exists:
                    logger.debug(f"File already exists in MyBookshelf2 database: {file_path.name}")
                    sanitized_file_path = self.migrator.sanitize_filename(str(file_path))
                    with self.progress_lock:
                        progress["completed_files"][file_hash] = {
                            "file": sanitized_file_path,
                            "status": "already_exists_in_db"
                        }
                    self.save_progress(progress)
                    return True
            except Exception as e:
                logger.debug(f"Error checking existing hashes: {e}")
            
            # Ensure metadata is available
            if not metadata.get('title') or not metadata.get('authors'):
                filename_without_ext = file_path.stem
                if filename_without_ext and not metadata.get('title'):
                    metadata['title'] = filename_without_ext[:200]
                if not metadata.get('title'):
                    logger.error(f"Cannot upload {file_path.name}: no title available")
                    return False
                if not metadata.get('authors'):
                    metadata['authors'] = ['Unknown']
                if not metadata.get('language'):
                    metadata['language'] = 'ru'
            
            # Copy file to container
            container_path = f"/tmp/{upload_path.name}"
            if self.migrator.running_in_container:
                if not Path(container_path).exists():
                    shutil.copy2(str(upload_path), container_path)
            else:
                try:
                    check_cmd = [self.migrator.docker_cmd, 'exec', self.migrator.container, 'test', '-f', container_path]
                    check_result = subprocess.run(check_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
                    if check_result.returncode != 0:
                        copy_cmd = [self.migrator.docker_cmd, 'cp', str(upload_path), f"{self.migrator.container}:{container_path}"]
                        subprocess.run(copy_cmd, check=True, timeout=60)
                except Exception as e:
                    logger.error(f"Failed to copy file to container: {e}")
                    return False
            
            # Build upload command
            if self.migrator.running_in_container:
                upload_cmd = [
                    'python3', 'cli/mbs2.py',
                    '-u', self.migrator.username,
                    '-p', self.migrator.password,
                    '--ws-url', 'ws://mybookshelf2_backend:8080/ws',
                    '--api-url', 'http://localhost:6006',
                    'upload',
                    '--file', container_path
                ]
            else:
                upload_cmd = [
                    self.migrator.docker_cmd, 'exec', self.migrator.container,
                    'python3', 'cli/mbs2.py',
                    '-u', self.migrator.username,
                    '-p', self.migrator.password,
                    '--ws-url', 'ws://mybookshelf2_backend:8080/ws',
                    '--api-url', self.migrator.api_url,
                    'upload',
                    '--file', container_path
                ]
            
            # Add --file-name with detected extension if file has no extension
            # This helps the CLI tool guess the correct mime_type
            # Note: --file-name should be just the filename (not full path) for guess_type to work
            if detected_extension:
                alt_filename = f"{file_path.name}{detected_extension}"
                upload_cmd.extend(['--file-name', alt_filename])
                logger.info(f"Using alternative filename with extension for extensionless file: {file_path.name} -> {alt_filename}")
            
            # Add metadata flags
            if metadata.get('title'):
                sanitized_title = self.migrator.sanitize_metadata_string(metadata['title'])
                upload_cmd.extend(['--title', sanitized_title])
            if metadata.get('authors'):
                for author in metadata['authors'][:20]:
                    sanitized_author = self.migrator.sanitize_metadata_string(author)
                    upload_cmd.extend(['--author', sanitized_author])
            if metadata.get('language'):
                sanitized_language = self.migrator.sanitize_metadata_string(metadata['language'])
                upload_cmd.extend(['--language', sanitized_language])
            if metadata.get('series'):
                sanitized_series = self.migrator.sanitize_metadata_string(metadata['series'])
                upload_cmd.extend(['--series', sanitized_series])
                if metadata.get('series_index') is not None:
                    upload_cmd.extend(['--series-index', str(metadata['series_index'])])
            
            # Upload with retry logic
            for attempt in range(self.migrator.max_retries):
                try:
                    result = subprocess.run(
                        upload_cmd,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        text=True,
                        timeout=600
                    )
                    
                    if result.returncode == 0:
                        # Success - log in format auto-monitor expects
                        logger.info(f"Successfully uploaded: {file_path.name}")
                        sanitized_file_path = self.migrator.sanitize_filename(str(file_path))
                        with self.progress_lock:
                            progress["completed_files"][file_hash] = {
                                "file": sanitized_file_path,
                                "uploaded_at": str(file_path.stat().st_mtime)
                            }
                        self.save_progress(progress)
                        
                        # Clean up copied file
                        try:
                            if self.migrator.running_in_container:
                                if Path(container_path).exists():
                                    Path(container_path).unlink()
                            else:
                                subprocess.run(
                                    [self.migrator.docker_cmd, 'exec', self.migrator.container, 'rm', '-f', container_path],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=10
                                )
                        except:
                            pass
                        
                        return True
                    else:
                        error_output = (result.stderr or "") + (result.stdout or "")
                        if result.returncode == 11 or "already exists" in error_output.lower():
                            # File already exists - treat as success
                            sanitized_file_path = self.migrator.sanitize_filename(str(file_path))
                            with self.progress_lock:
                                progress["completed_files"][file_hash] = {
                                    "file": sanitized_file_path,
                                    "status": "already_exists"
                                }
                            self.save_progress(progress)
                            return True
                        elif attempt < self.migrator.max_retries - 1:
                            delay = self.migrator.retry_delays[min(attempt, len(self.migrator.retry_delays) - 1)]
                            logger.warning(f"Upload failed for {file_path.name} (attempt {attempt + 1}/{self.migrator.max_retries}), retrying in {delay}s...")
                            time.sleep(delay)
                            continue
                        else:
                            # Log full error output for debugging
                            logger.error(f"Upload failed for {file_path.name} (return code: {result.returncode})")
                            logger.error(f"STDOUT: {result.stdout[:1000] if result.stdout else '(empty)'}")
                            logger.error(f"STDERR: {result.stderr[:1000] if result.stderr else '(empty)'}")
                            logger.error(f"Full error output: {error_output[:1000]}")
                            return False
                
                except subprocess.TimeoutExpired:
                    if attempt < self.migrator.max_retries - 1:
                        delay = self.migrator.retry_delays[min(attempt, len(self.migrator.retry_delays) - 1)]
                        logger.warning(f"Upload timeout for {file_path.name} (attempt {attempt + 1}/{self.migrator.max_retries}), retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"Upload timeout for {file_path.name} after {self.migrator.max_retries} attempts")
                        return False
                except Exception as e:
                    logger.error(f"Error uploading {file_path.name}: {e}")
                    import traceback
                    logger.error(f"Full traceback: {''.join(traceback.format_exc())}")
                    return False
            
            return False
        
        finally:
            self.migrator.calibre_dir = original_calibre_dir
    
    def load_all_workers_progress(self) -> set:
        """Load completed files from all workers' progress files to avoid cross-worker duplicates"""
        all_completed_hashes = set()
        progress_files = glob.glob("migration_progress_worker*.json")
        
        for file_path in progress_files:
            try:
                with open(file_path, 'r') as f:
                    content = f.read().strip()
                    if not content:
                        continue
                    
                    # Handle multiple JSON objects
                    if content.count('{') > 1:
                        last_brace = content.rfind('}')
                        if last_brace > 0:
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
                    if isinstance(progress, dict):
                        completed_files = progress.get("completed_files", {})
                        all_completed_hashes.update(completed_files.keys())
            except Exception as e:
                logger.debug(f"Error loading progress file {file_path}: {e}")
                continue
        
        return all_completed_hashes
    
    def load_all_workers_completed_files(self) -> set:
        """Alias for load_all_workers_progress for backward compatibility"""
        return self.load_all_workers_progress()
    
    def get_running_worker_ids(self) -> set:
        """
        Get IDs of workers that are actually running.
        Returns set of worker IDs that have active processes.
        """
        running_workers = set()
        try:
            result = subprocess.run(
                ['pgrep', '-af', 'bulk_migrate_calibre|upload_tar_files'],
                capture_output=True,
                text=True,
                timeout=5
            )
            for line in result.stdout.split('\n'):
                if '--worker-id' in line:
                    import re
                    match = re.search(r'--worker-id\s+(\d+)', line)
                    if match:
                        try:
                            worker_id = int(match.group(1))
                            running_workers.add(worker_id)
                        except ValueError:
                            pass
        except Exception:
            # Fallback to ps aux if pgrep fails
            try:
                result = subprocess.run(
                    ['ps', 'aux'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                for line in result.stdout.split('\n'):
                    if ('bulk_migrate_calibre' in line or 'upload_tar_files' in line) and '--worker-id' in line:
                        parts = line.split()
                        for i, part in enumerate(parts):
                            if part == '--worker-id' and i + 1 < len(parts):
                                try:
                                    worker_id = int(parts[i + 1])
                                    running_workers.add(worker_id)
                                except ValueError:
                                    pass
            except Exception:
                pass
        return running_workers
    
    def get_all_assigned_tar_files(self) -> set:
        """
        Get set of all tar file names assigned to all workers.
        This is used to identify orphaned extraction folders.
        """
        all_assigned_tars = set()
        progress_files = glob.glob("migration_progress_worker*.json")
        
        for file_path in progress_files:
            try:
                with open(file_path, 'r') as f:
                    content = f.read().strip()
                    if not content:
                        continue
                    
                    # Handle multiple JSON objects (same logic as load_all_workers_progress)
                    if content.count('{') > 1:
                        last_brace = content.rfind('}')
                        if last_brace > 0:
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
                    if isinstance(progress, dict):
                        # Get completed tars and current tar
                        completed_tars = progress.get("completed_tars", [])
                        current_tar = progress.get("current_tar")
                        
                        all_assigned_tars.update(completed_tars)
                        if current_tar:
                            all_assigned_tars.add(current_tar)
            except Exception as e:
                logger.debug(f"Error loading progress file {file_path} for assigned tars: {e}")
                continue
        
        return all_assigned_tars
    
    def get_tar_files_from_stopped_workers(self, completed_tars: set) -> List[Tuple[str, int]]:
        """
        Get tar files assigned to stopped workers (workers with progress files but no running process).
        Returns list of (tar_name, worker_id) tuples for uncompleted tar files.
        
        Args:
            completed_tars: Set of tar file names that are already completed (to skip)
        """
        stopped_worker_tars = []
        running_workers = self.get_running_worker_ids()
        progress_files = glob.glob("migration_progress_worker*.json")
        
        for file_path in progress_files:
            try:
                # Extract worker ID from filename
                match = re.search(r'worker(\d+)\.json', file_path)
                if not match:
                    continue
                
                worker_id = int(match.group(1))
                
                # Skip if this worker is currently running
                if worker_id in running_workers:
                    continue
                
                # Skip if this is our own worker (we're processing our own assigned tars separately)
                if self.worker_id and worker_id == self.worker_id:
                    continue
                
                # Load progress file to get assigned tar files
                with open(file_path, 'r') as f:
                    content = f.read().strip()
                    if not content:
                        continue
                    
                    # Handle multiple JSON objects
                    if content.count('{') > 1:
                        last_brace = content.rfind('}')
                        if last_brace > 0:
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
                    if not isinstance(progress, dict):
                        continue
                    
                    # Check if this is a tar upload worker (has tar-specific fields)
                    if "completed_tars" not in progress and "tar_progress" not in progress:
                        continue  # Not a tar upload worker, skip
                    
                    # Get tar files from this stopped worker
                    worker_completed_tars = set(progress.get("completed_tars", []))
                    current_tar = progress.get("current_tar")
                    tar_progress = progress.get("tar_progress", {})
                    
                    # Collect all tar files assigned to this worker
                    assigned_tars = set()
                    
                    # Add completed tars (they were assigned)
                    assigned_tars.update(worker_completed_tars)
                    
                    # Add current tar if it exists
                    if current_tar:
                        assigned_tars.add(current_tar)
                    
                    # Add tars from tar_progress (all tars this worker has touched)
                    assigned_tars.update(tar_progress.keys())
                    
                    # Find uncompleted tar files
                    for tar_name in assigned_tars:
                        if tar_name in completed_tars:
                            continue  # Already completed by another worker
                        
                        # Check if tar file exists in source directory
                        tar_path = self.tar_source_dir / tar_name
                        if not tar_path.exists():
                            continue  # Tar file doesn't exist, skip
                        
                        # Check status in tar_progress
                        tar_status = tar_progress.get(tar_name, {}).get("status", "unknown")
                        if tar_status == "completed":
                            continue  # This worker already completed it
                        
                        stopped_worker_tars.append((tar_name, worker_id))
                        
            except Exception as e:
                logger.debug(f"Error checking stopped worker progress file {file_path}: {e}")
                continue
        
        # Remove duplicates (same tar file from multiple stopped workers)
        seen_tars = set()
        unique_tars = []
        for tar_name, worker_id in stopped_worker_tars:
            if tar_name not in seen_tars:
                seen_tars.add(tar_name)
                unique_tars.append((tar_name, worker_id))
        
        if unique_tars:
            logger.info(f"Found {len(unique_tars)} tar file(s) assigned to stopped workers:")
            for tar_name, worker_id in unique_tars[:5]:  # Log first 5
                logger.info(f"  - {tar_name} (was assigned to stopped Worker {worker_id})")
            if len(unique_tars) > 5:
                logger.info(f"  ... and {len(unique_tars) - 5} more")
        
        return unique_tars
    
    def load_progress(self) -> Dict[str, Any]:
        """Load progress from file"""
        default_progress = {
            "completed_tars": [],
            "current_tar": None,
            "tar_progress": {},
            "completed_files": {}
        }
        
        if not os.path.exists(self.progress_file):
            return default_progress
        
        try:
            with open(self.progress_file, 'r') as f:
                content = f.read()
                if not content or not content.strip():
                    return default_progress
                
                # Handle multiple JSON objects (same as bulk_migrate_calibre.py)
                if content.strip().count('{') > 1:
                    last_brace = content.rfind('}')
                    if last_brace > 0:
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
                if not isinstance(progress, dict):
                    return default_progress
                
                # Ensure required keys exist
                for key in default_progress:
                    if key not in progress:
                        progress[key] = default_progress[key]
                
                return progress
        except Exception as e:
            logger.warning(f"Error loading progress file {self.progress_file}: {e}. Starting fresh.")
            return default_progress
    
    def save_progress(self, progress: Dict[str, Any]):
        """Save progress to file (thread-safe)"""
        with self.progress_lock:
            try:
                progress_file_str = str(self.progress_file)
                progress_dir = Path(progress_file_str).parent
                if progress_dir and not progress_dir.exists():
                    progress_dir.mkdir(parents=True, exist_ok=True)
                
                temp_file_str = progress_file_str[:-5] + '.tmp' if progress_file_str.endswith('.json') else progress_file_str + '.tmp'
                
                try:
                    with open(temp_file_str, 'w') as f:
                        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                        try:
                            json.dump(progress, f, indent=2)
                            f.flush()
                            os.fsync(f.fileno())
                        finally:
                            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                    
                    if os.path.exists(temp_file_str):
                        os.replace(temp_file_str, progress_file_str)
                except OSError as e:
                    logger.warning(f"Atomic write failed ({e}), using direct write")
                    with open(progress_file_str, 'w') as f:
                        json.dump(progress, f, indent=2)
            except Exception as e:
                logger.error(f"Error saving progress file: {e}")
    
    def upload_all_tars(self):
        """Main method to process all tar files"""
        if not self.tar_source_dir.exists():
            logger.error(f"Tar source directory does not exist: {self.tar_source_dir}")
            return
        
        # Get list of tar files to process
        if self.tar_list:
            # Worker mode: process only assigned tar files
            tar_files = []
            for tar_name in self.tar_list:
                tar_path = self.tar_source_dir / tar_name
                if tar_path.exists():
                    tar_files.append(tar_path)
                else:
                    logger.warning(f"Tar file not found: {tar_name}")
        else:
            # Single worker mode: process all tar files
            tar_files = self.find_tar_files()
        
        if not tar_files:
            logger.info("No tar files to process")
            return
        
        logger.info(f"Processing {len(tar_files)} tar file(s)")
        
        # Load progress
        progress = self.load_progress()
        completed_tars = set(progress.get("completed_tars", []))
        current_tar = progress.get("current_tar")
        
        # RESUME LOGIC: If there's a current_tar from previous run, resume from that tar
        # This handles restarts gracefully - worker will continue where it left off
        tar_files_to_process = []
        
        if current_tar:
            logger.info(f"Resuming from previous run: current tar is {current_tar}")
            # Find the current_tar in the list and process it first, then continue with remaining
            found_current = False
            
            for tar_path in tar_files:
                tar_name = tar_path.name
                
                if tar_name == current_tar:
                    # Found the tar we were processing - resume from here
                    found_current = True
                    if tar_name not in completed_tars:
                        tar_files_to_process.append(tar_path)
                        logger.info(f"Resuming processing of {tar_name} (was interrupted, will skip already-uploaded files)")
                elif found_current:
                    # After finding current_tar, add all remaining non-completed tars
                    if tar_name not in completed_tars:
                        tar_files_to_process.append(tar_path)
            
            # If current_tar was not found in the list, process all non-completed tars
            if not found_current:
                logger.warning(f"Current tar {current_tar} not found in assigned tar list, processing all non-completed tars")
                tar_files_to_process = [tp for tp in tar_files if tp.name not in completed_tars]
        else:
            # No current_tar - process all non-completed tars in order
            tar_files_to_process = [tp for tp in tar_files if tp.name not in completed_tars]
        
        if not tar_files_to_process:
            logger.info("No tar files to process (all completed or no files found)")
            return
        
        logger.info(f"Will process {len(tar_files_to_process)} tar file(s)")
        
        # Process each tar file
        for tar_path in tar_files_to_process:
            tar_name = tar_path.name
            
            logger.info(f"=== Processing tar file: {tar_name} ===")
            progress["current_tar"] = tar_name
            self.save_progress(progress)
            
            # Process tar file
            result = self.process_tar_file(tar_path)
            
            # Update progress
            progress["tar_progress"][tar_name] = result
            if result["status"] == "completed":
                progress["completed_tars"].append(tar_name)
                # Move tar file to processed folder
                self.move_tar_to_processed(tar_path)
            progress["current_tar"] = None
            self.save_progress(progress)
            
            logger.info(f"Tar file {tar_name} processing complete: {result['status']}")
        
        logger.info("All assigned tar files processed")
        
        # After processing all assigned tar files, check for orphaned extraction folders
        # Orphaned folders are those that have extracted books but their tar files weren't assigned to any worker
        if self.tar_list:  # Only in worker mode (not single worker mode)
            logger.info("Checking for orphaned extraction folders...")
            
            # Get all assigned tar files from all workers
            all_assigned_tars = self.get_all_assigned_tar_files()
            logger.debug(f"All assigned tar files across workers: {len(all_assigned_tars)}")
            
            # Find orphaned extraction folders
            orphaned_folders = self.find_orphaned_extraction_folders(completed_tars, all_assigned_tars)
            
            if orphaned_folders:
                logger.info(f"Found {len(orphaned_folders)} orphaned extraction folder(s) - processing them now")
                
                # Process orphaned folders
                for extracted_folder, tar_name in orphaned_folders:
                    logger.info(f"=== Processing orphaned extraction folder: {tar_name} ===")
                    progress["current_tar"] = tar_name
                    self.save_progress(progress)
                    
                    # Process orphaned folder
                    result = self.process_orphaned_extraction_folder(extracted_folder, tar_name)
                    
                    # Update progress
                    progress["tar_progress"][tar_name] = result
                    if result["status"] in ["completed", "completed_with_errors"]:
                        progress["completed_tars"].append(tar_name)
                        # Move tar file to processed folder if it exists
                        tar_path = self.tar_source_dir / tar_name
                        if tar_path.exists():
                            self.move_tar_to_processed(tar_path)
                    progress["current_tar"] = None
                    self.save_progress(progress)
                    
                    logger.info(f"Orphaned folder {tar_name} processing complete: {result['status']}")
            else:
                logger.info("No orphaned extraction folders found")
            
            # After processing orphaned extraction folders, check for tar files from stopped workers
            logger.info("Checking for tar files assigned to stopped workers...")
            
            # Get tar files from stopped workers
            stopped_worker_tars = self.get_tar_files_from_stopped_workers(completed_tars)
            
            if stopped_worker_tars:
                logger.info(f"Found {len(stopped_worker_tars)} tar file(s) from stopped workers - processing them now")
                
                # Process tar files from stopped workers
                for tar_name, original_worker_id in stopped_worker_tars:
                    tar_path = self.tar_source_dir / tar_name
                    if not tar_path.exists():
                        logger.warning(f"Tar file {tar_name} from stopped Worker {original_worker_id} not found, skipping")
                        continue
                    
                    logger.info(f"=== Processing tar file from stopped Worker {original_worker_id}: {tar_name} ===")
                    progress["current_tar"] = tar_name
                    self.save_progress(progress)
                    
                    # Process tar file (same logic as regular tar files)
                    result = self.process_tar_file(tar_path)
                    
                    # Update progress
                    progress["tar_progress"][tar_name] = result
                    if result["status"] == "completed":
                        progress["completed_tars"].append(tar_name)
                        # Move tar file to processed folder
                        self.move_tar_to_processed(tar_path)
                    progress["current_tar"] = None
                    self.save_progress(progress)
                    
                    logger.info(f"Tar file {tar_name} (from stopped Worker {original_worker_id}) processing complete: {result['status']}")
            else:
                logger.info("No tar files from stopped workers found")
        
        logger.info("All processing complete (assigned tar files + orphaned folders + stopped worker tar files)")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 upload_tar_files.py <tar_source_directory> [container_name] [username] [password] [--worker-id N] [--tar-list tar1.tar,tar2.tar] [--batch-size N] [--parallel-uploads N] [--temp-dir /path/to/temp]")
        print("Example: python3 upload_tar_files.py /media/haimengzhou/16TB985-CP18TBCD")
        print("         python3 upload_tar_files.py /media/haimengzhou/16TB985-CP18TBCD mybookshelf2_app admin mypassword123")
        print("         python3 upload_tar_files.py /media/haimengzhou/16TB985-CP18TBCD --worker-id 1 --tar-list tar1.tar,tar2.tar")
        print("         python3 upload_tar_files.py /media/haimengzhou/16TB985-CP18TBCD --batch-size 1000 --parallel-uploads 1")
        sys.exit(1)
    
    tar_source_dir = sys.argv[1]
    container = "mybookshelf2_app"
    username = "admin"
    password = "mypassword123"
    worker_id = None
    tar_list = None
    parallel_uploads = 1
    batch_size = 1000
    temp_dir = None
    
    # Parse arguments
    positional_args = []
    i = 2
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--worker-id':
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
        elif arg == '--tar-list':
            if i + 1 < len(sys.argv):
                tar_list = [t.strip() for t in sys.argv[i + 1].split(',')]
                i += 1
            else:
                print("Error: --tar-list requires a comma-separated list")
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
        elif arg == '--temp-dir':
            if i + 1 < len(sys.argv):
                temp_dir = sys.argv[i + 1]
                i += 1
            else:
                print("Error: --temp-dir requires a path")
                sys.exit(1)
        elif not arg.startswith('--'):
            positional_args.append(arg)
        i += 1
    
    # Assign positional arguments
    if len(positional_args) >= 1:
        container = positional_args[0]
    if len(positional_args) >= 2:
        username = positional_args[1]
    if len(positional_args) >= 3:
        password = positional_args[2]
    
    uploader = TarFileUploader(
        tar_source_dir, container, username, password,
        worker_id=worker_id, tar_list=tar_list,
        parallel_uploads=parallel_uploads, batch_size=batch_size,
        temp_dir=temp_dir
    )
    uploader.upload_all_tars()


if __name__ == "__main__":
    main()

