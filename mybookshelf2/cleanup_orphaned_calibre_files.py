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
4. Generates a detailed report categorizing files:
   - No hash match: Files not in mybookshelf2 (orphaned)
   - Hash exists but no path reference: Duplicate files where hash exists but this specific file path isn't referenced

Usage:
    # Dry-run (report only, no deletion)
    python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --container mybookshelf2_app
    
    # Actually delete orphaned files
    python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --container mybookshelf2_app --delete
    
    # With worker ID for progress tracking
    python3 cleanup_orphaned_calibre_files.py /path/to/calibre/library --worker-id 1 --batch-size 1000
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
                 batch_size: int = 1000, limit: Optional[int] = None):
        self.calibre_dir = Path(calibre_dir)
        self.container = container
        self.dry_run = dry_run
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.limit = limit  # Maximum number of files to process
        
        # Progress and report files
        if worker_id is not None:
            self.progress_file = f"calibre_cleanup_progress_worker{worker_id}.json"
            self.report_json = f"calibre_cleanup_report_worker{worker_id}.json"
            self.report_txt = f"calibre_cleanup_report_worker{worker_id}.txt"
        else:
            self.progress_file = "calibre_cleanup_progress.json"
            self.report_json = "calibre_cleanup_report.json"
            self.report_txt = "calibre_cleanup_report.txt"
        
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
            "errors": 0
        }
        
        # Categorized files
        self.files_not_in_calibre = []
        self.files_no_hash_match = []
        self.files_hash_match_no_path = []
        
        # Cache for database queries
        self.calibre_tracked_files = set()
        self.mybookshelf2_hashes = set()
        self.symlink_paths = set()
        
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
    
    def load_symlink_paths(self) -> Set[str]:
        """Get all symlink target paths from MyBookshelf2 container"""
        try:
            # Find all symlinks and get their resolved paths
            # Use readlink -f to get absolute resolved paths
            result = subprocess.run(
                [self.docker_cmd, 'exec', self.container, 'bash', '-c',
                 'find /data/books -type l -exec readlink -f {} \\;'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=60
            )
            if result.returncode == 0:
                paths = set()
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        paths.add(line.strip())
                logger.info(f"Loaded {len(paths):,} symlink target paths from MyBookshelf2")
                return paths
            else:
                logger.warning(f"Error getting symlink paths: {result.stderr}")
                return set()
        except subprocess.TimeoutExpired:
            logger.warning("Timeout getting symlink paths")
            return set()
        except Exception as e:
            logger.warning(f"Error getting symlink paths: {e}")
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
        self.symlink_paths = self.load_symlink_paths()
        
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
            "files_hash_match_no_path": self.files_hash_match_no_path
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
    
    def delete_files(self, file_list: List[str]) -> int:
        """Delete files from the list"""
        deleted_count = 0
        for file_path in file_list:
            try:
                path = Path(file_path)
                if path.exists():
                    path.unlink()
                    deleted_count += 1
                    logger.debug(f"Deleted: {file_path}")
                else:
                    logger.warning(f"File not found (may have been deleted already): {file_path}")
            except Exception as e:
                logger.error(f"Error deleting {file_path}: {e}")
        return deleted_count
    
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
            
            # Delete files with no hash match
            if self.files_no_hash_match:
                logger.info(f"Deleting {len(self.files_no_hash_match):,} files with no hash match...")
                file_paths = [item['path'] for item in self.files_no_hash_match]
                deleted = self.delete_files(file_paths)
                logger.info(f"Deleted {deleted:,} files")
            
            # Delete files with hash match but no path reference
            if self.files_hash_match_no_path:
                logger.info(f"Deleting {len(self.files_hash_match_no_path):,} duplicate files...")
                file_paths = [item['path'] for item in self.files_hash_match_no_path]
                deleted = self.delete_files(file_paths)
                logger.info(f"Deleted {deleted:,} files")
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
    
    args = parser.parse_args()
    
    # Setup logging first (needs worker_id)
    setup_logging(args.worker_id)
    
    # Determine dry-run mode
    dry_run = not args.delete
    
    # Create cleanup instance
    cleanup = CalibreCleanup(
        calibre_dir=args.calibre_dir,
        container=args.container,
        dry_run=dry_run,
        worker_id=args.worker_id,
        batch_size=args.batch_size,
        limit=args.limit
    )
    
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
