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
                 delete_existing: bool = False, limit: Optional[int] = None):
        self.calibre_dir = Path(calibre_dir)
        self.container = container
        self.username = username
        self.password = password
        self.progress_file = "migration_progress.json"
        self.error_file = "migration_errors.log"
        self.temp_dir = tempfile.mkdtemp(prefix="mbs2_migration_")
        self.ebook_convert = "/usr/bin/ebook-convert"
        self.ebook_meta = "/usr/bin/ebook-meta"
        self.delete_existing = delete_existing
        self.limit = limit
        
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
        """Calculate SHA256 hash of file for deduplication"""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def load_progress(self) -> Dict[str, Any]:
        """Load migration progress from file"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error loading progress file: {e}")
        return {"completed_files": {}, "errors": []}
    
    def save_progress(self, progress: Dict[str, Any]):
        """Save migration progress to file"""
        try:
            with open(self.progress_file, 'w') as f:
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
        """
        file_ext = file_path.suffix.lower()
        metadata = {}
        is_temp = False
        upload_file = file_path
        
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
        
        # Always copy file to container first (matches quick_migrate_10.sh behavior)
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
            
            # Clean up copied file from container (always)
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
                if "already exists" in error_msg.lower() or "duplicate" in error_msg.lower():
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
    
    def find_ebook_files(self) -> List[Path]:
        """Find all ebook files in Calibre directory - optimized for large directories using system find"""
        ebook_extensions = ['.epub', '.fb2', '.pdf', '.mobi', '.azw3', '.txt']
        
        logger.info("Scanning for ebook files using system find (faster for large libraries)...")
        
        # Use system 'find' command which is much faster than Python's rglob for large directories
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
            # If limit is set, pipe find through head to stop early (much faster!)
            if self.limit and self.limit > 0:
                # Use find | head to stop after finding enough files
                find_process = subprocess.Popen(
                    find_cmd,
                    stdout=subprocess.PIPE,
                    text=True
                )
                head_process = subprocess.Popen(
                    ['head', '-n', str(self.limit)],
                    stdin=find_process.stdout,
                    stdout=subprocess.PIPE,
                    text=True
                )
                find_process.stdout.close()
                stdout, stderr = head_process.communicate(timeout=300)
                returncode = head_process.returncode
            else:
                # Run find command for all files
                result = subprocess.run(
                    find_cmd,
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minute timeout for very large directories
                )
                stdout = result.stdout
                returncode = result.returncode
            
            if returncode != 0:
                logger.warning(f"Find command returned non-zero")
                # Fallback to Python rglob
                return self._find_ebook_files_fallback()
            
            # Parse output
            files = []
            for line in stdout.strip().split('\n'):
                if line.strip():
                    try:
                        file_path = Path(line.strip())
                        if file_path.exists() and file_path.is_file():
                            files.append(file_path)
                    except Exception as e:
                        logger.debug(f"Error parsing file path {line}: {e}")
            
            if self.limit is not None and self.limit > 0:
                files = files[:self.limit]  # Ensure we don't exceed limit
                logger.info(f"Found {len(files)} ebook files (limited to {self.limit})")
            else:
                logger.info(f"Found {len(files)} ebook files")
            
            return files
            
        except subprocess.TimeoutExpired:
            logger.error("File scanning timed out. Directory may be too large.")
            return []
        except Exception as e:
            logger.warning(f"Error using find command: {e}. Falling back to Python rglob.")
            return self._find_ebook_files_fallback()
    
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
        
        # Delete all existing books if requested
        if self.delete_existing:
            self.delete_all_books()
        
        # Load progress
        progress = self.load_progress()
        
        # Find all ebook files
        files = self.find_ebook_files()
        
        total = len(files)
        completed = len(progress.get("completed_files", {}))
        remaining = total - completed
        
        logger.info(f"Total files: {total}, Already completed: {completed}, Remaining: {remaining}")
        
        # Process files
        success_count = 0
        error_count = 0
        
        for i, file_path in enumerate(files, 1):
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{total} files processed")
            
            # Calculate hash for deduplication
            file_hash = self.get_file_hash(file_path)
            
            # Upload file
            if self.upload_file(file_path, file_hash, progress):
                success_count += 1
            else:
                error_count += 1
        
        # Cleanup temp directory
        try:
            shutil.rmtree(self.temp_dir)
        except:
            pass
        
        logger.info(f"Migration complete. Success: {success_count}, Errors: {error_count}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 bulk_migrate_calibre.py <calibre_directory> [container_name] [username] [password] [--delete-existing] [--limit N]")
        print("Example: python3 bulk_migrate_calibre.py /path/to/calibre/library")
        print("         python3 bulk_migrate_calibre.py /path/to/calibre/library mybookshelf2_app admin mypassword123 --delete-existing")
        print("         python3 bulk_migrate_calibre.py /path/to/calibre/library --limit 100")
        sys.exit(1)
    
    calibre_dir = sys.argv[1]
    container = "mybookshelf2_app"
    username = "admin"
    password = "mypassword123"
    delete_existing = False
    limit = None
    
    # Parse arguments
    i = 2
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--delete-existing':
            delete_existing = True
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
        elif not arg.startswith('--'):
            # Positional arguments
            if container == "mybookshelf2_app":
                container = arg
            elif username == "admin":
                username = arg
            elif password == "mypassword123":
                password = arg
        i += 1
    
    migrator = MyBookshelf2Migrator(calibre_dir, container, username, password, delete_existing, limit)
    migrator.migrate()


if __name__ == "__main__":
    main()
