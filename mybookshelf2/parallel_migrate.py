#!/usr/bin/env python3
"""
Parallel migration script for Calibre to MyBookshelf2
Launches multiple worker processes to migrate books in parallel
"""

import sys
import subprocess
import time
import signal
import os
from pathlib import Path
import sqlite3
import argparse
from typing import List, Tuple

def get_total_book_count(calibre_dir: Path) -> int:
    """Get total number of book files from Calibre database"""
    db_path = calibre_dir / "metadata.db"
    if not db_path.exists():
        raise FileNotFoundError(f"Calibre database not found at {db_path}")
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    query = """
        SELECT COUNT(*)
        FROM books b
        JOIN data d ON b.id = d.book
        WHERE d.format IN ('EPUB', 'PDF', 'FB2', 'MOBI', 'AZW3', 'TXT')
    """
    
    cursor.execute(query)
    count = cursor.fetchone()[0]
    conn.close()
    
    return count

def calculate_worker_ranges(total_books: int, num_workers: int) -> List[Tuple[int, int]]:
    """Calculate database offset ranges for each worker"""
    books_per_worker = total_books // num_workers
    remainder = total_books % num_workers
    
    ranges = []
    offset = 0
    
    for i in range(num_workers):
        # Distribute remainder across first few workers
        count = books_per_worker + (1 if i < remainder else 0)
        ranges.append((offset, count))
        offset += count
    
    return ranges

def launch_worker(worker_id: int, calibre_dir: str, offset: int, limit: int, 
                  container: str, username: str, password: str, use_symlinks: bool) -> subprocess.Popen:
    """Launch a single worker process"""
    script_path = Path(__file__).parent / "bulk_migrate_calibre.py"
    
    # Build command with proper argument order
    # bulk_migrate_calibre.py expects: calibre_dir [container] [username] [password] [--options]
    # Put options after positional args to match expected parsing
    cmd = [
        sys.executable,
        str(script_path),
        calibre_dir,
        container,
        username,
        password,
        '--worker-id', str(worker_id),
        '--offset', str(offset),
        '--limit', str(limit)
    ]
    
    if use_symlinks:
        cmd.append('--use-symlinks')
    
    log_file = open(f"migration_worker{worker_id}.log", "w")
    
    process = subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid  # Create new process group
    )
    
    return process

def monitor_workers(workers: List[Tuple[int, subprocess.Popen]], calibre_dir: str):
    """Monitor worker processes and display progress"""
    print("\n=== Migration Progress ===")
    print("Press Ctrl+C to stop all workers\n")
    
    try:
        while True:
            # Check if all workers are done
            all_done = all(proc.poll() is not None for _, proc in workers)
            if all_done:
                break
            
            # Display status
            print(f"\r[{time.strftime('%H:%M:%S')}] ", end="")
            for worker_id, proc in workers:
                status = "Running" if proc.poll() is None else f"Done (exit {proc.returncode})"
                print(f"Worker {worker_id}: {status}  ", end="")
            sys.stdout.flush()
            
            time.sleep(5)
        
        print("\n\nAll workers completed!")
        
    except KeyboardInterrupt:
        print("\n\nStopping all workers...")
        for worker_id, proc in workers:
            if proc.poll() is None:
                print(f"Stopping worker {worker_id}...")
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        print("All workers stopped.")

def main():
    parser = argparse.ArgumentParser(description='Parallel migration of Calibre books to MyBookshelf2')
    parser.add_argument('calibre_dir', help='Path to Calibre library directory')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    parser.add_argument('--use-symlinks', action='store_true', help='Use symlinks instead of copying files')
    parser.add_argument('--container', default='mybookshelf2_app', help='Docker container name')
    parser.add_argument('--username', default='admin', help='MyBookshelf2 username')
    parser.add_argument('--password', default='mypassword123', help='MyBookshelf2 password')
    parser.add_argument('--batch-size', type=int, default=10000, help='Books per batch per worker (default: 10000)')
    
    args = parser.parse_args()
    
    calibre_dir = Path(args.calibre_dir)
    if not calibre_dir.exists():
        print(f"Error: Calibre directory does not exist: {calibre_dir}")
        sys.exit(1)
    
    print(f"=== Parallel Migration Setup ===")
    print(f"Calibre directory: {calibre_dir}")
    print(f"Workers: {args.workers}")
    print(f"Batch size per worker: {args.batch_size:,}")
    print(f"Use symlinks: {args.use_symlinks}")
    print()
    
    # Get total book count
    print("Querying Calibre database...")
    try:
        total_books = get_total_book_count(calibre_dir)
        print(f"Total books found: {total_books:,}")
    except Exception as e:
        print(f"Error querying database: {e}")
        sys.exit(1)
    
    # Calculate worker ranges
    ranges = calculate_worker_ranges(total_books, args.workers)
    
    print(f"\nWorker assignments:")
    for i, (offset, count) in enumerate(ranges, 1):
        print(f"  Worker {i}: offset {offset:,}, count {count:,} books")
    
    # Launch workers
    print(f"\nLaunching {args.workers} workers...")
    workers = []
    
    for worker_id, (offset, count) in enumerate(ranges, 1):
        # Each worker processes in batches of --batch-size (10k)
        # The worker will process its assigned range in batches, continuing until done
        # We pass the batch_size as --limit, and the worker will loop internally
        proc = launch_worker(
            worker_id, str(calibre_dir), offset, args.batch_size,
            args.container, args.username, args.password, args.use_symlinks
        )
        workers.append((worker_id, proc))
        print(f"  Worker {worker_id} started (PID: {proc.pid}) - will process {count:,} books in batches of {args.batch_size:,}")
        time.sleep(1)  # Stagger worker starts
    
    print(f"\nAll workers launched. Monitoring progress...")
    print(f"Logs: migration_worker{{N}}.log")
    print(f"Progress: migration_progress_worker{{N}}.json")
    print()
    
    # Monitor workers
    monitor_workers(workers, str(calibre_dir))
    
    # Summary
    print("\n=== Migration Summary ===")
    for worker_id, proc in workers:
        exit_code = proc.returncode
        if exit_code == 0:
            print(f"Worker {worker_id}: Completed successfully")
        else:
            print(f"Worker {worker_id}: Failed with exit code {exit_code}")
            print(f"  Check migration_worker{worker_id}.log for details")

if __name__ == "__main__":
    main()

