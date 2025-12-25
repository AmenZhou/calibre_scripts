#!/usr/bin/env python3
"""
Parallel tar file upload script for MyBookshelf2
Launches multiple worker processes to upload books from tar files in parallel
"""

import sys
import subprocess
import time
import signal
import os
from pathlib import Path
import argparse
from typing import List, Tuple

def get_tar_files(tar_source_dir: Path) -> List[Path]:
    """Get all tar files from source directory"""
    tar_files = []
    for item in tar_source_dir.iterdir():
        if item.is_file() and item.suffix.lower() == '.tar':
            tar_files.append(item)
    return sorted(tar_files)

def distribute_tars_to_workers(tar_files: List[Path], num_workers: int) -> List[List[Path]]:
    """Distribute tar files to workers using round-robin"""
    worker_assignments = [[] for _ in range(num_workers)]
    
    for i, tar_file in enumerate(tar_files):
        worker_index = i % num_workers
        worker_assignments[worker_index].append(tar_file)
    
    return worker_assignments

def launch_tar_worker(worker_id: int, tar_source_dir: str, tar_files: List[str],
                     container: str, username: str, password: str,
                     parallel_uploads: int = 1, batch_size: int = 1000) -> subprocess.Popen:
    """Launch a single tar upload worker process"""
    script_path = Path(__file__).parent / "upload_tar_files.py"
    
    # Build command
    cmd = [
        sys.executable,
        str(script_path),
        tar_source_dir,
        container,
        username,
        password,
        '--worker-id', str(worker_id),
        '--tar-list', ','.join(tar_files),
        '--parallel-uploads', str(parallel_uploads),
        '--batch-size', str(batch_size)
    ]
    
    log_file = open(f"migration_worker{worker_id}.log", "w")
    
    process = subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid  # Create new process group
    )
    
    return process

def monitor_workers(workers: List[Tuple[int, subprocess.Popen]], tar_source_dir: str):
    """Monitor worker processes and display progress"""
    print("\n=== Tar Upload Progress ===")
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
    parser = argparse.ArgumentParser(description='Parallel upload of books from tar files to MyBookshelf2')
    parser.add_argument('tar_source_dir', help='Path to directory containing tar files')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    parser.add_argument('--container', default='mybookshelf2_app', help='Docker container name')
    parser.add_argument('--username', default='admin', help='MyBookshelf2 username')
    parser.add_argument('--password', default='mypassword123', help='MyBookshelf2 password')
    parser.add_argument('--batch-size', type=int, default=1000, help='Files per batch per worker (default: 1000)')
    parser.add_argument('--parallel-uploads', type=int, default=1, help='Concurrent uploads per worker (default: 1, max: 10)')
    
    args = parser.parse_args()
    
    # Validate parallel_uploads
    if args.parallel_uploads < 1 or args.parallel_uploads > 10:
        print("Error: --parallel-uploads must be between 1 and 10")
        sys.exit(1)
    
    tar_source_dir = Path(args.tar_source_dir)
    if not tar_source_dir.exists():
        print(f"Error: Tar source directory does not exist: {tar_source_dir}")
        sys.exit(1)
    
    print(f"=== Parallel Tar Upload Setup ===")
    print(f"Tar source directory: {tar_source_dir}")
    print(f"Workers: {args.workers}")
    print(f"Batch size per worker: {args.batch_size:,}")
    print(f"Parallel uploads per worker: {args.parallel_uploads}")
    print()
    
    # Get all tar files
    print("Scanning for tar files...")
    tar_files = get_tar_files(tar_source_dir)
    if not tar_files:
        print(f"Error: No tar files found in {tar_source_dir}")
        sys.exit(1)
    
    print(f"Found {len(tar_files)} tar file(s)")
    
    # Distribute tar files to workers
    worker_assignments = distribute_tars_to_workers(tar_files, args.workers)
    
    print(f"\nWorker assignments:")
    for i, assignment in enumerate(worker_assignments, 1):
        tar_names = [t.name for t in assignment]
        print(f"  Worker {i}: {len(assignment)} tar file(s) - {', '.join(tar_names[:3])}{'...' if len(tar_names) > 3 else ''}")
    
    # Launch workers
    print(f"\nLaunching {args.workers} workers...")
    workers = []
    
    for worker_id, assignment in enumerate(worker_assignments, 1):
        if not assignment:
            print(f"  Worker {worker_id}: No tar files assigned, skipping")
            continue
        
        tar_names = [t.name for t in assignment]
        proc = launch_tar_worker(
            worker_id, str(tar_source_dir), tar_names,
            args.container, args.username, args.password,
            args.parallel_uploads, args.batch_size
        )
        workers.append((worker_id, proc))
        print(f"  Worker {worker_id} started (PID: {proc.pid}) - processing {len(assignment)} tar file(s)")
        time.sleep(1)  # Stagger worker starts
    
    if not workers:
        print("Error: No workers launched")
        sys.exit(1)
    
    print(f"\nAll workers launched. Monitoring progress...")
    print(f"Logs: migration_worker{{N}}.log")
    print(f"Progress: migration_progress_worker{{N}}.json")
    print()
    
    # Monitor workers
    monitor_workers(workers, str(tar_source_dir))
    
    # Summary
    print("\n=== Upload Summary ===")
    for worker_id, proc in workers:
        exit_code = proc.returncode
        if exit_code == 0:
            print(f"Worker {worker_id}: Completed successfully")
        else:
            print(f"Worker {worker_id}: Failed with exit code {exit_code}")
            print(f"  Check migration_worker{worker_id}.log for details")

if __name__ == "__main__":
    main()

