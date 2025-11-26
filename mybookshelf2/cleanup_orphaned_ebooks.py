#!/usr/bin/env python3
"""
Cleanup script to remove orphaned ebook records (ebooks without source files)
from MyBookshelf2 database.

These orphaned records are created when uploads fail after creating ebook metadata
but before creating source file records.

Usage:
    # Dry-run (show what would be deleted, don't actually delete)
    python3 cleanup_orphaned_ebooks.py --dry-run
    
    # Actually delete orphaned records
    python3 cleanup_orphaned_ebooks.py --delete
"""

import sys
import subprocess
import argparse
from pathlib import Path
from datetime import datetime

def get_docker_cmd():
    """Determine docker command"""
    try:
        result = subprocess.run(['docker', 'ps'], capture_output=True, timeout=5)
        return "docker" if result.returncode == 0 else "sudo docker"
    except:
        return "sudo docker"

def get_orphaned_ebooks(container: str = "mybookshelf2_app", dry_run: bool = True):
    """Get list of orphaned ebook IDs (ebooks without source files)"""
    script = """
import sys
sys.path.insert(0, '/code')
from app import app, db
from app import model

with app.app_context():
    # Find ebooks without any source files
    # Use NOT EXISTS to find ebooks that have no sources
    from sqlalchemy import exists
    orphaned = db.session.query(model.Ebook.id, model.Ebook.title).filter(
        ~exists().where(model.Source.ebook_id == model.Ebook.id)
    ).all()
    
    # Return as JSON-like format (escape pipes in titles)
    results = []
    for ebook_id, title in orphaned:
        # Replace pipes in title with placeholder to avoid splitting issues
        safe_title = (title or "Unknown").replace('|', '||')
        results.append(f"{ebook_id}|{safe_title}")
    print('|'.join(results))
    sys.stdout.flush()
"""
    
    try:
        docker_cmd = get_docker_cmd()
        result = subprocess.run(
            [docker_cmd, 'exec', container, 'python3', '-c', script],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            orphaned_list = []
            output = result.stdout.strip()
            if output:
                # Split by '|' but handle escaped pipes (||)
                items = []
                current_item = ""
                i = 0
                while i < len(output):
                    if output[i:i+2] == '||':
                        current_item += '|'
                        i += 2
                    elif output[i] == '|':
                        if current_item:
                            items.append(current_item)
                        current_item = ""
                        i += 1
                    else:
                        current_item += output[i]
                        i += 1
                if current_item:
                    items.append(current_item)
                
                # Process items in pairs (id|title)
                for i in range(0, len(items) - 1, 2):
                    try:
                        ebook_id = int(items[i])
                        title = items[i+1].replace('||', '|') if i+1 < len(items) else "Unknown"
                        orphaned_list.append((ebook_id, title))
                    except (ValueError, IndexError):
                        continue
            return orphaned_list
        else:
            print(f"Error querying database: {result.stderr}", file=sys.stderr)
            return []
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return []

def delete_orphaned_ebooks(ebook_ids: list, container: str = "mybookshelf2_app"):
    """Delete orphaned ebook records from database"""
    if not ebook_ids:
        print("No orphaned ebooks to delete.")
        return True
    
    # Delete in batches to avoid memory issues
    batch_size = 1000
    script_template = """
import sys
sys.path.insert(0, '/code')
from app import app, db
from app import model
from sqlalchemy import text

with app.app_context():
    ebook_ids = [{ids}]
    
    # Delete in correct order to handle foreign key constraints
    # 1. Delete ebook_authors relationships
    db.session.execute(
        text("DELETE FROM ebook_authors WHERE ebook_id = ANY(:ids)"),
        {{"ids": ebook_ids}}
    )
    
    # 2. Delete ebook_genres relationships  
    db.session.execute(
        text("DELETE FROM ebook_genres WHERE ebook_id = ANY(:ids)"),
        {{"ids": ebook_ids}}
    )
    
    # 3. Delete ebook_ratings
    db.session.execute(
        text("DELETE FROM ebook_rating WHERE ebook_id = ANY(:ids)"),
        {{"ids": ebook_ids}}
    )
    
    # 4. Delete bookshelf items
    db.session.execute(
        text("DELETE FROM bookshelf_item WHERE ebook_id = ANY(:ids)"),
        {{"ids": ebook_ids}}
    )
    
    # 5. Finally delete ebooks
    deleted = db.session.execute(
        text("DELETE FROM ebook WHERE id = ANY(:ids)"),
        {{"ids": ebook_ids}}
    ).rowcount
    
    db.session.commit()
    print(f"Deleted {{deleted}} orphaned ebooks")
"""
    
    try:
        docker_cmd = get_docker_cmd()
        total_deleted = 0
        
        for i in range(0, len(ebook_ids), batch_size):
            batch = ebook_ids[i:i+batch_size]
            script = script_template.format(ids=','.join(map(str, batch)))
            
            result = subprocess.run(
                [docker_cmd, 'exec', container, 'python3', '-c', script],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                # Extract deleted count from output
                output = result.stdout.strip()
                if "Deleted" in output:
                    try:
                        deleted = int(output.split("Deleted")[1].split()[0])
                        total_deleted += deleted
                    except:
                        pass
                print(f"Processed batch {i//batch_size + 1}: {len(batch)} ebooks")
            else:
                print(f"Error deleting batch: {result.stderr}", file=sys.stderr)
                return False
        
        return total_deleted
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Cleanup orphaned ebook records (ebooks without source files)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting (default)'
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Actually delete orphaned ebook records'
    )
    parser.add_argument(
        '--container',
        default='mybookshelf2_app',
        help='Docker container name (default: mybookshelf2_app)'
    )
    parser.add_argument(
        '--save-list',
        type=str,
        help='Save list of orphaned ebook IDs to file before deleting'
    )
    
    args = parser.parse_args()
    
    # Default to dry-run if neither flag is specified
    dry_run = not args.delete
    
    print("=" * 80)
    print("  MyBookshelf2 Orphaned Ebook Cleanup")
    print("=" * 80)
    print(f"Mode: {'DRY-RUN (no changes will be made)' if dry_run else 'DELETE (will permanently remove records)'}")
    print(f"Container: {args.container}")
    print()
    
    # Get orphaned ebooks
    print("Scanning database for orphaned ebooks...")
    orphaned = get_orphaned_ebooks(args.container, dry_run)
    
    if not orphaned:
        print("✓ No orphaned ebooks found. Database is clean!")
        return 0
    
    print(f"\nFound {len(orphaned):,} orphaned ebooks (ebooks without source files)")
    print()
    
    # Show sample
    print("Sample orphaned ebooks (first 10):")
    for ebook_id, title in orphaned[:10]:
        print(f"  ID {ebook_id}: {title[:60]}...")
    if len(orphaned) > 10:
        print(f"  ... and {len(orphaned) - 10:,} more")
    print()
    
    # Save list if requested
    if args.save_list:
        with open(args.save_list, 'w') as f:
            f.write(f"# Orphaned ebook IDs - {datetime.now().isoformat()}\n")
            for ebook_id, title in orphaned:
                f.write(f"{ebook_id}\t{title}\n")
        print(f"✓ Saved list to {args.save_list}")
        print()
    
    if dry_run:
        print("=" * 80)
        print("DRY-RUN: No changes made.")
        print(f"To actually delete these {len(orphaned):,} orphaned ebooks, run:")
        print(f"  python3 cleanup_orphaned_ebooks.py --delete")
        if args.save_list:
            print(f"  (List saved to {args.save_list})")
        print("=" * 80)
        return 0
    
    # Confirm deletion
    print("=" * 80)
    print(f"WARNING: This will permanently delete {len(orphaned):,} orphaned ebook records!")
    print("These ebooks have no source files and cannot be accessed.")
    print("=" * 80)
    response = input("Type 'DELETE' to confirm: ")
    
    if response != 'DELETE':
        print("Cancelled. No changes made.")
        return 0
    
    # Delete orphaned ebooks
    print()
    print("Deleting orphaned ebooks...")
    ebook_ids = [ebook_id for ebook_id, _ in orphaned]
    result = delete_orphaned_ebooks(ebook_ids, args.container)
    
    if result:
        print()
        print("=" * 80)
        print(f"✓ Successfully deleted {result:,} orphaned ebook records")
        print("=" * 80)
        
        # Verify
        print("\nVerifying...")
        remaining = get_orphaned_ebooks(args.container, dry_run=True)
        if remaining:
            print(f"⚠️  Warning: {len(remaining):,} orphaned ebooks still remain")
        else:
            print("✓ All orphaned ebooks removed. Database is clean!")
        
        return 0
    else:
        print("✗ Error deleting orphaned ebooks. Check logs above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

