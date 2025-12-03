#!/usr/bin/env python3
"""
Test script to verify the migration changes work correctly.
Tests sanitization, hash refresh, and other new functionality.
"""

import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from bulk_migrate_calibre import MyBookshelf2Migrator

def test_sanitization():
    """Test filename and metadata sanitization"""
    print("Testing sanitization functions...")
    
    # Create a minimal migrator instance (won't actually connect to anything)
    # We'll test the methods directly
    migrator = MyBookshelf2Migrator.__new__(MyBookshelf2Migrator)
    
    # Test NUL character removal
    test_cases = [
        ("normal_file.txt", "normal_file.txt"),
        ("file\x00with\x00nulls.txt", "filewithnulls.txt"),
        ("normal", "normal"),
        ("\x00\x00\x00", ""),
        ("", ""),
    ]
    
    print("  Testing sanitize_filename():")
    for input_val, expected in test_cases:
        result = migrator.sanitize_filename(input_val)
        status = "✓" if result == expected else "✗"
        print(f"    {status} '{input_val}' -> '{result}' (expected: '{expected}')")
        if result != expected:
            print(f"      ERROR: Expected '{expected}', got '{result}'")
            return False
    
    print("  Testing sanitize_metadata_string():")
    for input_val, expected in test_cases:
        result = migrator.sanitize_metadata_string(input_val)
        status = "✓" if result == expected else "✗"
        print(f"    {status} '{input_val}' -> '{result}' (expected: '{expected}')")
        if result != expected:
            print(f"      ERROR: Expected '{expected}', got '{result}'")
            return False
    
    print("✓ Sanitization tests passed\n")
    return True

def test_hash_methods():
    """Test that hash refresh/update methods exist and can be called"""
    print("Testing hash management methods...")
    
    migrator = MyBookshelf2Migrator.__new__(MyBookshelf2Migrator)
    
    # Initialize required attributes
    migrator.existing_hashes = set()
    migrator.last_hash_refresh = 0
    migrator.files_processed_since_refresh = 0
    
    # Test update_existing_hashes
    print("  Testing update_existing_hashes():")
    migrator.update_existing_hashes("test_hash_123", 456)
    if ("test_hash_123", 456) in migrator.existing_hashes:
        print("    ✓ update_existing_hashes() works correctly")
    else:
        print("    ✗ update_existing_hashes() failed")
        return False
    
    if migrator.files_processed_since_refresh == 1:
        print("    ✓ files_processed_since_refresh counter updated")
    else:
        print(f"    ✗ files_processed_since_refresh counter not updated (got {migrator.files_processed_since_refresh})")
        return False
    
    print("✓ Hash management tests passed\n")
    return True

def main():
    print("=" * 60)
    print("Testing Migration Script Changes")
    print("=" * 60)
    print()
    
    all_passed = True
    
    # Test sanitization
    if not test_sanitization():
        all_passed = False
    
    # Test hash methods
    if not test_hash_methods():
        all_passed = False
    
    print("=" * 60)
    if all_passed:
        print("✓ All tests passed!")
        print("\nNext steps:")
        print("1. Run a small test migration with --limit 10 to verify end-to-end")
        print("2. Monitor logs for:")
        print("   - Hash refresh messages (every 1000 files or 10 min)")
        print("   - Performance metrics (upload rate every 100 files)")
        print("   - Retry attempts on connection errors")
        print("   - Sanitization working (no NUL character errors)")
        return 0
    else:
        print("✗ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())



