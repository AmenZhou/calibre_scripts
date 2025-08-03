# Calibre Library Splitting Strategy for Large Collections

## Why Split Libraries?
With 1.5M books, you're hitting performance walls. Splitting into themed libraries dramatically improves:
- Search speed (10-50x faster)
- Import speed (5-20x faster)  
- GUI responsiveness (much snappier)
- Database maintenance speed

## Recommended Split Strategy

### Option A: By Genre/Category
```
├── Fiction_Library/          (~400K books)
├── NonFiction_Library/       (~300K books)
├── Technical_Library/        (~200K books)
├── Academic_Library/         (~300K books)
├── Comics_Magazines/         (~200K books)
└── Audiobooks_Library/       (~100K books)
```

### Option B: By Language/Region
```
├── English_Library/          (~1M books)
├── Spanish_Library/          (~200K books)
├── French_Library/           (~150K books)
├── German_Library/           (~100K books)
└── Other_Languages/          (~50K books)
```

### Option C: By Source/Collection
```
├── Personal_Collection/      (~300K books)
├── Project_Gutenberg/        (~200K books)
├── Academic_Sources/         (~400K books)
├── Modern_Fiction/           (~400K books)
└── Reference_Technical/      (~200K books)
```

## How to Split Your Library

### Method 1: Using Virtual Libraries + Export
1. Create Virtual Libraries in your main library:
   ```
   Preferences → Virtual Libraries
   - Create filters like: "tag:fiction" or "language:English"
   ```

2. Export each Virtual Library:
   ```bash
   # Example for fiction books
   calibredb --library-path="/path/to/main" export --all \
     --filter="tag:fiction" \
     --to-dir="/path/to/new/fiction_library"
   ```

### Method 2: Search + Export Script
Create targeted exports based on metadata:

```bash
#!/bin/bash
MAIN_LIB="/path/to/main/library"
NEW_LIB="/path/to/new/library"

# Export by publisher
calibredb --library-path="$MAIN_LIB" export --all \
  --filter="publisher:\"O'Reilly\"" \
  --to-dir="$NEW_LIB"
```

## Managing Multiple Libraries

### Desktop Shortcuts
Create separate shortcuts for each library:
```bash
# Linux/Mac
calibre --library-path="/path/to/fiction_library"
calibre --library-path="/path/to/technical_library"

# Windows
"C:\Program Files\Calibre2\calibre.exe" --library-path="C:\Fiction"
"C:\Program Files\Calibre2\calibre.exe" --library-path="C:\Technical"
```

### Library Switching
- Use **Switch Libraries** option in Calibre GUI
- Keep a "master index" spreadsheet tracking which library contains what

### Cross-Library Search
- Use CalibreSpy plugin for multi-library searching
- Maintain consistent tagging across libraries
- Consider a "Recently Added" library for new imports

## Performance Expectations After Split

| Library Size | Search Time | Import Speed | GUI Response |
|-------------|-------------|--------------|--------------|
| 1.5M books  | 30-120 sec  | 1-5 books/min| Sluggish     |
| 200K books  | 2-10 sec    | 50-200/min   | Responsive   |
| 100K books  | 1-5 sec     | 100-500/min  | Very Fast    |

## Maintenance Strategy
- Run database maintenance monthly on each library
- Use consistent metadata standards across all libraries
- Regular backups of smaller libraries are much faster
- Consider automated sync scripts between libraries if needed 