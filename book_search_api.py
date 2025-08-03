#!/usr/bin/env python3

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg2
import psycopg2.extras
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import time
import logging
from contextlib import contextmanager
import uvicorn
import redis
import json
import hashlib
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DATABASE_URL = "postgresql://calibre_user:calibre_pass@localhost:5432/calibre_books"
REDIS_URL = "redis://localhost:6379/0"
CACHE_TTL = 3600  # 1 hour cache

# Initialize FastAPI app
app = FastAPI(
    title="Calibre Fast Search API",
    description="Ultra-fast search API for large Calibre book collections using PostgreSQL",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Redis for caching
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    logger.info("Redis connection established")
except:
    logger.warning("Redis not available, caching disabled")
    redis_client = None

# Database connection management
@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")
    finally:
        if conn:
            conn.close()

# Pydantic models for API responses
class BookSummary(BaseModel):
    id: int
    calibre_id: int
    title: str
    authors: List[str]
    series_name: Optional[str] = None
    series_index: Optional[float] = None
    publisher: Optional[str] = None
    pubdate: Optional[str] = None
    rating: Optional[int] = None
    has_cover: bool = False
    formats: List[str] = []
    tags: List[str] = []

class BookDetail(BookSummary):
    sort_title: Optional[str] = None
    uuid: Optional[str] = None
    isbn: Optional[str] = None
    path: Optional[str] = None
    comments: Optional[str] = None
    languages: List[str] = []
    identifiers: Dict[str, str] = {}
    custom_columns: Dict[str, Any] = {}
    last_modified: Optional[str] = None

class SearchResult(BaseModel):
    books: List[BookSummary]
    total_count: int
    page: int
    page_size: int
    search_time_ms: float
    from_cache: bool = False

class SearchStats(BaseModel):
    total_books: int
    total_authors: int
    total_series: int
    total_publishers: int
    total_tags: int
    recent_searches: List[Dict[str, Any]]

class FacetCounts(BaseModel):
    authors: List[Dict[str, Any]]
    series: List[Dict[str, Any]]
    publishers: List[Dict[str, Any]]
    tags: List[Dict[str, Any]]
    languages: List[Dict[str, Any]]
    formats: List[Dict[str, Any]]

# Cache helper functions
def generate_cache_key(endpoint: str, **params) -> str:
    """Generate cache key from endpoint and parameters"""
    param_str = json.dumps(params, sort_keys=True)
    return f"{endpoint}:{hashlib.md5(param_str.encode()).hexdigest()}"

def get_from_cache(cache_key: str) -> Optional[Dict]:
    """Get data from Redis cache"""
    if not redis_client:
        return None
    try:
        cached = redis_client.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception as e:
        logger.warning(f"Cache get error: {e}")
    return None

def set_cache(cache_key: str, data: Dict, ttl: int = CACHE_TTL):
    """Set data in Redis cache"""
    if not redis_client:
        return
    try:
        redis_client.setex(cache_key, ttl, json.dumps(data, default=str))
    except Exception as e:
        logger.warning(f"Cache set error: {e}")

# Search functions
def build_search_query(query: str, filters: Dict[str, Any]) -> tuple:
    """Build optimized PostgreSQL search query with filters"""
    
    base_query = """
    SELECT DISTINCT
        b.id,
        b.calibre_id,
        b.title,
        b.sort_title,
        b.uuid,
        b.isbn,
        b.path,
        b.pubdate,
        b.rating,
        b.comments,
        b.has_cover,
        b.last_modified,
        ARRAY_AGG(DISTINCT a.name) as authors,
        s.name as series_name,
        b.series_index,
        p.name as publisher,
        ARRAY_AGG(DISTINCT t.name) FILTER (WHERE t.name IS NOT NULL) as tags,
        ARRAY_AGG(DISTINCT l.code) FILTER (WHERE l.code IS NOT NULL) as languages,
        ARRAY_AGG(DISTINCT bf.format) FILTER (WHERE bf.format IS NOT NULL) as formats
    FROM books b
    LEFT JOIN book_authors ba ON b.id = ba.book_id
    LEFT JOIN authors a ON ba.author_id = a.id
    LEFT JOIN book_series bs ON b.id = bs.book_id
    LEFT JOIN series s ON bs.series_id = s.id
    LEFT JOIN book_publishers bp ON b.id = bp.book_id
    LEFT JOIN publishers p ON bp.publisher_id = p.id
    LEFT JOIN book_tags bt ON b.id = bt.book_id
    LEFT JOIN tags t ON bt.tag_id = t.id
    LEFT JOIN book_languages bl ON b.id = bl.book_id
    LEFT JOIN languages l ON bl.language_id = l.id
    LEFT JOIN book_formats bf ON b.id = bf.book_id
    """
    
    where_conditions = []
    params = []
    
    # Full-text search
    if query:
        where_conditions.append("""
            (b.search_vector @@ plainto_tsquery('english', %s)
             OR b.title ILIKE %s
             OR EXISTS (SELECT 1 FROM book_authors ba2 
                       JOIN authors a2 ON ba2.author_id = a2.id 
                       WHERE ba2.book_id = b.id AND a2.name ILIKE %s)
             OR EXISTS (SELECT 1 FROM book_series bs2 
                       JOIN series s2 ON bs2.series_id = s2.id 
                       WHERE bs2.book_id = b.id AND s2.name ILIKE %s))
        """)
        fuzzy_query = f"%{query}%"
        params.extend([query, fuzzy_query, fuzzy_query, fuzzy_query])
    
    # Author filter
    if filters.get('author'):
        where_conditions.append("""
            EXISTS (SELECT 1 FROM book_authors ba3 
                   JOIN authors a3 ON ba3.author_id = a3.id 
                   WHERE ba3.book_id = b.id AND a3.name ILIKE %s)
        """)
        params.append(f"%{filters['author']}%")
    
    # Series filter
    if filters.get('series'):
        where_conditions.append("""
            EXISTS (SELECT 1 FROM book_series bs3 
                   JOIN series s3 ON bs3.series_id = s3.id 
                   WHERE bs3.book_id = b.id AND s3.name ILIKE %s)
        """)
        params.append(f"%{filters['series']}%")
    
    # Publisher filter
    if filters.get('publisher'):
        where_conditions.append("""
            EXISTS (SELECT 1 FROM book_publishers bp3 
                   JOIN publishers p3 ON bp3.publisher_id = p3.id 
                   WHERE bp3.book_id = b.id AND p3.name ILIKE %s)
        """)
        params.append(f"%{filters['publisher']}%")
    
    # Tag filter
    if filters.get('tag'):
        where_conditions.append("""
            EXISTS (SELECT 1 FROM book_tags bt3 
                   JOIN tags t3 ON bt3.tag_id = t3.id 
                   WHERE bt3.book_id = b.id AND t3.name ILIKE %s)
        """)
        params.append(f"%{filters['tag']}%")
    
    # Language filter
    if filters.get('language'):
        where_conditions.append("""
            EXISTS (SELECT 1 FROM book_languages bl3 
                   JOIN languages l3 ON bl3.language_id = l3.id 
                   WHERE bl3.book_id = b.id AND l3.code = %s)
        """)
        params.append(filters['language'])
    
    # Format filter
    if filters.get('format'):
        where_conditions.append("""
            EXISTS (SELECT 1 FROM book_formats bf3 
                   WHERE bf3.book_id = b.id AND bf3.format ILIKE %s)
        """)
        params.append(filters['format'])
    
    # Rating filter
    if filters.get('min_rating'):
        where_conditions.append("b.rating >= %s")
        params.append(filters['min_rating'])
    
    if filters.get('max_rating'):
        where_conditions.append("b.rating <= %s")
        params.append(filters['max_rating'])
    
    # Date filters
    if filters.get('start_date'):
        where_conditions.append("b.pubdate >= %s")
        params.append(filters['start_date'])
    
    if filters.get('end_date'):
        where_conditions.append("b.pubdate <= %s")
        params.append(filters['end_date'])
    
    # Build complete query
    if where_conditions:
        base_query += " WHERE " + " AND ".join(where_conditions)
    
    base_query += """
    GROUP BY b.id, b.calibre_id, b.title, b.sort_title, b.uuid, b.isbn, 
             b.path, b.pubdate, b.rating, b.comments, b.has_cover, 
             b.last_modified, s.name, b.series_index, p.name
    """
    
    return base_query, params

# API Endpoints

@app.get("/", response_model=Dict[str, str])
async def root():
    """API root endpoint"""
    return {
        "name": "Calibre Fast Search API",
        "version": "1.0.0",
        "description": "Ultra-fast search for large Calibre collections"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            return {"status": "healthy", "database": "connected"}
    except:
        return {"status": "unhealthy", "database": "disconnected"}

@app.get("/stats", response_model=SearchStats)
async def get_stats():
    """Get library statistics"""
    cache_key = generate_cache_key("stats")
    cached = get_from_cache(cache_key)
    if cached:
        return SearchStats(**cached)
    
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get basic counts
        cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM books) as total_books,
                (SELECT COUNT(*) FROM authors) as total_authors,
                (SELECT COUNT(*) FROM series) as total_series,
                (SELECT COUNT(*) FROM publishers) as total_publishers,
                (SELECT COUNT(*) FROM tags) as total_tags
        """)
        counts = cursor.fetchone()
        
        # Get recent searches
        cursor.execute("""
            SELECT query, results_count, execution_time_ms, created_at
            FROM search_history 
            ORDER BY created_at DESC 
            LIMIT 10
        """)
        recent_searches = cursor.fetchall()
        
        stats = SearchStats(
            total_books=counts['total_books'],
            total_authors=counts['total_authors'],
            total_series=counts['total_series'],
            total_publishers=counts['total_publishers'],
            total_tags=counts['total_tags'],
            recent_searches=[dict(row) for row in recent_searches]
        )
        
        # Cache for 5 minutes
        set_cache(cache_key, stats.dict(), 300)
        return stats

@app.get("/search", response_model=SearchResult)
async def search_books(
    q: Optional[str] = Query(None, description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    author: Optional[str] = Query(None, description="Filter by author"),
    series: Optional[str] = Query(None, description="Filter by series"),
    publisher: Optional[str] = Query(None, description="Filter by publisher"),
    tag: Optional[str] = Query(None, description="Filter by tag"),
    language: Optional[str] = Query(None, description="Filter by language"),
    format: Optional[str] = Query(None, description="Filter by format"),
    min_rating: Optional[int] = Query(None, ge=0, le=10, description="Minimum rating"),
    max_rating: Optional[int] = Query(None, ge=0, le=10, description="Maximum rating"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    sort_by: str = Query("relevance", description="Sort by: relevance, title, author, pubdate, rating"),
    sort_order: str = Query("desc", description="Sort order: asc, desc")
):
    """Search books with advanced filtering and sorting"""
    
    start_time = time.time()
    
    # Generate cache key
    filters = {
        'author': author, 'series': series, 'publisher': publisher,
        'tag': tag, 'language': language, 'format': format,
        'min_rating': min_rating, 'max_rating': max_rating,
        'start_date': start_date, 'end_date': end_date
    }
    cache_key = generate_cache_key("search", query=q, page=page, page_size=page_size, 
                                 sort_by=sort_by, sort_order=sort_order, **filters)
    
    # Check cache
    cached = get_from_cache(cache_key)
    if cached:
        cached['from_cache'] = True
        return SearchResult(**cached)
    
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Build query
        base_query, params = build_search_query(q or "", filters)
        
        # Add sorting
        sort_mapping = {
            'title': 'b.title',
            'author': 'MIN(a.name)',
            'pubdate': 'b.pubdate',
            'rating': 'b.rating',
            'relevance': 'ts_rank(b.search_vector, plainto_tsquery(%s))'
        }
        
        sort_column = sort_mapping.get(sort_by, 'b.title')
        if sort_by == 'relevance' and q:
            params.append(q)
        
        order_query = base_query + f" ORDER BY {sort_column} {sort_order.upper()}"
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM ({base_query}) as count_subquery"
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()['count']
        
        # Get paginated results
        offset = (page - 1) * page_size
        paginated_query = order_query + " LIMIT %s OFFSET %s"
        
        if sort_by == 'relevance' and q:
            cursor.execute(paginated_query, params + [page_size, offset])
        else:
            cursor.execute(paginated_query, params + [page_size, offset])
        
        rows = cursor.fetchall()
        
        # Convert to BookSummary objects
        books = []
        for row in rows:
            book = BookSummary(
                id=row['id'],
                calibre_id=row['calibre_id'],
                title=row['title'],
                authors=row['authors'] or [],
                series_name=row['series_name'],
                series_index=row['series_index'],
                publisher=row['publisher'],
                pubdate=row['pubdate'].isoformat() if row['pubdate'] else None,
                rating=row['rating'],
                has_cover=row['has_cover'],
                formats=row['formats'] or [],
                tags=row['tags'] or []
            )
            books.append(book)
        
        search_time = (time.time() - start_time) * 1000
        
        # Log search
        cursor.execute("""
            INSERT INTO search_history (query, results_count, execution_time_ms)
            VALUES (%s, %s, %s)
        """, (q or "", total_count, int(search_time)))
        
        result = SearchResult(
            books=books,
            total_count=total_count,
            page=page,
            page_size=page_size,
            search_time_ms=search_time,
            from_cache=False
        )
        
        # Cache results for 1 hour
        set_cache(cache_key, result.dict(), CACHE_TTL)
        
        return result

@app.get("/book/{book_id}", response_model=BookDetail)
async def get_book_detail(book_id: int):
    """Get detailed information for a specific book"""
    
    cache_key = generate_cache_key("book_detail", book_id=book_id)
    cached = get_from_cache(cache_key)
    if cached:
        return BookDetail(**cached)
    
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get book with all details
        cursor.execute("""
            SELECT 
                b.*,
                ARRAY_AGG(DISTINCT a.name) FILTER (WHERE a.name IS NOT NULL) as authors,
                s.name as series_name,
                p.name as publisher,
                ARRAY_AGG(DISTINCT t.name) FILTER (WHERE t.name IS NOT NULL) as tags,
                ARRAY_AGG(DISTINCT l.code) FILTER (WHERE l.code IS NOT NULL) as languages,
                ARRAY_AGG(DISTINCT bf.format) FILTER (WHERE bf.format IS NOT NULL) as formats
            FROM books b
            LEFT JOIN book_authors ba ON b.id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.id
            LEFT JOIN book_series bs ON b.id = bs.book_id
            LEFT JOIN series s ON bs.series_id = s.id
            LEFT JOIN book_publishers bp ON b.id = bp.book_id
            LEFT JOIN publishers p ON bp.publisher_id = p.id
            LEFT JOIN book_tags bt ON b.id = bt.book_id
            LEFT JOIN tags t ON bt.tag_id = t.id
            LEFT JOIN book_languages bl ON b.id = bl.book_id
            LEFT JOIN languages l ON bl.language_id = l.id
            LEFT JOIN book_formats bf ON b.id = bf.book_id
            WHERE b.id = %s
            GROUP BY b.id, s.name, p.name
        """, (book_id,))
        
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Book not found")
        
        # Get identifiers
        cursor.execute("""
            SELECT identifier_type, identifier_value 
            FROM book_identifiers 
            WHERE book_id = %s
        """, (book_id,))
        identifiers = {row['identifier_type']: row['identifier_value'] 
                      for row in cursor.fetchall()}
        
        # Get custom columns
        cursor.execute("""
            SELECT column_name, column_value, datatype 
            FROM custom_columns 
            WHERE book_id = %s
        """, (book_id,))
        custom_columns = {row['column_name']: {
            'value': row['column_value'],
            'datatype': row['datatype']
        } for row in cursor.fetchall()}
        
        book_detail = BookDetail(
            id=row['id'],
            calibre_id=row['calibre_id'],
            title=row['title'],
            sort_title=row['sort_title'],
            uuid=row['uuid'],
            isbn=row['isbn'],
            path=row['path'],
            authors=row['authors'] or [],
            series_name=row['series_name'],
            series_index=row['series_index'],
            publisher=row['publisher'],
            pubdate=row['pubdate'].isoformat() if row['pubdate'] else None,
            rating=row['rating'],
            comments=row['comments'],
            has_cover=row['has_cover'],
            formats=row['formats'] or [],
            tags=row['tags'] or [],
            languages=row['languages'] or [],
            identifiers=identifiers,
            custom_columns=custom_columns,
            last_modified=row['last_modified'].isoformat() if row['last_modified'] else None
        )
        
        # Cache for 24 hours
        set_cache(cache_key, book_detail.dict(), 86400)
        
        return book_detail

@app.get("/facets", response_model=FacetCounts)
async def get_facets(
    q: Optional[str] = Query(None, description="Search query to filter facets"),
    limit: int = Query(20, ge=1, le=100, description="Limit per facet type")
):
    """Get facet counts for filtering options"""
    
    cache_key = generate_cache_key("facets", query=q, limit=limit)
    cached = get_from_cache(cache_key)
    if cached:
        return FacetCounts(**cached)
    
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Build base filter if query provided
        base_filter = ""
        params = []
        if q:
            base_filter = """
                WHERE book_id IN (
                    SELECT id FROM books 
                    WHERE search_vector @@ plainto_tsquery('english', %s)
                       OR title ILIKE %s
                )
            """
            params = [q, f"%{q}%"]
        
        # Get author facets
        cursor.execute(f"""
            SELECT a.name, COUNT(*) as count
            FROM authors a
            JOIN book_authors ba ON a.id = ba.author_id
            {base_filter}
            GROUP BY a.id, a.name
            ORDER BY count DESC, a.name
            LIMIT %s
        """, params + [limit])
        
        authors_facets = [{'name': row['name'], 'count': row['count']} 
                         for row in cursor.fetchall()]
        
        # Similar queries for other facets...
        # (Abbreviated for space - would include series, publishers, tags, etc.)
        
        facets = FacetCounts(
            authors=authors_facets,
            series=[],  # Would be populated similarly
            publishers=[],
            tags=[],
            languages=[],
            formats=[]
        )
        
        # Cache for 30 minutes
        set_cache(cache_key, facets.dict(), 1800)
        
        return facets

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 