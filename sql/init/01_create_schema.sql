-- Create database schema for Calibre book metadata
-- Optimized for fast searches on large collections (1.5M+ books)

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For fuzzy text search
CREATE EXTENSION IF NOT EXISTS "unaccent"; -- For accent-insensitive search

-- Authors table
CREATE TABLE authors (
    id SERIAL PRIMARY KEY,
    name VARCHAR(512) NOT NULL,
    sort_name VARCHAR(512),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name)
);

-- Series table
CREATE TABLE series (
    id SERIAL PRIMARY KEY,
    name VARCHAR(512) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name)
);

-- Publishers table
CREATE TABLE publishers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(512) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name)
);

-- Tags table
CREATE TABLE tags (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name)
);

-- Languages table
CREATE TABLE languages (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) NOT NULL,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code)
);

-- Main books table
CREATE TABLE books (
    id INTEGER PRIMARY KEY,  -- Keep Calibre's original ID
    calibre_id INTEGER NOT NULL UNIQUE,  -- Explicit Calibre ID reference
    title VARCHAR(1024) NOT NULL,
    sort_title VARCHAR(1024),
    uuid UUID,
    isbn VARCHAR(20),
    lccn VARCHAR(50),
    path VARCHAR(1024),
    
    -- Dates
    timestamp TIMESTAMP,
    pubdate DATE,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Numbers
    series_index DECIMAL(10,2),
    rating INTEGER CHECK (rating >= 0 AND rating <= 10),
    
    -- Text fields
    comments TEXT,
    author_sort VARCHAR(1024),
    
    -- Boolean fields
    has_cover BOOLEAN DEFAULT FALSE,
    
    -- Full-text search column (generated)
    search_vector tsvector,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Book-Author relationship table
CREATE TABLE book_authors (
    book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
    author_id INTEGER REFERENCES authors(id) ON DELETE CASCADE,
    PRIMARY KEY (book_id, author_id)
);

-- Book-Series relationship table
CREATE TABLE book_series (
    book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
    series_id INTEGER REFERENCES series(id) ON DELETE CASCADE,
    series_index DECIMAL(10,2),
    PRIMARY KEY (book_id, series_id)
);

-- Book-Publisher relationship table
CREATE TABLE book_publishers (
    book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
    publisher_id INTEGER REFERENCES publishers(id) ON DELETE CASCADE,
    PRIMARY KEY (book_id, publisher_id)
);

-- Book-Tags relationship table
CREATE TABLE book_tags (
    book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
    tag_id INTEGER REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (book_id, tag_id)
);

-- Book-Languages relationship table
CREATE TABLE book_languages (
    book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
    language_id INTEGER REFERENCES languages(id) ON DELETE CASCADE,
    PRIMARY KEY (book_id, language_id)
);

-- Book formats table
CREATE TABLE book_formats (
    id SERIAL PRIMARY KEY,
    book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
    format VARCHAR(10) NOT NULL,
    filename VARCHAR(512),
    file_size BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Book identifiers table (ISBN, Goodreads, etc.)
CREATE TABLE book_identifiers (
    id SERIAL PRIMARY KEY,
    book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
    identifier_type VARCHAR(50) NOT NULL,
    identifier_value VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(book_id, identifier_type, identifier_value)
);

-- Custom columns table
CREATE TABLE custom_columns (
    id SERIAL PRIMARY KEY,
    book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
    column_name VARCHAR(100) NOT NULL,
    column_value TEXT,
    datatype VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Search history for analytics
CREATE TABLE search_history (
    id SERIAL PRIMARY KEY,
    query TEXT NOT NULL,
    results_count INTEGER,
    execution_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Export/sync tracking
CREATE TABLE sync_status (
    id SERIAL PRIMARY KEY,
    export_timestamp TIMESTAMP NOT NULL,
    books_imported INTEGER,
    calibre_library_path VARCHAR(1024),
    last_calibre_modification TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for fast searches
-- Primary search indexes
CREATE INDEX idx_books_title_gin ON books USING gin(to_tsvector('english', title));
CREATE INDEX idx_books_title_trgm ON books USING gin(title gin_trgm_ops);
CREATE INDEX idx_books_search_vector ON books USING gin(search_vector);

-- Author search indexes
CREATE INDEX idx_authors_name_gin ON authors USING gin(to_tsvector('english', name));
CREATE INDEX idx_authors_name_trgm ON authors USING gin(name gin_trgm_ops);

-- Series search indexes
CREATE INDEX idx_series_name_gin ON series USING gin(to_tsvector('english', name));
CREATE INDEX idx_series_name_trgm ON series USING gin(name gin_trgm_ops);

-- Publisher search indexes
CREATE INDEX idx_publishers_name_gin ON publishers USING gin(to_tsvector('english', name));

-- Tag search indexes
CREATE INDEX idx_tags_name_gin ON tags USING gin(to_tsvector('english', name));
CREATE INDEX idx_tags_name_trgm ON tags USING gin(name gin_trgm_ops);

-- Date/numerical indexes
CREATE INDEX idx_books_pubdate ON books(pubdate);
CREATE INDEX idx_books_timestamp ON books(timestamp);
CREATE INDEX idx_books_rating ON books(rating);
CREATE INDEX idx_books_series_index ON books(series_index);

-- UUID and identifier indexes
CREATE INDEX idx_books_uuid ON books(uuid);
CREATE INDEX idx_books_isbn ON books(isbn);
CREATE INDEX idx_book_identifiers_type_value ON book_identifiers(identifier_type, identifier_value);

-- Relationship indexes
CREATE INDEX idx_book_authors_author_id ON book_authors(author_id);
CREATE INDEX idx_book_series_series_id ON book_series(series_id);
CREATE INDEX idx_book_publishers_publisher_id ON book_publishers(publisher_id);
CREATE INDEX idx_book_tags_tag_id ON book_tags(tag_id);
CREATE INDEX idx_book_languages_language_id ON book_languages(language_id);

-- Format indexes
CREATE INDEX idx_book_formats_book_id ON book_formats(book_id);
CREATE INDEX idx_book_formats_format ON book_formats(format);

-- Comments full-text search
CREATE INDEX idx_books_comments_gin ON books USING gin(to_tsvector('english', coalesce(comments, '')));

-- Function to update search vector
CREATE OR REPLACE FUNCTION update_book_search_vector() RETURNS trigger AS $$
BEGIN
    NEW.search_vector := 
        to_tsvector('english', coalesce(NEW.title, '')) ||
        to_tsvector('english', coalesce(NEW.comments, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update search vector
CREATE TRIGGER update_book_search_vector_trigger
    BEFORE INSERT OR UPDATE ON books
    FOR EACH ROW EXECUTE FUNCTION update_book_search_vector();

-- Function to update timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update updated_at
CREATE TRIGGER update_books_updated_at
    BEFORE UPDATE ON books
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert default languages
INSERT INTO languages (code, name) VALUES 
    ('en', 'English'),
    ('es', 'Spanish'),
    ('fr', 'French'),
    ('de', 'German'),
    ('it', 'Italian'),
    ('pt', 'Portuguese'),
    ('ru', 'Russian'),
    ('ja', 'Japanese'),
    ('zh', 'Chinese'),
    ('ar', 'Arabic')
ON CONFLICT (code) DO NOTHING;

-- Create materialized views for common aggregations
CREATE MATERIALIZED VIEW mv_author_book_counts AS
SELECT 
    a.id,
    a.name,
    COUNT(ba.book_id) as book_count
FROM authors a
LEFT JOIN book_authors ba ON a.id = ba.author_id
GROUP BY a.id, a.name;

CREATE UNIQUE INDEX idx_mv_author_book_counts_id ON mv_author_book_counts(id);

CREATE MATERIALIZED VIEW mv_series_book_counts AS
SELECT 
    s.id,
    s.name,
    COUNT(bs.book_id) as book_count
FROM series s
LEFT JOIN book_series bs ON s.id = bs.series_id
GROUP BY s.id, s.name;

CREATE UNIQUE INDEX idx_mv_series_book_counts_id ON mv_series_book_counts(id);

CREATE MATERIALIZED VIEW mv_tag_book_counts AS
SELECT 
    t.id,
    t.name,
    COUNT(bt.book_id) as book_count
FROM tags t
LEFT JOIN book_tags bt ON t.id = bt.tag_id
GROUP BY t.id, t.name;

CREATE UNIQUE INDEX idx_mv_tag_book_counts_id ON mv_tag_book_counts(id);

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_all_materialized_views() RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_author_book_counts;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_series_book_counts;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_tag_book_counts;
END;
$$ LANGUAGE plpgsql; 