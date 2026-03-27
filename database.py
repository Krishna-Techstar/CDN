"""
Database utilities for the CDN system.
Provides SQLite-based persistence for metrics, cache metadata, and system state.
"""

import sqlite3
import os
import time
from typing import Dict, List, Optional, Any
from contextlib import contextmanager

class CDNDatabase:
    """SQLite database manager for the CDN system."""

    def __init__(self, db_path: str = "cdn.db"):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self):
        """Initialize database tables."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Metrics tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS global_metrics (
                    id INTEGER PRIMARY KEY,
                    total_requests INTEGER DEFAULT 0,
                    cache_hits INTEGER DEFAULT 0,
                    cache_misses INTEGER DEFAULT 0,
                    total_response_time_ms REAL DEFAULT 0.0,
                    start_time REAL,
                    created_at REAL DEFAULT (strftime('%s', 'now')),
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS edge_metrics (
                    id INTEGER PRIMARY KEY,
                    edge_id TEXT NOT NULL,
                    cache_hits INTEGER DEFAULT 0,
                    cache_misses INTEGER DEFAULT 0,
                    total_requests INTEGER DEFAULT 0,
                    total_response_time_ms REAL DEFAULT 0.0,
                    latency_ms REAL DEFAULT 0.0,
                    status TEXT DEFAULT 'unknown',
                    created_at REAL DEFAULT (strftime('%s', 'now')),
                    updated_at REAL DEFAULT (strftime('%s', 'now')),
                    UNIQUE(edge_id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS file_metrics (
                    id INTEGER PRIMARY KEY,
                    filename TEXT NOT NULL,
                    requests INTEGER DEFAULT 0,
                    hits INTEGER DEFAULT 0,
                    misses INTEGER DEFAULT 0,
                    created_at REAL DEFAULT (strftime('%s', 'now')),
                    updated_at REAL DEFAULT (strftime('%s', 'now')),
                    UNIQUE(filename)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS request_logs (
                    id INTEGER PRIMARY KEY,
                    timestamp REAL NOT NULL,
                    edge_id TEXT,
                    filename TEXT,
                    cache_hit BOOLEAN,
                    response_time_ms REAL,
                    client_ip TEXT,
                    user_agent TEXT
                )
            ''')

            # Cache metadata table (for edge servers)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cache_metadata (
                    id INTEGER PRIMARY KEY,
                    edge_id TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    content TEXT,
                    content_type TEXT,
                    timestamp REAL NOT NULL,
                    ttl INTEGER NOT NULL,
                    size_bytes INTEGER,
                    created_at REAL DEFAULT (strftime('%s', 'now')),
                    UNIQUE(edge_id, filename)
                )
            ''')

            # Edge server registry (for load balancer)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS edge_servers (
                    id INTEGER PRIMARY KEY,
                    edge_id TEXT NOT NULL UNIQUE,
                    url TEXT NOT NULL,
                    latency_ms REAL DEFAULT 999,
                    healthy BOOLEAN DEFAULT 1,
                    last_check REAL DEFAULT 0,
                    fail_count INTEGER DEFAULT 0,
                    registered_at REAL DEFAULT (strftime('%s', 'now')),
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
            ''')

            # Health check history
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS health_checks (
                    id INTEGER PRIMARY KEY,
                    edge_id TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    latency_ms REAL,
                    healthy BOOLEAN,
                    response_time_ms REAL
                )
            ''')

            # Initialize global metrics if not exists
            cursor.execute('''
                INSERT OR IGNORE INTO global_metrics (id, start_time)
                VALUES (1, ?)
            ''', (time.time(),))

            conn.commit()

    # --- Global Metrics Methods ---
    def get_global_metrics(self) -> Dict[str, Any]:
        """Get current global metrics."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM global_metrics WHERE id = 1')
            row = cursor.fetchone()
            if row:
                return dict(row)
            return {}

    def update_global_metrics(self, total_requests: int = None, cache_hits: int = None,
                            cache_misses: int = None, total_response_time_ms: float = None):
        """Update global metrics."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            updates = []
            params = []

            if total_requests is not None:
                updates.append('total_requests = total_requests + ?')
                params.append(total_requests)

            if cache_hits is not None:
                updates.append('cache_hits = cache_hits + ?')
                params.append(cache_hits)

            if cache_misses is not None:
                updates.append('cache_misses = cache_misses + ?')
                params.append(cache_misses)

            if total_response_time_ms is not None:
                updates.append('total_response_time_ms = total_response_time_ms + ?')
                params.append(total_response_time_ms)

            if updates:
                updates.append('updated_at = strftime(\'%s\', \'now\')')
                query = f'UPDATE global_metrics SET {", ".join(updates)} WHERE id = 1'
                cursor.execute(query, params)
                conn.commit()

    # --- Edge Metrics Methods ---
    def get_edge_metrics(self, edge_id: str = None) -> List[Dict[str, Any]]:
        """Get edge metrics for all edges or specific edge."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if edge_id:
                cursor.execute('SELECT * FROM edge_metrics WHERE edge_id = ?', (edge_id,))
            else:
                cursor.execute('SELECT * FROM edge_metrics ORDER BY edge_id')
            return [dict(row) for row in cursor.fetchall()]

    def update_edge_metrics(self, edge_id: str, cache_hits: int = None, cache_misses: int = None,
                          total_requests: int = None, total_response_time_ms: float = None,
                          latency_ms: float = None, status: str = None):
        """Update or insert edge metrics."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Insert if not exists
            cursor.execute('''
                INSERT OR IGNORE INTO edge_metrics (edge_id, cache_hits, cache_misses,
                                                  total_requests, total_response_time_ms,
                                                  latency_ms, status)
                VALUES (?, 0, 0, 0, 0.0, 0.0, 'unknown')
            ''', (edge_id,))

            # Update values
            updates = []
            params = []

            if cache_hits is not None:
                updates.append('cache_hits = cache_hits + ?')
                params.append(cache_hits)

            if cache_misses is not None:
                updates.append('cache_misses = cache_misses + ?')
                params.append(cache_misses)

            if total_requests is not None:
                updates.append('total_requests = total_requests + ?')
                params.append(total_requests)

            if total_response_time_ms is not None:
                updates.append('total_response_time_ms = total_response_time_ms + ?')
                params.append(total_response_time_ms)

            if latency_ms is not None:
                updates.append('latency_ms = ?')
                params.append(latency_ms)

            if status is not None:
                updates.append('status = ?')
                params.append(status)

            if updates:
                updates.append('updated_at = strftime(\'%s\', \'now\')')
                query = f'UPDATE edge_metrics SET {", ".join(updates)} WHERE edge_id = ?'
                params.append(edge_id)
                cursor.execute(query, params)
                conn.commit()

    # --- File Metrics Methods ---
    def get_file_metrics(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get top file metrics ordered by requests."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM file_metrics
                ORDER BY requests DESC
                LIMIT ?
            ''', (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def update_file_metrics(self, filename: str, requests: int = None, hits: int = None, misses: int = None):
        """Update or insert file metrics."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Insert if not exists
            cursor.execute('''
                INSERT OR IGNORE INTO file_metrics (filename, requests, hits, misses)
                VALUES (?, 0, 0, 0)
            ''', (filename,))

            # Update values
            updates = []
            params = []

            if requests is not None:
                updates.append('requests = requests + ?')
                params.append(requests)

            if hits is not None:
                updates.append('hits = hits + ?')
                params.append(hits)

            if misses is not None:
                updates.append('misses = misses + ?')
                params.append(misses)

            if updates:
                updates.append('updated_at = strftime(\'%s\', \'now\')')
                query = f'UPDATE file_metrics SET {", ".join(updates)} WHERE filename = ?'
                params.append(filename)
                cursor.execute(query, params)
                conn.commit()

    # --- Request Logging ---
    def log_request(self, edge_id: str, filename: str, cache_hit: bool,
                   response_time_ms: float, client_ip: str = None, user_agent: str = None):
        """Log a request."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO request_logs (timestamp, edge_id, filename, cache_hit,
                                        response_time_ms, client_ip, user_agent)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (time.time(), edge_id, filename, cache_hit, response_time_ms, client_ip, user_agent))
            conn.commit()

    def get_request_logs(self, limit: int = 100, edge_id: str = None,
                        filename: str = None) -> List[Dict[str, Any]]:
        """Get recent request logs."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = 'SELECT * FROM request_logs WHERE 1=1'
            params = []

            if edge_id:
                query += ' AND edge_id = ?'
                params.append(edge_id)

            if filename:
                query += ' AND filename = ?'
                params.append(filename)

            query += ' ORDER BY timestamp DESC LIMIT ?'
            params.append(limit)

            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    # --- Cache Metadata Methods ---
    def get_cache_metadata(self, edge_id: str, filename: str = None) -> List[Dict[str, Any]]:
        """Get cache metadata for an edge server."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if filename:
                cursor.execute('''
                    SELECT * FROM cache_metadata
                    WHERE edge_id = ? AND filename = ?
                ''', (edge_id, filename))
            else:
                cursor.execute('''
                    SELECT * FROM cache_metadata
                    WHERE edge_id = ?
                    ORDER BY timestamp DESC
                ''', (edge_id,))
            return [dict(row) for row in cursor.fetchall()]

    def set_cache_metadata(self, edge_id: str, filename: str, content: str,
                          content_type: str, ttl: int, size_bytes: int = None):
        """Set cache metadata and content for a file."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO cache_metadata
                (edge_id, filename, content, content_type, timestamp, ttl, size_bytes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (edge_id, filename, content, content_type, time.time(), ttl, size_bytes))
            conn.commit()

    def delete_cache_metadata(self, edge_id: str, filename: str = None):
        """Delete cache metadata."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if filename:
                cursor.execute('''
                    DELETE FROM cache_metadata
                    WHERE edge_id = ? AND filename = ?
                ''', (edge_id, filename))
            else:
                cursor.execute('''
                    DELETE FROM cache_metadata
                    WHERE edge_id = ?
                ''', (edge_id,))
            conn.commit()

    def cleanup_expired_cache(self, edge_id: str):
        """Remove expired cache entries."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time = time.time()
            cursor.execute('''
                DELETE FROM cache_metadata
                WHERE edge_id = ? AND (timestamp + ttl) < ?
            ''', (edge_id, current_time))
            conn.commit()

    # --- Edge Server Registry Methods ---
    def get_edge_servers(self) -> List[Dict[str, Any]]:
        """Get all registered edge servers."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM edge_servers ORDER BY edge_id')
            return [dict(row) for row in cursor.fetchall()]

    def register_edge_server(self, edge_id: str, url: str):
        """Register or update an edge server."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO edge_servers
                (edge_id, url, updated_at)
                VALUES (?, ?, strftime('%s', 'now'))
            ''', (edge_id, url))
            conn.commit()

    def update_edge_health(self, edge_id: str, latency_ms: float, healthy: bool):
        """Update edge server health status."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            fail_count = 0 if healthy else 1  # Reset or increment
            cursor.execute('''
                UPDATE edge_servers
                SET latency_ms = ?, healthy = ?, last_check = ?, fail_count = ?, updated_at = strftime('%s', 'now')
                WHERE edge_id = ?
            ''', (latency_ms, healthy, time.time(), fail_count, edge_id))
            conn.commit()

    def log_health_check(self, edge_id: str, latency_ms: float, healthy: bool, response_time_ms: float):
        """Log a health check result."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO health_checks (edge_id, timestamp, latency_ms, healthy, response_time_ms)
                VALUES (?, ?, ?, ?, ?)
            ''', (edge_id, time.time(), latency_ms, healthy, response_time_ms))
            conn.commit()

    def reset_metrics(self):
        """Reset all metrics data (for testing/debugging)."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # Reset global metrics but keep start_time
            cursor.execute('''
                UPDATE global_metrics
                SET total_requests = 0, cache_hits = 0, cache_misses = 0,
                    total_response_time_ms = 0.0, updated_at = strftime('%s', 'now')
                WHERE id = 1
            ''')
            # Clear edge metrics
            cursor.execute('DELETE FROM edge_metrics')
            # Clear file metrics
            cursor.execute('DELETE FROM file_metrics')
            # Clear request logs
            cursor.execute('DELETE FROM request_logs')
            conn.commit()

# Global database instance
db = CDNDatabase()