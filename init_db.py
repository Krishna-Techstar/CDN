#!/usr/bin/env python3
"""
Database initialization script for the CDN.
Run this to set up the SQLite database schema.
"""

import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import db

def main():
    print("Initializing CDN database...")
    print("Database file: cdn.db")

    # The database is initialized automatically when the db object is created
    # Let's verify by checking some basic operations
    print("Testing database operations...")

    # Test global metrics
    initial = db.get_global_metrics()
    print(f"Initial global metrics: {initial}")

    # Test edge server registration
    db.register_edge_server("test_edge", "http://localhost:9999")
    edges = db.get_edge_servers()
    print(f"Registered edges: {len(edges)}")

    # Test metrics update
    db.update_global_metrics(total_requests=5, cache_hits=3)
    updated = db.get_global_metrics()
    print(f"Updated global metrics: {updated}")

    print("✅ Database initialization complete!")
    print("You can now run the CDN servers.")

if __name__ == "__main__":
    main()