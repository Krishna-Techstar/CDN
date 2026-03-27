"""
Edge Server - The core of the CDN.
Caches content locally, serves from cache on hits, fetches from origin on misses.
Each edge server runs as a separate process on a different port.
"""

import argparse
import time
import threading
import requests
from flask import Flask, jsonify, Response
from collections import defaultdict

app = Flask(__name__)

# ─── Configuration (set via CLI args) ─────────────────────────────────────────
EDGE_ID = "edge1"
EDGE_PORT = 8001
ORIGIN_URL = "http://localhost:8000"
METRICS_URL = "http://localhost:9000"

# ─── In-Memory Cache ───────────────────────────────────────────────────────────
# Structure: { filename: { content, content_type, timestamp, ttl } }
cache = {}
cache_lock = threading.Lock()

# ─── Local Stats ───────────────────────────────────────────────────────────────
local_stats = {
    "cache_hits": 0,
    "cache_misses": 0,
    "total_requests": 0,
    "total_response_time_ms": 0,
}
stats_lock = threading.Lock()


def is_cache_valid(entry: dict) -> bool:
    """Check if a cache entry is still within its TTL."""
    return (time.time() - entry["timestamp"]) < entry["ttl"]


def get_from_cache(filename: str):
    """Return cached entry if valid, else None."""
    with cache_lock:
        entry = cache.get(filename)
        if entry and is_cache_valid(entry):
            return entry
        if entry:
            # Expired — evict it
            del cache[filename]
            print(f"[{EDGE_ID}] Cache EXPIRED for '{filename}'")
        return None


def store_in_cache(filename: str, data: dict):
    """Store fetched content in the local cache."""
    with cache_lock:
        cache[filename] = {
            "content": data["content"],
            "content_type": data.get("content_type", "text/plain"),
            "timestamp": time.time(),
            "ttl": data.get("ttl", 60),
            "size": data.get("size", 0),
        }
    print(f"[{EDGE_ID}] Cached '{filename}' (TTL={data.get('ttl', 60)}s)")


def fetch_from_origin(filename: str):
    """Fetch a file from the origin server (cache miss path)."""
    try:
        resp = requests.get(f"{ORIGIN_URL}/fetch/{filename}", timeout=5)
        if resp.status_code == 200:
            return resp.json()
        return None
    except requests.exceptions.RequestException as e:
        print(f"[{EDGE_ID}] ERROR fetching from origin: {e}")
        return None


def report_metrics_to_central(filename: str, cache_hit: bool, response_time_ms: float):
    """Push per-request metrics to the central metrics server."""
    try:
        requests.post(f"{METRICS_URL}/report", json={
            "edge_id": EDGE_ID,
            "filename": filename,
            "cache_hit": cache_hit,
            "response_time_ms": response_time_ms,
        }, timeout=1)
    except Exception:
        pass  # Don't fail if metrics server is down


# ─── Routes ────────────────────────────────────────────────────────────────────

@app.route("/get/<filename>", methods=["GET"])
def get_file(filename: str):
    """Main content serving endpoint."""
    start_time = time.time()

    with stats_lock:
        local_stats["total_requests"] += 1

    # Try cache first
    cached = get_from_cache(filename)
    if cached:
        with stats_lock:
            local_stats["cache_hits"] += 1
        elapsed = (time.time() - start_time) * 1000
        with stats_lock:
            local_stats["total_response_time_ms"] += elapsed

        print(f"[{EDGE_ID}] CACHE HIT  → '{filename}' ({elapsed:.1f}ms)")
        report_metrics_to_central(filename, cache_hit=True, response_time_ms=elapsed)

        return Response(
            cached["content"],
            content_type=cached["content_type"],
            headers={
                "X-Cache": "HIT",
                "X-Edge-Server": EDGE_ID,
                "X-Response-Time": f"{elapsed:.1f}ms",
            }
        )

    # Cache miss — fetch from origin
    with stats_lock:
        local_stats["cache_misses"] += 1

    print(f"[{EDGE_ID}] CACHE MISS → '{filename}' — fetching from origin...")
    origin_data = fetch_from_origin(filename)

    if not origin_data:
        return jsonify({"error": f"File '{filename}' not found"}), 404

    store_in_cache(filename, origin_data)

    elapsed = (time.time() - start_time) * 1000
    with stats_lock:
        local_stats["total_response_time_ms"] += elapsed

    print(f"[{EDGE_ID}] CACHE MISS → '{filename}' served from origin ({elapsed:.1f}ms)")
    report_metrics_to_central(filename, cache_hit=False, response_time_ms=elapsed)

    return Response(
        origin_data["content"],
        content_type=origin_data.get("content_type", "text/plain"),
        headers={
            "X-Cache": "MISS",
            "X-Edge-Server": EDGE_ID,
            "X-Response-Time": f"{elapsed:.1f}ms",
        }
    )


@app.route("/invalidate", methods=["DELETE", "POST"])
def invalidate():
    """Remove a file from the local cache (called by load balancer during invalidation)."""
    from flask import request
    filename = request.args.get("file")
    if not filename:
        return jsonify({"error": "Missing 'file' query param"}), 400

    with cache_lock:
        removed = cache.pop(filename, None)

    if removed:
        print(f"[{EDGE_ID}] INVALIDATED '{filename}' from cache")
        return jsonify({"status": "invalidated", "file": filename, "edge": EDGE_ID})
    return jsonify({"status": "not_cached", "file": filename, "edge": EDGE_ID})


@app.route("/invalidate/all", methods=["DELETE", "POST"])
def invalidate_all():
    """Flush the entire cache."""
    with cache_lock:
        count = len(cache)
        cache.clear()
    print(f"[{EDGE_ID}] Flushed entire cache ({count} entries)")
    return jsonify({"status": "flushed", "cleared": count, "edge": EDGE_ID})


@app.route("/cache/status", methods=["GET"])
def cache_status():
    """Show what's currently in the cache."""
    with cache_lock:
        entries = []
        for fname, entry in cache.items():
            age = time.time() - entry["timestamp"]
            entries.append({
                "filename": fname,
                "age_seconds": round(age, 1),
                "ttl": entry["ttl"],
                "expires_in": round(entry["ttl"] - age, 1),
                "valid": is_cache_valid(entry),
                "size": entry.get("size", 0),
            })
    return jsonify({"edge": EDGE_ID, "cached_files": entries, "count": len(entries)})


@app.route("/stats", methods=["GET"])
def stats():
    """Return local performance stats."""
    with stats_lock:
        total = local_stats["total_requests"]
        hits = local_stats["cache_hits"]
        misses = local_stats["cache_misses"]
        avg_rt = (
            local_stats["total_response_time_ms"] / total if total > 0 else 0
        )
    return jsonify({
        "edge": EDGE_ID,
        "total_requests": total,
        "cache_hits": hits,
        "cache_misses": misses,
        "hit_ratio": round(hits / total, 3) if total > 0 else 0,
        "avg_response_time_ms": round(avg_rt, 2),
        "cached_files_count": len(cache),
    })


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint used by load balancer."""
    return jsonify({
        "status": "healthy",
        "edge": EDGE_ID,
        "cached_files": len(cache),
        "timestamp": time.time(),
    }), 200


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mini CDN Edge Server")
    parser.add_argument("--id", default="edge1", help="Edge server ID")
    parser.add_argument("--port", type=int, default=8001, help="Port to run on")
    parser.add_argument("--origin", default="http://localhost:8000", help="Origin server URL")
    parser.add_argument("--metrics", default="http://localhost:9000", help="Metrics server URL")
    args = parser.parse_args()

    EDGE_ID = args.id
    EDGE_PORT = args.port
    ORIGIN_URL = args.origin
    METRICS_URL = args.metrics

    print(f"[{EDGE_ID}] Starting edge server on port {EDGE_PORT}...")
    print(f"[{EDGE_ID}] Origin: {ORIGIN_URL} | Metrics: {METRICS_URL}")
    app.run(host="0.0.0.0", port=EDGE_PORT, debug=False)
