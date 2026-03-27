"""
Metrics Server - Central observability hub for the Mini CDN.
Collects per-request metrics reported by edge servers,
aggregates global stats, and exposes a /metrics API for dashboards.
"""

import time
import threading
import requests
from flask import Flask, jsonify, request
from collections import deque
import sys
import os

# Add parent directory to path to import database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import db

app = Flask(__name__)

# ─── Global Metrics Store (now in database) ───────────────────────────────────
metrics_lock = threading.Lock()

# Rolling window of recent requests for requests-per-second calculation
recent_requests = deque()  # stores timestamps of last 60s of requests
RPS_WINDOW = 60  # seconds

# Known edge servers (for active polling)
KNOWN_EDGES = {
    "edge1": "http://localhost:8001",
    "edge2": "http://localhost:8002",
    "edge3": "http://localhost:8003",
}


# ─── Helpers ───────────────────────────────────────────────────────────────────

def calculate_rps() -> float:
    """Calculate requests per second over the last 60 seconds."""
    now = time.time()
    cutoff = now - RPS_WINDOW
    while recent_requests and recent_requests[0] < cutoff:
        recent_requests.popleft()
    return round(len(recent_requests) / RPS_WINDOW, 3)


def get_hit_ratio() -> float:
    """Get cache hit ratio from database."""
    global_metrics = db.get_global_metrics()
    total = global_metrics.get("total_requests", 0)
    if total == 0:
        return 0.0
    hits = global_metrics.get("cache_hits", 0)
    return round(hits / total, 4)


def get_avg_response_time() -> float:
    """Get average response time from database."""
    global_metrics = db.get_global_metrics()
    total = global_metrics.get("total_requests", 0)
    if total == 0:
        return 0.0
    total_time = global_metrics.get("total_response_time_ms", 0.0)
    return round(total_time / total, 2)


def get_uptime() -> float:
    """Get system uptime from database."""
    global_metrics = db.get_global_metrics()
    start_time = global_metrics.get("start_time", time.time())
    return round(time.time() - start_time, 1)


# ─── Routes ────────────────────────────────────────────────────────────────────

@app.route("/report", methods=["POST"])
def report():
    """
    Edge servers POST per-request data here.
    Called after every request served by an edge.
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    edge_id = data.get("edge_id", "unknown")
    filename = data.get("filename", "unknown")
    cache_hit = data.get("cache_hit", False)
    response_time = data.get("response_time_ms", 0.0)

    with metrics_lock:
        # Update global stats in database
        db.update_global_metrics(
            total_requests=1,
            cache_hits=1 if cache_hit else 0,
            cache_misses=0 if cache_hit else 1,
            total_response_time_ms=response_time
        )

        # Update per-edge stats in database
        db.update_edge_metrics(
            edge_id=edge_id,
            total_requests=1,
            cache_hits=1 if cache_hit else 0,
            cache_misses=0 if cache_hit else 1,
            total_response_time_ms=response_time
        )

        # Update per-file stats in database
        db.update_file_metrics(
            filename=filename,
            requests=1,
            hits=1 if cache_hit else 0,
            misses=0 if cache_hit else 1
        )

        # Log the request
        client_ip = request.remote_addr
        user_agent = request.headers.get('User-Agent')
        db.log_request(edge_id, filename, cache_hit, response_time, client_ip, user_agent)

        # Track request timestamp for RPS
        recent_requests.append(time.time())

    return jsonify({"status": "recorded"}), 200


@app.route("/metrics", methods=["GET"])
def metrics():
    """
    Main metrics endpoint — consumed by dashboards and monitoring tools.
    Returns comprehensive system-wide stats.
    """
    global_metrics = db.get_global_metrics()
    uptime = get_uptime()

    # Build per-edge summary
    edges_data = db.get_edge_metrics()
    edges_summary = {}
    for edge in edges_data:
        total = edge["total_requests"]
        edges_summary[edge["edge_id"]] = {
            "total_requests": total,
            "cache_hits": edge["cache_hits"],
            "cache_misses": edge["cache_misses"],
            "hit_ratio": round(edge["cache_hits"] / total, 3) if total > 0 else 0,
            "avg_response_time_ms": round(
                edge["total_response_time_ms"] / total, 2
            ) if total > 0 else 0,
            "latency_ms": edge["latency_ms"],
            "status": edge["status"],
        }

    # Top files from database
    top_files_data = db.get_file_metrics(limit=5)
    top_files = []
    for file_data in top_files_data:
        total = file_data["requests"]
        top_files.append({
            "file": file_data["filename"],
            "requests": total,
            "hits": file_data["hits"],
            "misses": file_data["misses"],
            "hit_ratio": round(file_data["hits"] / total, 3) if total > 0 else 0,
        })

    return jsonify({
        "system": {
            "uptime_seconds": uptime,
            "requests_per_second": calculate_rps(),
            "total_requests": global_metrics.get("total_requests", 0),
            "cache_hits": global_metrics.get("cache_hits", 0),
            "cache_misses": global_metrics.get("cache_misses", 0),
            "hit_ratio": get_hit_ratio(),
            "avg_response_time_ms": get_avg_response_time(),
        },
        "edges": edges_summary,
        "top_files": top_files,
    })


@app.route("/metrics/edge/<edge_id>", methods=["GET"])
def edge_metrics(edge_id: str):
    """Return metrics for a specific edge server."""
    edge_data = db.get_edge_metrics(edge_id)
    if not edge_data:
        return jsonify({"error": f"No data for edge '{edge_id}'"}), 404

    edge = edge_data[0]
    total = edge["total_requests"]
    return jsonify({
        "edge": edge_id,
        "total_requests": total,
        "cache_hits": edge["cache_hits"],
        "cache_misses": edge["cache_misses"],
        "total_response_time_ms": edge["total_response_time_ms"],
        "latency_ms": edge["latency_ms"],
        "status": edge["status"],
        "hit_ratio": round(edge["cache_hits"] / total, 3) if total > 0 else 0,
        "avg_response_time_ms": round(
            edge["total_response_time_ms"] / total, 2
        ) if total > 0 else 0,
    })


@app.route("/metrics/files", methods=["GET"])
def file_metrics():
    """Return per-file request breakdown."""
    files_data = db.get_file_metrics(limit=100)  # Get all files
    files = []
    for file_data in files_data:
        requests = file_data["requests"]
        files.append({
            "file": file_data["filename"],
            "requests": requests,
            "hits": file_data["hits"],
            "misses": file_data["misses"],
            "hit_ratio": round(file_data["hits"] / requests, 3) if requests > 0 else 0,
        })
    return jsonify({"files": files})


@app.route("/metrics/logs", methods=["GET"])
def request_logs():
    """Return recent request logs."""
    limit = int(request.args.get("limit", 100))
    edge_id = request.args.get("edge")
    filename = request.args.get("file")

    logs = db.get_request_logs(limit=limit, edge_id=edge_id, filename=filename)
    return jsonify({"logs": logs, "count": len(logs)})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "server": "metrics"}), 200


if __name__ == "__main__":
    print("[Metrics] Starting metrics server on port 9000...")
    app.run(host="0.0.0.0", port=9000, debug=False)
