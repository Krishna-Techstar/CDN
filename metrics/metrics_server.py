"""
Metrics Server - Central observability hub for the Mini CDN.
Collects per-request metrics reported by edge servers,
aggregates global stats, and exposes a /metrics API for dashboards.
"""

import time
import threading
import requests
from flask import Flask, jsonify, request
from collections import defaultdict, deque

app = Flask(__name__)

# ─── Global Metrics Store (in-memory) ─────────────────────────────────────────
metrics_lock = threading.Lock()

global_stats = {
    "total_requests": 0,
    "cache_hits": 0,
    "cache_misses": 0,
    "total_response_time_ms": 0.0,
    "start_time": time.time(),
}

# Per-edge stats: { edge_id: { hits, misses, requests, total_response_time_ms } }
edge_stats = defaultdict(lambda: {
    "cache_hits": 0,
    "cache_misses": 0,
    "total_requests": 0,
    "total_response_time_ms": 0.0,
    "latency_ms": 0.0,
    "status": "unknown",
})

# Per-file stats: { filename: { hits, misses, requests } }
file_stats = defaultdict(lambda: {"hits": 0, "misses": 0, "requests": 0})

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
    total = global_stats["total_requests"]
    if total == 0:
        return 0.0
    return round(global_stats["cache_hits"] / total, 4)


def get_avg_response_time() -> float:
    total = global_stats["total_requests"]
    if total == 0:
        return 0.0
    return round(global_stats["total_response_time_ms"] / total, 2)


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
        # Update global stats
        global_stats["total_requests"] += 1
        global_stats["total_response_time_ms"] += response_time
        if cache_hit:
            global_stats["cache_hits"] += 1
        else:
            global_stats["cache_misses"] += 1

        # Update per-edge stats
        edge_stats[edge_id]["total_requests"] += 1
        edge_stats[edge_id]["total_response_time_ms"] += response_time
        if cache_hit:
            edge_stats[edge_id]["cache_hits"] += 1
        else:
            edge_stats[edge_id]["cache_misses"] += 1

        # Update per-file stats
        file_stats[filename]["requests"] += 1
        if cache_hit:
            file_stats[filename]["hits"] += 1
        else:
            file_stats[filename]["misses"] += 1

        # Track request timestamp for RPS
        recent_requests.append(time.time())

    return jsonify({"status": "recorded"}), 200


@app.route("/metrics", methods=["GET"])
def metrics():
    """
    Main metrics endpoint — consumed by dashboards and monitoring tools.
    Returns comprehensive system-wide stats.
    """
    uptime = round(time.time() - global_stats["start_time"], 1)

    # Build per-edge summary
    edges_summary = {}
    with metrics_lock:
        for eid, estats in edge_stats.items():
            total = estats["total_requests"]
            edges_summary[eid] = {
                "total_requests": total,
                "cache_hits": estats["cache_hits"],
                "cache_misses": estats["cache_misses"],
                "hit_ratio": round(estats["cache_hits"] / total, 3) if total > 0 else 0,
                "avg_response_time_ms": round(
                    estats["total_response_time_ms"] / total, 2
                ) if total > 0 else 0,
                "latency_ms": estats.get("latency_ms", 0),
                "status": estats.get("status", "unknown"),
            }

        # Top 5 most requested files
        top_files = sorted(
            [{"file": f, **s} for f, s in file_stats.items()],
            key=lambda x: x["requests"],
            reverse=True,
        )[:5]

        return jsonify({
            "system": {
                "uptime_seconds": uptime,
                "requests_per_second": calculate_rps(),
                "total_requests": global_stats["total_requests"],
                "cache_hits": global_stats["cache_hits"],
                "cache_misses": global_stats["cache_misses"],
                "hit_ratio": get_hit_ratio(),
                "avg_response_time_ms": get_avg_response_time(),
            },
            "edges": edges_summary,
            "top_files": top_files,
        })


@app.route("/metrics/edge/<edge_id>", methods=["GET"])
def edge_metrics(edge_id: str):
    """Return metrics for a specific edge server."""
    with metrics_lock:
        if edge_id not in edge_stats:
            return jsonify({"error": f"No data for edge '{edge_id}'"}), 404
        estats = dict(edge_stats[edge_id])

    total = estats["total_requests"]
    return jsonify({
        "edge": edge_id,
        **estats,
        "hit_ratio": round(estats["cache_hits"] / total, 3) if total > 0 else 0,
        "avg_response_time_ms": round(
            estats["total_response_time_ms"] / total, 2
        ) if total > 0 else 0,
    })


@app.route("/metrics/files", methods=["GET"])
def file_metrics():
    """Return per-file request breakdown."""
    with metrics_lock:
        files = [
            {
                "file": fname,
                **fstats,
                "hit_ratio": round(fstats["hits"] / fstats["requests"], 3)
                if fstats["requests"] > 0 else 0,
            }
            for fname, fstats in file_stats.items()
        ]
    return jsonify({"files": sorted(files, key=lambda x: x["requests"], reverse=True)})


@app.route("/metrics/reset", methods=["POST"])
def reset_metrics():
    """Reset all collected metrics (useful for testing)."""
    with metrics_lock:
        global_stats.update({
            "total_requests": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "total_response_time_ms": 0.0,
            "start_time": time.time(),
        })
        edge_stats.clear()
        file_stats.clear()
        recent_requests.clear()
    return jsonify({"status": "metrics reset"})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "server": "metrics"}), 200


if __name__ == "__main__":
    print("[Metrics] Starting metrics server on port 9000...")
    app.run(host="0.0.0.0", port=9000, debug=False)
