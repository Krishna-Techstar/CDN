"""
Load Balancer - Intelligent request routing layer.
Measures edge server latency, routes to the fastest healthy node,
performs health checks, and handles cache invalidation across all edges.
"""

import time
import threading
import requests
from flask import Flask, jsonify, request, Response

app = Flask(__name__)

# ─── Edge Server Registry ──────────────────────────────────────────────────────
# Each entry: { url, id, latency_ms, healthy, last_check, fail_count }
EDGE_SERVERS = [
    {"id": "edge1", "url": "http://localhost:8001", "latency_ms": 999, "healthy": True, "last_check": 0, "fail_count": 0},
    {"id": "edge2", "url": "http://localhost:8002", "latency_ms": 999, "healthy": True, "last_check": 0, "fail_count": 0},
    {"id": "edge3", "url": "http://localhost:8003", "latency_ms": 999, "healthy": True, "last_check": 0, "fail_count": 0},
]

HEALTH_CHECK_INTERVAL = 10   # seconds between health checks
MAX_FAIL_COUNT = 3           # mark unhealthy after this many consecutive failures
registry_lock = threading.Lock()

# ─── Metrics ───────────────────────────────────────────────────────────────────
lb_stats = {
    "total_requests": 0,
    "routed_requests": 0,
    "failed_routes": 0,
}
stats_lock = threading.Lock()


# ─── Health Check Loop ─────────────────────────────────────────────────────────

def measure_latency(edge: dict) -> float:
    """Ping the edge's /health endpoint and return round-trip time in ms."""
    try:
        start = time.time()
        resp = requests.get(f"{edge['url']}/health", timeout=3)
        elapsed = (time.time() - start) * 1000
        if resp.status_code == 200:
            return elapsed
    except Exception:
        pass
    return float("inf")  # Unreachable


def run_health_checks():
    """Background thread: continuously checks all edge servers."""
    while True:
        with registry_lock:
            for edge in EDGE_SERVERS:
                latency = measure_latency(edge)
                edge["last_check"] = time.time()

                if latency == float("inf"):
                    edge["fail_count"] += 1
                    if edge["fail_count"] >= MAX_FAIL_COUNT:
                        if edge["healthy"]:
                            print(f"[LB] ⚠️  Edge '{edge['id']}' marked UNHEALTHY (failed {edge['fail_count']}x)")
                        edge["healthy"] = False
                else:
                    if not edge["healthy"]:
                        print(f"[LB] ✅  Edge '{edge['id']}' RECOVERED (latency={latency:.1f}ms)")
                    edge["latency_ms"] = round(latency, 2)
                    edge["healthy"] = True
                    edge["fail_count"] = 0

        time.sleep(HEALTH_CHECK_INTERVAL)


def select_best_edge() -> dict | None:
    """
    Latency-based selection: choose the healthy edge with lowest measured latency.
    Falls back to round-robin if all latencies are equal.
    """
    with registry_lock:
        healthy = [e for e in EDGE_SERVERS if e["healthy"]]
    if not healthy:
        return None
    return min(healthy, key=lambda e: e["latency_ms"])


# ─── Routes ────────────────────────────────────────────────────────────────────

@app.route("/get/<filename>", methods=["GET"])
def proxy_request(filename: str):
    """
    Main entry point for client requests.
    Selects best edge and proxies the request.
    """
    with stats_lock:
        lb_stats["total_requests"] += 1

    edge = select_best_edge()
    if not edge:
        with stats_lock:
            lb_stats["failed_routes"] += 1
        return jsonify({"error": "No healthy edge servers available"}), 503

    target_url = f"{edge['url']}/get/{filename}"
    print(f"[LB] Routing '{filename}' → {edge['id']} (latency={edge['latency_ms']}ms)")

    try:
        start = time.time()
        upstream = requests.get(target_url, timeout=10)
        elapsed = (time.time() - start) * 1000

        with stats_lock:
            lb_stats["routed_requests"] += 1

        # Pass through the upstream response to the client
        return Response(
            upstream.content,
            status=upstream.status_code,
            content_type=upstream.headers.get("Content-Type", "text/plain"),
            headers={
                "X-LB-Selected-Edge": edge["id"],
                "X-LB-Edge-Latency": f"{edge['latency_ms']}ms",
                "X-LB-Proxy-Time": f"{elapsed:.1f}ms",
                "X-Cache": upstream.headers.get("X-Cache", "UNKNOWN"),
            }
        )
    except requests.exceptions.RequestException as e:
        # Mark this edge as potentially failing
        with registry_lock:
            edge["fail_count"] += 1
        print(f"[LB] ERROR routing to {edge['id']}: {e}")
        with stats_lock:
            lb_stats["failed_routes"] += 1
        return jsonify({"error": "Edge server error", "detail": str(e)}), 502


@app.route("/invalidate", methods=["POST", "DELETE"])
def invalidate_cache():
    """
    Cache invalidation: broadcast delete to ALL edge servers.
    Forces fresh content to be fetched from origin on next request.
    """
    filename = request.args.get("file")
    if not filename:
        return jsonify({"error": "Missing 'file' query param"}), 400

    results = []
    with registry_lock:
        edges_snapshot = list(EDGE_SERVERS)

    for edge in edges_snapshot:
        try:
            resp = requests.post(
                f"{edge['url']}/invalidate",
                params={"file": filename},
                timeout=3
            )
            results.append({"edge": edge["id"], "status": resp.json()})
        except Exception as e:
            results.append({"edge": edge["id"], "status": "error", "detail": str(e)})

    print(f"[LB] Invalidated '{filename}' across {len(edges_snapshot)} edges")
    return jsonify({"file": filename, "invalidation_results": results})


@app.route("/invalidate/all", methods=["POST", "DELETE"])
def invalidate_all():
    """Flush the entire cache on all edge servers."""
    results = []
    with registry_lock:
        edges_snapshot = list(EDGE_SERVERS)

    for edge in edges_snapshot:
        try:
            resp = requests.post(f"{edge['url']}/invalidate/all", timeout=3)
            results.append({"edge": edge["id"], "status": resp.json()})
        except Exception as e:
            results.append({"edge": edge["id"], "status": "error", "detail": str(e)})

    print(f"[LB] Full cache flush across all edges")
    return jsonify({"status": "flushed_all", "results": results})


@app.route("/edges", methods=["GET"])
def list_edges():
    """Return the current state of all registered edge servers."""
    with registry_lock:
        return jsonify({"edges": EDGE_SERVERS, "count": len(EDGE_SERVERS)})


@app.route("/edges/register", methods=["POST"])
def register_edge():
    """Dynamically register a new edge server at runtime."""
    data = request.get_json()
    if not data or "id" not in data or "url" not in data:
        return jsonify({"error": "Provide 'id' and 'url'"}), 400

    new_edge = {
        "id": data["id"],
        "url": data["url"],
        "latency_ms": 999,
        "healthy": True,
        "last_check": 0,
        "fail_count": 0,
    }
    with registry_lock:
        EDGE_SERVERS.append(new_edge)

    print(f"[LB] Registered new edge: {data['id']} at {data['url']}")
    return jsonify({"status": "registered", "edge": new_edge}), 201


@app.route("/stats", methods=["GET"])
def stats():
    """Load balancer performance stats."""
    with stats_lock:
        return jsonify({
            "server": "load_balancer",
            **lb_stats,
            "healthy_edges": sum(1 for e in EDGE_SERVERS if e["healthy"]),
            "total_edges": len(EDGE_SERVERS),
        })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "server": "load_balancer"}), 200


# ─── Startup ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Start background health checker
    hc_thread = threading.Thread(target=run_health_checks, daemon=True)
    hc_thread.start()
    print("[LB] Health check thread started (interval=10s)")
    print("[LB] Starting load balancer on port 8080...")
    app.run(host="0.0.0.0", port=8080, debug=False)
