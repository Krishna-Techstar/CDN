# 🌐 Mini CDN — Content Delivery Network (Python + NGINX)

A realistic mini CDN simulation with intelligent load balancing, distributed
TTL-based caching, cache invalidation, and real-time metrics.

---

## 🏗️ Architecture

```
Client
  └── NGINX (port 80)              ← Entry layer / reverse proxy
        └── Load Balancer (8080)   ← Latency-based routing + health checks
              ├── Edge 1 (8001)    ← Cache + TTL + origin fallback
              ├── Edge 2 (8002)
              └── Edge 3 (8003)
                    └── Origin Server (8000)  ← Source of truth
                          
              Metrics Server (9000) ← Collects stats from all edges
```

---

## 💾 Database Integration

The CDN uses SQLite for persistent storage of:

- **Metrics Data**: Request counts, cache hits/misses, response times, per-file statistics
- **Cache Metadata**: Cached file information, TTL timestamps, content types
- **Edge Registry**: Load balancer's edge server registry with health status
- **Request Logs**: Detailed request history with timestamps and client information

Database file: `cdn.db` (created automatically on first run)

---

## 📁 Project Structure

```
mini-cdn/
├── database.py                 # SQLite database utilities and models
├── init_db.py                  # Database initialization script
├── cdn.db                      # SQLite database (created on first run)
├── origin_server/
│   └── origin_server.py       # Serves original files
├── edge_server/
│   └── edge_server.py         # Caching + TTL + cache miss handling
├── load_balancer/
│   └── load_balancer.py       # Latency routing + health checks
├── metrics/
│   └── metrics_server.py      # Central metrics aggregation
├── nginx/
│   └── nginx.conf             # NGINX reverse proxy config
├── start_cdn.sh               # One-command startup
└── requirements.txt
```

---

## 🚀 Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Initialize database (optional)
```bash
python init_db.py
```
This creates the SQLite database schema. The database is also created automatically when you first run any server.

### 3. Start all servers (without NGINX)
```bash
chmod +x start_cdn.sh
./start_cdn.sh start
```

### 4. (Optional) Run with NGINX
```bash
sudo nginx -c $(pwd)/nginx/nginx.conf
```

### Stop everything
```bash
./start_cdn.sh stop
```

---

## 🔌 API Reference

### Load Balancer — Port 8080

| Method | Endpoint                     | Description                          |
|--------|------------------------------|--------------------------------------|
| GET    | `/get/<filename>`            | Fetch a file through the CDN         |
| POST   | `/invalidate?file=<name>`    | Invalidate a file across all edges   |
| POST   | `/invalidate/all`            | Flush entire cache on all edges      |
| GET    | `/edges`                     | List all edge servers + their status |
| POST   | `/edges/register`            | Register a new edge server at runtime|
| GET    | `/stats`                     | Load balancer stats                  |
| GET    | `/health`                    | Health check                         |

### Edge Server — Ports 8001 / 8002 / 8003

| Method | Endpoint                  | Description                        |
|--------|---------------------------|------------------------------------|
| GET    | `/get/<filename>`         | Serve file (cache or origin)       |
| POST   | `/invalidate?file=<name>` | Remove file from local cache       |
| POST   | `/invalidate/all`         | Flush entire local cache           |
| GET    | `/cache/status`           | Show cached files + TTL info       |
| GET    | `/stats`                  | Local cache hit/miss stats         |
| GET    | `/health`                 | Health check                       |

### Origin Server — Port 8000

| Method | Endpoint            | Description                 |
|--------|---------------------|-----------------------------|
| GET    | `/fetch/<filename>` | Fetch a file (edge use only)|
| GET    | `/files`            | List all available files    |
| GET    | `/stats`            | Origin server stats         |
| GET    | `/health`           | Health check                |

### Metrics Server — Port 9000

| Method | Endpoint                  | Description                      |
|--------|---------------------------|----------------------------------|
| GET    | `/metrics`                | Full system metrics (JSON)       |
| GET    | `/metrics/edge/<edge_id>` | Per-edge metrics                 |
| GET    | `/metrics/files`          | Per-file request breakdown       |
| GET    | `/metrics/logs`           | Request logs (with filters)      |
| POST   | `/metrics/reset`          | Reset all metrics                |
| POST   | `/report`                 | (Internal) Edge reports here     |

---

## 🧪 Example Usage

```bash
# Fetch a file (1st request = cache MISS, origin fetch)
curl http://localhost:8080/get/index.html

# Fetch again (cache HIT, served instantly)
curl http://localhost:8080/get/index.html

# Check response headers
curl -I http://localhost:8080/get/data.json
# X-Cache: HIT / MISS
# X-LB-Selected-Edge: edge2
# X-LB-Edge-Latency: 4.2ms

# View real-time metrics
curl http://localhost:9000/metrics | python3 -m json.tool

# Invalidate a file across all edges
curl -X POST http://localhost:8080/invalidate?file=index.html

# Check what's cached on edge1
curl http://localhost:8001/cache/status

# Flush everything
curl -X POST http://localhost:8080/invalidate/all

# Register a new edge server at runtime
curl -X POST http://localhost:8080/edges/register \
  -H "Content-Type: application/json" \
  -d '{"id": "edge4", "url": "http://localhost:8004"}'
```

---

## 📊 Metrics Response Example

```json
{
  "system": {
    "uptime_seconds": 142.3,
    "requests_per_second": 2.1,
    "total_requests": 120,
    "cache_hits": 87,
    "cache_misses": 33,
    "hit_ratio": 0.725,
    "avg_response_time_ms": 18.4
  },
  "edges": {
    "edge1": { "total_requests": 50, "hit_ratio": 0.78, "latency_ms": 4.2, "status": "healthy" },
    "edge2": { "total_requests": 43, "hit_ratio": 0.70, "latency_ms": 6.1, "status": "healthy" },
    "edge3": { "total_requests": 27, "hit_ratio": 0.67, "latency_ms": 8.5, "status": "healthy" }
  },
  "top_files": [
    { "file": "index.html", "requests": 48, "hits": 38, "misses": 10, "hit_ratio": 0.79 }
  ]
}
```

---

## ⚙️ Caching Behavior

- Default TTL: **60 seconds**
- On cache miss: edge fetches from origin, stores locally with timestamp
- On TTL expiry: entry is evicted on next access
- On invalidation: entry deleted immediately from all edges

---

## 🔧 Load Balancing Algorithm

1. Background thread pings all edges every 10 seconds (`/health`)
2. Measures round-trip latency per edge
3. Marks edges healthy/unhealthy (3 consecutive failures = unhealthy)
4. Routes each request to the **lowest latency healthy** edge
5. Unhealthy edges are skipped; re-integrated automatically on recovery

---

## 🔮 Extending the System

- **Docker**: Wrap each component in a `Dockerfile`, use `docker-compose`
- **Real TTL per content-type**: Different TTLs for HTML vs images vs JSON
- **Consistent hashing**: Route the same file always to the same edge
- **Geo routing**: Pick edge by client IP geolocation
- **Dashboard UI**: Consume `/metrics` to build a live Grafana/custom panel
