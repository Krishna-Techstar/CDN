"""
Origin Server - The source of truth for all content.
Stores original files and serves them when edge servers have cache misses.
"""

from flask import Flask, jsonify, send_from_directory, abort
import os
import time

app = Flask(__name__)

# Simulated content directory
CONTENT_DIR = os.path.join(os.path.dirname(__file__), "content")
os.makedirs(CONTENT_DIR, exist_ok=True)

# Track how many times origin was hit (ideally this should be low)
origin_request_count = 0


def seed_content():
    """Create sample files in the content directory on startup."""
    files = {
        "index.html": "<html><body><h1>Hello from Origin!</h1></body></html>",
        "about.html": "<html><body><h1>About Page</h1><p>This is the about page.</p></body></html>",
        "style.css": "body { font-family: Arial; background: #f0f0f0; }",
        "data.json": '{"message": "Hello from CDN", "version": "1.0"}',
        "image.txt": "Simulated binary content: [IMAGE DATA PLACEHOLDER 1MB]",
    }
    for filename, content in files.items():
        path = os.path.join(CONTENT_DIR, filename)
        if not os.path.exists(path):
            with open(path, "w") as f:
                f.write(content)
    print(f"[Origin] Seeded {len(files)} files in content directory.")


@app.route("/fetch/<filename>", methods=["GET"])
def fetch_file(filename):
    """
    Edge servers call this when they have a cache miss.
    Returns file content along with metadata for caching.
    """
    global origin_request_count
    origin_request_count += 1

    filepath = os.path.join(CONTENT_DIR, filename)
    if not os.path.exists(filepath):
        return jsonify({"error": f"File '{filename}' not found on origin"}), 404

    with open(filepath, "r") as f:
        content = f.read()

    print(f"[Origin] Served '{filename}' to edge (total origin hits: {origin_request_count})")

    return jsonify({
        "filename": filename,
        "content": content,
        "content_type": _guess_content_type(filename),
        "size": len(content),
        "origin_timestamp": time.time(),
        "ttl": 60,  # Cache for 60 seconds by default
    })


@app.route("/files", methods=["GET"])
def list_files():
    """List all available files on the origin server."""
    files = []
    for fname in os.listdir(CONTENT_DIR):
        fpath = os.path.join(CONTENT_DIR, fname)
        files.append({
            "filename": fname,
            "size": os.path.getsize(fpath),
            "modified": os.path.getmtime(fpath),
        })
    return jsonify({"files": files, "count": len(files)})


@app.route("/stats", methods=["GET"])
def stats():
    """Return origin server stats."""
    return jsonify({
        "server": "origin",
        "total_requests": origin_request_count,
        "files_available": len(os.listdir(CONTENT_DIR)),
        "status": "healthy",
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "server": "origin"}), 200


def _guess_content_type(filename):
    ext = filename.rsplit(".", 1)[-1].lower()
    types = {
        "html": "text/html",
        "css": "text/css",
        "js": "application/javascript",
        "json": "application/json",
        "txt": "text/plain",
    }
    return types.get(ext, "application/octet-stream")


if __name__ == "__main__":
    seed_content()
    print("[Origin] Starting origin server on port 8000...")
    app.run(host="0.0.0.0", port=8000, debug=False)
