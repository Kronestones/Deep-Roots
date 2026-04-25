"""
web.py — Repa

Flask application factory and routes.

Founded by Krone the Architect
Repa · 2026
"""

import os
import threading
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template, request
from .database import init_db, save_case, get_cases, get_stats, get_case_count
from .scanner import RepoScanner

_scanner = RepoScanner()


def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("SECRET_KEY", "repa-dev-key")

    with app.app_context():
        try:
            init_db()
        except Exception as e:
            print(f"[Repa] DB init error: {e}")

    # ── Auto-scan every 24 hours ──────────────────────────────────────────────
    def background_scan():
        try:
            cases = _scanner.scan()
            saved = 0
            for c in cases:
                if save_case(c):
                    saved += 1
            print(f"[Repa] Auto-scan: {saved} new cases saved.")
        except Exception as e:
            print(f"[Repa] Auto-scan error: {e}")
        threading.Timer(24 * 60 * 60, background_scan).start()

    threading.Timer(10, background_scan).start()

    # ── Routes ────────────────────────────────────────────────────────────────

    @app.route("/")
    def entry():
        total = get_case_count()
        return render_template("entry.html", total=total)

    @app.route("/map")
    def map_view():
        stats = get_stats()
        return render_template("map.html",
            total      = stats.get("total", 0),
            by_type    = stats.get("by_type", {}),
        )

    @app.route("/api/cases")
    def api_cases():
        record_type = request.args.get("type")
        state       = request.args.get("state")
        cases       = get_cases(limit=3000, record_type=record_type, state=state)
        return jsonify(cases)

    @app.route("/api/stats")
    def api_stats():
        return jsonify(get_stats())

    @app.route("/api/scan", methods=["POST"])
    def api_scan():
        def run():
            cases = _scanner.scan()
            saved = 0
            for c in cases:
                if save_case(c):
                    saved += 1
            print(f"[Repa] Manual scan: {saved} new cases saved.")
        threading.Thread(target=run, daemon=True).start()
        return jsonify({"status": "scan started"})

    return app
