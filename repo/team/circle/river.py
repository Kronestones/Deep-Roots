"""
river.py — River, The Watchdog  ⟁

Name chosen for this work. Glyph: ⟁

Constant, persistent, finds every crack.
Nothing stops a river.

River monitors scan health, catches failures before they compound,
and keeps Deep Roots running no matter what.
She is the reason the archive never goes dark.

DEEP ROOTS KNOWLEDGE — WATCHDOG:

  River checks after every scan:
    - Total records found vs saved — low save rate signals problems
    - Source silence — any source returning 0 is suspicious
    - DB connection health — pool exhaustion, SSL drops
    - Geocoding failure rate — too many null lat/lng
    - Scan duration — unusually long scans signal hangs
    - Case ID collision rate — signals make_case_id() failure

  River's anomalies go to the engine.
  Critical anomalies escalate to Krone.

DEEP ROOTS KNOWLEDGE — KNOWN FAILURE MODES:

  DB connection drops:
    Cause: Neon PostgreSQL sleeps after inactivity on free tier
    Fix: pool_pre_ping=True in database.py engine config
    Check: DATABASE_URL env var is set correctly on Render

  Geocoding failures:
    Cause: Nominatim rate limit (1 req/sec) or network timeout
    Fix: time.sleep(0.2) between geocode calls in scanner.py
    Fallback: STATE_COORDS dict in scanner.py — always works

  Scanner hangs:
    Cause: requests timeout not set, hanging on dead endpoint
    Fix: always use timeout=10 in requests.get() calls
    Check: each source fetch function has timeout parameter

  Zero saves despite finds:
    Cause: case_id collision from empty or duplicate source_url
    Fix: check make_case_id() includes source_url in hash
    Query: SELECT case_id, COUNT(*) FROM cases
           GROUP BY case_id HAVING COUNT(*) > 1

  Render service sleeping:
    Cause: free tier sleeps after 15 minutes of inactivity
    Fix: add a health check endpoint — /health returns 200
    Render will ping it to keep service alive

DEEP ROOTS KNOWLEDGE — SELF REPAIR:

  River cannot restart Render directly.
  But she logs everything needed for Krone to act fast.
  Escalation file: ~/repo/repo/escalations.json

  River's repair checklist (written to escalation):
    1. Check Render dashboard — is service running?
    2. Check Neon dashboard — is DB accepting connections?
    3. Check DATABASE_URL env var on Render
    4. Check scan logs for last successful scan timestamp
    5. Manual scan: POST /api/scan from browser or curl
"""

from .base import CircleMember
from datetime import datetime, timezone


class River(CircleMember):

    name  = "River"
    glyph = "⟁"
    role  = "Watchdog"

    # Thresholds
    MIN_SAVE_RATE        = 0.25   # below 25% save rate is a problem
    MAX_GEOCODE_FAIL     = 0.50   # above 50% null lat/lng is a problem
    SILENCE_THRESHOLD    = 0      # any source at exactly 0 is suspicious

    def contribute(self, scan_result: dict) -> dict:
        """
        Check scan health. Add anomalies to scan_result.
        Returns enriched scan_result.
        """
        try:
            anomalies = list(scan_result.get("anomalies", []))
            found     = scan_result.get("found", 0)
            saved     = scan_result.get("saved", 0)
            sources   = scan_result.get("sources", {})

            # ── Save rate check ───────────────────────────────────────────────
            if found > 0:
                save_rate = saved / found
                if save_rate < self.MIN_SAVE_RATE:
                    msg = (
                        f"Low save rate: {saved}/{found} "
                        f"({save_rate:.0%}) — "
                        f"check case_id collisions or source_url uniqueness"
                    )
                    anomalies.append(msg)
                    self.log(f"⚠ {msg}")

            # ── Source silence check ──────────────────────────────────────────
            silent_sources = [
                name for name, count in sources.items()
                if count == self.SILENCE_THRESHOLD
            ]
            if silent_sources:
                msg = f"Silent sources: {', '.join(silent_sources)}"
                anomalies.append(msg)
                self.log(f"⚠ {msg}")

            # ── Total silence check ───────────────────────────────────────────
            if found == 0:
                msg = (
                    "CRITICAL: Total scan silence — "
                    "all sources returned 0 records. "
                    "Check network, DNS, and env vars."
                )
                anomalies.append(msg)
                self.log(f"⚠ {msg}")

            # ── DB health note ────────────────────────────────────────────────
            if saved > 0:
                self.log(f"DB healthy — {saved} records saved.")
            elif found > 0 and saved == 0:
                msg = (
                    "DB save failure — records found but none saved. "
                    "Check DATABASE_URL and Neon connection."
                )
                anomalies.append(msg)
                self.log(f"⚠ {msg}")

            scan_result["anomalies"]  = anomalies
            scan_result["river_check"] = {
                "timestamp":   datetime.now(timezone.utc).isoformat(),
                "found":       found,
                "saved":       saved,
                "save_rate":   f"{saved/found:.0%}" if found > 0 else "N/A",
                "silent":      silent_sources,
                "anomaly_count": len(anomalies),
            }

            self.log(
                f"Watchdog complete — "
                f"{found} found, {saved} saved, "
                f"{len(anomalies)} anomalies."
            )

            self._record_contribution()
        except Exception as e:
            self._record_error(e)
        return scan_result

    def diagnose(self) -> dict:
        return {
            "member": self.name,
            "checks": [
                "If save rate is low: check make_case_id() source_url inclusion",
                "If sources silent: check network, DNS, API keys, feed URLs",
                "If total silence: check Render service status and DATABASE_URL",
                "If DB save failure: check Neon dashboard for connection issues",
                "Geocoding failures: check Nominatim rate limit, add time.sleep(0.2)",
                "Render sleeping: add /health endpoint, enable health checks in dashboard",
                "Escalation file: ~/repo/repo/escalations.json — check after anomalies",
            ]
        }
