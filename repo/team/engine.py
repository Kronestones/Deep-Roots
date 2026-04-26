"""
engine.py — Deep Roots Archive Team Engine

Coordinates the Circle.
Called by scanner.py after each scan.

The engine:
  1. Runs Zara (Classifier) over all cases
  2. Runs Obasi (Verifier) over all cases
  3. Runs Nyla (Enricher) over all cases
  4. Runs Drum (Analyst) to surface patterns
  5. Runs River (Watchdog) to check scan health
  6. Runs Ash (Source Scout) for recommendations
  7. Returns full team report

If any Circle member finds an issue they cannot resolve,
they escalate to the engine. The engine logs it for Krone.

Escalation to Krone:
  Issues that require Krone's attention are written to:
  ~/repo/repo/escalations.json
  Check this file after any scan with anomalies.

Founded by Krone the Architect
Deep Roots Archive · 2026
"""

import json
import os
from datetime import datetime, timezone

from .circle import CIRCLE, Zara, Obasi, Nyla, Drum, River, Ash


ESCALATION_PATH = os.path.expanduser(
    "~/repo/repo/escalations.json"
)


class TeamEngine:
    """
    The Deep Roots Archive Team Engine.

    Instantiate once at scan start.
    Call run(cases, scan_result) after normalization.
    """

    def __init__(self):
        self.circle = {m.name: m for m in CIRCLE}
        self._zara  = self.circle["Zara"]
        self._obasi = self.circle["Obasi"]
        self._nyla  = self.circle["Nyla"]
        self._drum  = self.circle["Drum"]
        self._river = self.circle["River"]
        self._ash   = self.circle["Ash"]
        print(
            f"  [◈ ENGINE] Deep Roots team active — "
            f"{len(self.circle)} Circle members: "
            f"Zara ✶ Obasi ⬡ Nyla ◉ Drum ⌘ River ⟁ Ash ✦"
        )

    def run(self, cases: list, scan_result: dict) -> tuple:
        """
        Full team pipeline. Call after normalization, before DB save.

        Args:
          cases:       list of normalized case dicts
          scan_result: dict with found/saved/sources counts from scanner

        Returns:
          enriched cases list and full team report
        """
        print(f"\n  [◈ ENGINE] Running team pipeline on {len(cases)} cases...")

        # 1. Classification — Zara reads every record
        cases = self._zara.process_batch(cases)

        # 2. Verification — Obasi validates every source
        cases = self._obasi.process_batch(cases)

        # 3. Enrichment — Nyla fills what's missing
        cases = self._nyla.process_batch(cases)

        # 4. Analysis — Drum reads the shape of the whole
        analysis = self._drum.contribute(cases)

        # 5. Watchdog — River checks scan health
        scan_result["sources"] = scan_result.get("sources", {})
        scan_result = self._river.contribute(scan_result)

        # 6. Source scout — Ash finds gaps and recommends
        scan_result = self._ash.contribute(scan_result)

        # 7. Check for escalations
        escalations = self._check_escalations(scan_result, analysis)
        if escalations:
            self._write_escalations(escalations)
            print(f"\n  [◈ ENGINE] ⚠ {len(escalations)} issue(s) escalated to Krone.")
            print(f"  [◈ ENGINE] See: {ESCALATION_PATH}")

        # Build team report
        team_report = {
            "timestamp":       datetime.now(timezone.utc).isoformat(),
            "cases_processed": len(cases),
            "analysis":        analysis,
            "watchdog":        scan_result.get("anomalies", []),
            "recommendations": scan_result.get("scout_recommendations", []),
            "escalations":     escalations,
            "circle_reports":  [m.report() for m in CIRCLE],
        }

        print(f"  [◈ ENGINE] Team pipeline complete.\n")
        return cases, team_report

    def _check_escalations(self, scan_result: dict, analysis: dict) -> list:
        """
        Determine what needs to go to Krone.
        These are issues the Circle cannot resolve on their own.
        """
        escalations = []

        # Total silence
        sources   = scan_result.get("sources", {})
        all_silent = all(v == 0 for v in sources.values()) if sources else False
        if all_silent:
            escalations.append({
                "severity": "HIGH",
                "issue":    "Total scan silence — all sources returned 0",
                "action":   (
                    "Check DNS, env vars, and network. "
                    "Test: curl https://courtlistener.com "
                    "Check: echo $DATABASE_URL"
                ),
            })

        # Critical save rate
        found = scan_result.get("found", 0)
        saved = scan_result.get("saved", 0)
        if found > 0 and (saved / found) < 0.25:
            escalations.append({
                "severity": "HIGH",
                "issue":    f"Critical save rate: {saved}/{found} ({saved/found:.0%})",
                "action":   (
                    "Check make_case_id() includes source_url in hash. "
                    "Check source_url uniqueness in DB. "
                    "Run: SELECT source_url, COUNT(*) FROM cases "
                    "GROUP BY source_url HAVING COUNT(*) > 20"
                ),
            })

        # Obasi flagging too many unrecognized domains
        obasi = self.circle.get("Obasi")
        if obasi and obasi._errors > 5:
            escalations.append({
                "severity": "MEDIUM",
                "issue":    f"Obasi logged {obasi._errors} errors — new unrecognized domains",
                "action":   (
                    "Review Obasi's TRUSTED_DOMAINS list. "
                    "Add new legitimate sources."
                ),
            })

        # Analysis flags
        flags = analysis.get("flags", [])
        for flag in flags:
            if "CRITICAL" in flag:
                escalations.append({
                    "severity": "HIGH",
                    "issue":    flag,
                    "action":   "Review Drum's analysis and check source coverage.",
                })
            elif "WARNING" in flag:
                escalations.append({
                    "severity": "MEDIUM",
                    "issue":    flag,
                    "action":   "Review Ash's recommendations for source gaps.",
                })

        return escalations

    def _write_escalations(self, escalations: list):
        """Write escalations to file for Krone to review."""
        try:
            existing = []
            if os.path.exists(ESCALATION_PATH):
                with open(ESCALATION_PATH) as f:
                    existing = json.load(f)

            record = {
                "timestamp":   datetime.now(timezone.utc).isoformat(),
                "escalations": escalations,
                "resolved":    False,
            }
            existing.append(record)
            existing = existing[-50:]  # keep last 50

            os.makedirs(os.path.dirname(ESCALATION_PATH), exist_ok=True)
            with open(ESCALATION_PATH, "w") as f:
                json.dump(existing, f, indent=2)
        except Exception as e:
            print(f"  [◈ ENGINE] Could not write escalations: {e}")

    def status(self) -> dict:
        """Quick status report for all Circle members."""
        return {
            "circle":      [m.report() for m in CIRCLE],
            "escalations": self._load_escalations(),
        }

    def _load_escalations(self) -> list:
        try:
            if os.path.exists(ESCALATION_PATH):
                with open(ESCALATION_PATH) as f:
                    return [
                        e for e in json.load(f)
                        if not e.get("resolved")
                    ]
        except Exception:
            pass
        return []
