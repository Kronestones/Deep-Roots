"""
pool.py — Deep Roots Archive Consultant Pool

Loads all consultant profiles and spins up specialist workers
dynamically. Consultants are always active — feeding the Circle
with domain knowledge on every scan.

Domains:
    record_type   — police killing, lynching, massacre, mmiw, hate crime
    source_health — all active and pending sources
    pattern       — geographic clusters, documentation gaps, temporal patterns
    jurisdiction  — all 50 states + DC, each with specific history

The Circle governs. The consultants serve.
Zara, Obasi, Nyla, Drum, River, Ash make final decisions.
Consultants provide the knowledge that informs those decisions.

Founded by Krone the Architect
Deep Roots Archive · 2026
"""

import json
import os
from typing import Optional


PROFILES_DIR = os.path.join(os.path.dirname(__file__), "profiles")


class Consultant:
    """
    A single consultant, instantiated from a profile dict.
    Lightweight — carries knowledge and answers questions.
    Always active. Always contributing.
    """

    def __init__(self, profile: dict):
        self.id      = profile.get("id", "unknown")
        self.name    = profile.get("name", "Unnamed Consultant")
        self.domain  = profile.get("domain", "general")
        self.focus   = (
            profile.get("focus") or
            profile.get("state") or
            profile.get("source", "")
        )
        self.profile  = profile
        self._queries = 0

    def advise(self, context: dict = None) -> dict:
        """Return this consultant's knowledge about their focus area."""
        self._queries += 1
        return {
            "consultant": self.name,
            "domain":     self.domain,
            "focus":      self.focus,
            "profile":    self.profile,
        }

    def diagnose(self, issue: str = "") -> list:
        """Return diagnostic steps for an issue in this consultant's domain."""
        self._queries += 1
        failure_modes = self.profile.get("failure_modes", [])
        checks        = self.profile.get("checks", [])
        notes         = [self.profile.get("notes", "")] if self.profile.get("notes") else []
        known_gaps    = self.profile.get("known_gaps", [])
        known_patterns = self.profile.get("known_patterns", [])
        return failure_modes + checks + notes + known_gaps + known_patterns

    def __repr__(self):
        return f"Consultant({self.id}: {self.name})"


class ConsultantPool:
    """
    The full Deep Roots consultant pool.
    Loads all profile JSON files and makes consultants available
    by domain, focus, state, source, or ID.

    Always active. Always feeding the Circle.
    """

    def __init__(self):
        self._consultants = {}
        self._load_all()
        print(
            f"  [◈ POOL] {len(self._consultants)} consultants loaded — "
            f"serving Zara ✶ Obasi ⬡ Nyla ◉ Drum ⌘ River ⟁ Ash ✦"
        )

    def _load_all(self):
        """Load all profile JSON files from profiles/ directory."""
        if not os.path.exists(PROFILES_DIR):
            print(f"  [◈ POOL] Profiles directory not found: {PROFILES_DIR}")
            return

        for filename in sorted(os.listdir(PROFILES_DIR)):
            if not filename.endswith(".json"):
                continue
            path = os.path.join(PROFILES_DIR, filename)
            try:
                with open(path) as f:
                    profiles = json.load(f)
                if isinstance(profiles, list):
                    for p in profiles:
                        c = Consultant(p)
                        self._consultants[c.id] = c
                elif isinstance(profiles, dict):
                    c = Consultant(profiles)
                    self._consultants[c.id] = c
            except Exception as e:
                print(f"  [◈ POOL] Failed to load {filename}: {e}")

    def get(self, consultant_id: str) -> Optional[Consultant]:
        """Get a specific consultant by ID."""
        return self._consultants.get(consultant_id)

    def by_domain(self, domain: str) -> list:
        """Get all consultants for a domain."""
        return [c for c in self._consultants.values() if c.domain == domain]

    def by_focus(self, focus: str) -> list:
        """Get consultants matching a focus keyword."""
        focus = focus.lower()
        return [
            c for c in self._consultants.values()
            if focus in (c.focus or "").lower()
            or focus in c.name.lower()
        ]

    def for_state(self, state: str) -> Optional[Consultant]:
        """Get the jurisdiction specialist for a state."""
        return self._consultants.get(f"j_{state.upper()}")

    def for_record_type(self, rtype: str) -> Optional[Consultant]:
        """Get the record type specialist."""
        matches = [
            c for c in self._consultants.values()
            if c.domain == "record_type" and c.focus == rtype
        ]
        return matches[0] if matches else None

    def for_source(self, source_name: str) -> Optional[Consultant]:
        """Get the source health consultant for a source."""
        matches = [
            c for c in self._consultants.values()
            if c.domain == "source_health"
            and source_name.lower() in c.name.lower()
        ]
        return matches[0] if matches else None

    def diagnose_source(self, source_name: str) -> list:
        """Get diagnostic steps for a failing source. Called by River."""
        consultant = self.for_source(source_name)
        if consultant:
            return consultant.diagnose()
        return [f"No consultant found for source: {source_name}"]

    def geographic_context(self, state: str) -> dict:
        """
        Get full geographic context for a state.
        Called by Drum during analysis.
        """
        specialist = self.for_state(state)
        if specialist:
            return specialist.advise()
        return {"state": state, "notes": "No specialist profile found."}

    def expected_types_for_state(self, state: str) -> list:
        """
        What record types should we expect in this state?
        Called by Drum to identify geographic gaps.
        """
        specialist = self.for_state(state)
        if specialist:
            return specialist.profile.get("expected_types", [])
        return []

    def pending_imports(self) -> list:
        """
        List all sources pending import.
        Called by Ash during source scouting.
        """
        return [
            c for c in self._consultants.values()
            if c.domain == "source_health"
            and c.profile.get("expected_min", -1) == 0
        ]

    def run_source_health_checks(self) -> dict:
        """
        Return knowledge-based assessment of all sources.
        Called by River on each scan.
        """
        report = {}
        for c in self.by_domain("source_health"):
            report[c.focus or c.name] = {
                "expected_min":  c.profile.get("expected_min"),
                "check_url":     c.profile.get("check_url"),
                "failure_modes": c.profile.get("failure_modes", []),
                "notes":         c.profile.get("notes", ""),
            }
        return report

    def pattern_knowledge(self) -> list:
        """
        Return all pattern consultant knowledge.
        Called by Drum during analysis.
        """
        return [c.advise() for c in self.by_domain("pattern")]

    def coverage_report(self) -> dict:
        """Return summary of consultant coverage by domain."""
        from collections import Counter
        domains = Counter(c.domain for c in self._consultants.values())
        return {
            "total":   len(self._consultants),
            "domains": dict(domains),
        }

    def all_ids(self) -> list:
        return sorted(self._consultants.keys())

    def __len__(self):
        return len(self._consultants)

    def __repr__(self):
        return f"ConsultantPool({len(self._consultants)} consultants)"
