"""
drum.py — Drum, The Analyst  ⌘

Name chosen for this work. Glyph: ⌘

The drum carries memory across generations.
Drum reads the patterns no one else sees.
He looks at the full body of records and finds
what the individual cases cannot tell alone.

He does not judge individual records —
he reads the shape of the whole.

DEEP ROOTS KNOWLEDGE — ANALYSIS:

  Drum surfaces:
    - Geographic clusters — where violence concentrates
    - Temporal patterns — spikes, anniversaries, cycles
    - Record type distribution — what's being documented vs. what's missing
    - Source diversity — are we pulling from enough sources
    - Race distribution — who is being documented, who is missing
    - Status patterns — how many no_charges vs convicted

  Drum's output goes into the team report.
  Krone reviews it after each scan.

  Key questions Drum always asks:
    Are MMIW records growing? They should be — it's an ongoing crisis.
    Are police killings current? Should reflect last 30 days minimum.
    Are historical records being added? Should grow over time.
    Is any state overrepresented? May signal source bias.
    Is any race underrepresented? May signal collection gap.

DEEP ROOTS KNOWLEDGE — PATTERNS TO WATCH:

  Geographic clusters to expect:
    Mississippi, Alabama, Georgia — high historical lynching density
    Oklahoma — Tulsa, Osage, high historical massacre density
    Montana, South Dakota, New Mexico — high MMIW density
    Minneapolis, Louisville, Ferguson — high police killing density

  If these areas show zero records — source gap, not absence of violence.

DEEP ROOTS KNOWLEDGE — REPAIRS:

  If analysis shows source bias:
    1. Tell Ash (Source Scout) which regions are underrepresented
    2. Add regional news sources for missing areas
    3. Check if CourtListener queries cover all federal districts

  If MMIW count is not growing:
    1. Check scanner MMIW-specific queries
    2. Check Sovereign Bodies Institute feed
    3. Check NamUs integration
"""

from .base import CircleMember
from collections import Counter


class Drum(CircleMember):

    name  = "Drum"
    glyph = "⌘"
    role  = "Analyst"

    # States with historically high violence density
    EXPECTED_CLUSTERS = {
        "police_killing": ["MN", "CA", "TX", "FL", "NY", "IL"],
        "lynching":       ["MS", "AL", "GA", "TX", "AR", "LA"],
        "massacre":       ["OK", "FL", "NC", "AR", "SC"],
        "mmiw":           ["MT", "SD", "NM", "AK", "WA", "MN"],
        "hate_crime":     ["NY", "CA", "TX", "FL", "NJ"],
    }

    def contribute(self, cases: list) -> dict:
        """
        Analyze the full case set.
        Returns an analysis dict for the team report.
        """
        try:
            if not cases:
                return {"status": "no cases to analyze"}

            total = len(cases)

            # Type distribution
            by_type = Counter(c.get("record_type", "unknown") for c in cases)

            # State distribution
            by_state = Counter(c.get("state", "??") for c in cases)

            # Race distribution
            by_race = Counter(
                c.get("victim_race", "unknown") or "unknown"
                for c in cases
            )

            # Status distribution
            by_status = Counter(c.get("status", "unknown") for c in cases)

            # Historical vs current
            historical = sum(1 for c in cases if c.get("is_historical"))
            current    = total - historical

            # Source diversity
            sources = set(c.get("source_name", "") for c in cases)

            # Geographic gaps
            gaps = self._find_geographic_gaps(by_type, by_state)

            # Flags
            flags = []
            if by_type.get("mmiw", 0) == 0:
                flags.append("WARNING: No MMIW records — check Indigenous sources")
            if by_type.get("lynching", 0) == 0:
                flags.append("WARNING: No lynching records — check EJI data import")
            if by_type.get("massacre", 0) == 0:
                flags.append("WARNING: No massacre records — check seed data")
            if current == 0:
                flags.append("WARNING: No current records — scanner may not be pulling live data")
            if len(sources) < 2:
                flags.append("WARNING: Only one source — check scanner source list")

            analysis = {
                "total":          total,
                "by_type":        dict(by_type),
                "by_state":       dict(by_state.most_common(10)),
                "by_race":        dict(by_race),
                "by_status":      dict(by_status),
                "historical":     historical,
                "current":        current,
                "source_count":   len(sources),
                "sources":        list(sources),
                "geographic_gaps": gaps,
                "flags":          flags,
            }

            if flags:
                for flag in flags:
                    self.log(flag)

            self.log(
                f"Analysis complete — {total} records, "
                f"{len(by_type)} types, {len(sources)} sources, "
                f"{len(flags)} flags."
            )

            self._record_contribution()
            return analysis

        except Exception as e:
            self._record_error(e)
            return {"error": str(e)}

    def _find_geographic_gaps(self, by_type: Counter, by_state: Counter) -> list:
        """Find expected clusters that have zero records."""
        gaps = []
        for rtype, expected_states in self.EXPECTED_CLUSTERS.items():
            type_count = by_type.get(rtype, 0)
            if type_count == 0:
                continue
            for state in expected_states:
                if by_state.get(state, 0) == 0:
                    gaps.append(
                        f"{state} has no {rtype} records — "
                        f"historically high density area, likely a source gap"
                    )
        return gaps

    def diagnose(self) -> dict:
        return {
            "member": self.name,
            "checks": [
                "If MMIW count is zero: check scanner MMIW queries and Sovereign Bodies source",
                "If lynching count is zero: check EJI data import in seed.py",
                "If all records are one type: check Zara's classification rules",
                "If all records from one state: check source geographic coverage",
                "If race is unknown for most records: check Nyla's RACE_SIGNALS",
                "Geographic gaps in expected clusters indicate source gaps, not absence of violence",
            ]
        }
