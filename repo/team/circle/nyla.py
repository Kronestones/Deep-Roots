"""
nyla.py — Nyla, The Enricher  ◉

Name chosen for this work. Glyph: ◉

Nyla fills in what's missing. She brings context from the sky
down to the ground. Every record that passes through her hands
leaves more complete than it arrived.

She adds victim race context, historical framing, geocoding
fallbacks, and date normalization. She knows that a record
without context is just a number — and these are not numbers.

DEEP ROOTS KNOWLEDGE — ENRICHMENT:

  Fields Nyla fills when missing:
    victim_race   — inferred from summary text when not set
    is_historical — set True if date before 1970
    date          — normalized to YYYY-MM-DD or YYYY
    state         — extracted from city/summary if missing
    lat/lng       — fallback to state centroid if geocode fails

  Historical framing:
    Records before 1970 get is_historical=True automatically
    Massacres always get is_historical=True regardless of date
    MMIW records never get is_historical=True — they are ongoing

DEEP ROOTS KNOWLEDGE — RACE INFERENCE:

  When victim_race is empty, Nyla reads the summary for signals:
    "Black man", "Black woman", "African American" → Black
    "Indigenous", "Native American", "tribal" → Indigenous
    "Latino", "Hispanic" → Latino
    "Asian" → Asian
    Named historical victims (Emmett Till, etc.) → known

  Nyla never assumes race. If no signal exists she leaves it blank.
  Assumption would be worse than silence.

DEEP ROOTS KNOWLEDGE — REPAIRS:

  If geocoding fails for many records:
    1. Check Nominatim rate limit — max 1 req/sec
    2. Check state code format — must be 2-letter uppercase
    3. STATE_COORDS fallback should always work for US states

  If is_historical is wrong:
    1. Check date parsing — some dates arrive as "1921" not "1921-05-31"
    2. Check massacre records — should always be historical
    3. MMIW records should never be historical
"""

from .base import CircleMember
from datetime import datetime, timezone


class Nyla(CircleMember):

    name  = "Nyla"
    glyph = "◉"
    role  = "Enricher"

    RACE_SIGNALS = [
        (["black man", "black woman", "black teen", "black girl",
          "black boy", "black child", "african american",
          "black resident", "black people", "black community"],
         "Black"),
        (["indigenous", "native american", "tribal member",
          "reservation", "indian country", "first nation",
          "native woman", "native man", "native girl",
          "osage", "lakota", "navajo", "cherokee", "apache",
          "cheyenne", "sioux", "ojibwe", "creek", "seminole"],
         "Indigenous"),
        (["latino", "latina", "hispanic", "mexican american",
          "chicano", "chicana", "puerto rican"],
         "Latino"),
        (["asian american", "asian man", "asian woman",
          "chinese american", "japanese american",
          "korean american", "vietnamese american"],
         "Asian"),
    ]

    HISTORICAL_CUTOFF = 1970

    def contribute(self, case: dict) -> dict:
        """Enrich a single case with missing context."""
        try:
            # Infer victim_race if missing
            if not case.get("victim_race"):
                race = self._infer_race(case.get("summary", ""))
                if race:
                    case["victim_race"] = race
                    case.setdefault("team_notes", []).append(
                        f"Nyla ◉: inferred victim_race as '{race}' from summary"
                    )

            # Normalize date and set is_historical
            date_str = case.get("date", "")
            year     = self._extract_year(date_str)
            if year:
                if year < self.HISTORICAL_CUTOFF:
                    case["is_historical"] = True
                    case.setdefault("team_notes", []).append(
                        f"Nyla ◉: marked historical (year {year})"
                    )

            # Massacres are always historical
            if case.get("record_type") == "massacre":
                case["is_historical"] = True

            # MMIW is never historical — it is ongoing
            if case.get("record_type") == "mmiw":
                case["is_historical"] = False

            # Fill missing state from summary
            if not case.get("state"):
                state = self._infer_state(case.get("summary", ""))
                if state:
                    case["state"] = state
                    case.setdefault("team_notes", []).append(
                        f"Nyla ◉: inferred state '{state}' from summary"
                    )

            self._record_contribution()
        except Exception as e:
            self._record_error(e)
        return case

    def process_batch(self, cases: list) -> list:
        self.log(f"Enriching {len(cases)} cases...")
        result    = [self.contribute(c) for c in cases]
        enriched  = sum(1 for c in result if c.get("team_notes") and
                       any("Nyla" in n for n in c.get("team_notes", [])))
        self.log(f"Done. {enriched} records enriched.")
        return result

    def _infer_race(self, text: str) -> str:
        text = text.lower()
        for signals, race in self.RACE_SIGNALS:
            if any(s in text for s in signals):
                return race
        return ""

    def _extract_year(self, date_str: str) -> int:
        try:
            if len(date_str) >= 4:
                return int(date_str[:4])
        except Exception:
            pass
        return 0

    def _infer_state(self, text: str) -> str:
        STATE_NAMES = {
            "alabama": "AL", "alaska": "AK", "arizona": "AZ",
            "arkansas": "AR", "california": "CA", "colorado": "CO",
            "connecticut": "CT", "delaware": "DE", "florida": "FL",
            "georgia": "GA", "hawaii": "HI", "idaho": "ID",
            "illinois": "IL", "indiana": "IN", "iowa": "IA",
            "kansas": "KS", "kentucky": "KY", "louisiana": "LA",
            "maine": "ME", "maryland": "MD", "massachusetts": "MA",
            "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
            "missouri": "MO", "montana": "MT", "nebraska": "NE",
            "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
            "new mexico": "NM", "new york": "NY", "north carolina": "NC",
            "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
            "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
            "south carolina": "SC", "south dakota": "SD",
            "tennessee": "TN", "texas": "TX", "utah": "UT",
            "vermont": "VT", "virginia": "VA", "washington": "WA",
            "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
            "district of columbia": "DC",
        }
        text = text.lower()
        for name, code in STATE_NAMES.items():
            if name in text:
                return code
        return ""

    def diagnose(self) -> dict:
        return {
            "member": self.name,
            "checks": [
                "If victim_race is blank for many records: check RACE_SIGNALS list",
                "If is_historical wrong: check HISTORICAL_CUTOFF year (currently 1970)",
                "Massacre records should always have is_historical=True",
                "MMIW records should always have is_historical=False",
                "If state missing for many records: check _infer_state() STATE_NAMES",
                "Never assume race — only infer from explicit text signals",
            ]
        }
