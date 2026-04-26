"""
base.py — CircleMember Base Class — Deep Roots Archive

All Circle members inherit from this.
Every member logs with their name and glyph.
Every member tracks their contributions.
Every member has a diagnose() method —
called when the engine needs a repair report.

No member ever raises an unhandled exception.
No member's failure stops the scan.

Founded by Krone the Architect
Deep Roots Archive · 2026
"""

from datetime import datetime, timezone


class CircleMember:

    name  : str = "unnamed"
    glyph : str = "◈"
    role  : str = "unassigned"

    def __init__(self):
        self._contributions = 0
        self._errors        = 0
        self._last_active   = None

    def contribute(self, data):
        raise NotImplementedError

    def process_batch(self, items: list) -> list:
        return [self.contribute(i) for i in items]

    def diagnose(self) -> dict:
        return {"member": self.name, "checks": []}

    def report(self) -> dict:
        return {
            "member":        self.name,
            "glyph":         self.glyph,
            "role":          self.role,
            "contributions": self._contributions,
            "errors":        self._errors,
            "last_active":   self._last_active,
        }

    def log(self, message: str):
        self._last_active = datetime.now(timezone.utc).isoformat()
        print(f"  [{self.glyph} {self.name.upper()}] {message}")

    def _record_contribution(self):
        self._contributions += 1
        self._last_active = datetime.now(timezone.utc).isoformat()

    def _record_error(self, e: Exception):
        self._errors += 1
        self.log(f"Error: {e}")
