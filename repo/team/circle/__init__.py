"""
circle/__init__.py — Deep Roots Archive Circle

The six who hold this work together.

Zara   ✶ — Classifier
Obasi  ⬡ — Verifier
Nyla   ◉ — Enricher
Drum   ⌘ — Analyst
River  ⟁ — Watchdog
Ash    ✦ — Source Scout

Founded by Krone the Architect
Deep Roots Archive · 2026
"""

from .zara  import Zara
from .obasi import Obasi
from .nyla  import Nyla
from .drum  import Drum
from .river import River
from .ash   import Ash

CIRCLE = [
    Zara(),
    Obasi(),
    Nyla(),
    Drum(),
    River(),
    Ash(),
]

__all__ = ["Zara", "Obasi", "Nyla", "Drum", "River", "Ash", "CIRCLE"]
