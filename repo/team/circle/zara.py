"""
zara.py — Zara, The Classifier  ✶

Name chosen for this work. Glyph: ✶

Zara reads every record that comes through the pipeline and determines
record type with precision — not just first keyword match, but
weighted context analysis across the full record.

She finds what something truly is beneath the surface label.

DEEP ROOTS KNOWLEDGE — CLASSIFICATION:

  Valid record types:
    police_killing  — any death caused by law enforcement
    lynching        — racial terror killing, 1865-present
    massacre        — mass killing of POC by white mobs or state
    mmiw            — missing and murdered Indigenous people
    hate_crime      — racially motivated violence, court documented

  Known failure: records may arrive with wrong or missing record_type
  Zara corrects this by running weighted analysis on full text.

  Sources use different language:
    CourtListener: "civil rights", "section 1983", "excessive force"
    AP/news: "killed by police", "officer-involved", "hate crime"
    DOJ: "civil rights violation", "color of law", "hate crime"

DEEP ROOTS KNOWLEDGE — REPAIRS:

  If classification accuracy drops:
    1. Check RULES list below — add new keyword patterns
    2. Check each source's fetch function for type inference
    3. Review misclassified records in DB and add their keywords

  To add a new record type:
    1. Add to database.py RepoCase record_type column comment
    2. Add to get_stats() query in database.py
    3. Add to TYPE_COLORS in map.html
    4. Update Zara's RULES below
"""

from .base import CircleMember


class Zara(CircleMember):

    name  = "Zara"
    glyph = "✶"
    role  = "Classifier"

    # (keywords, record_type, confidence_weight)
    # Higher weight wins. More specific terms have higher weight.
    RULES = [
        # Police killings
        (["killed by police", "police killing", "police killed",
          "officer-involved shooting", "officer-involved death",
          "died in police custody", "police use of force",
          "excessive force", "police brutality", "choked by police",
          "shot by officer", "killed by officer", "police shooting",
          "deputy shooting", "trooper shooting", "unarmed man killed",
          "unarmed woman killed", "unarmed black", "died in custody"],
         "police_killing", 10),

        # Lynchings
        (["lynched", "lynching", "racial terror", "hanged by mob",
          "burned alive", "dragged behind", "white mob killed",
          "racial terror killing", "ku klux klan killed",
          "kkk murder", "racial terror murder"],
         "lynching", 10),

        # Massacres
        (["massacre", "race massacre", "race riot", "pogrom",
          "burned to the ground", "destroyed by mob",
          "black wall street", "greenwood", "rosewood",
          "tulsa 1921", "elaine arkansas", "wilmington 1898",
          "move bombing", "osage murders", "mass killing of black",
          "mass killing of indigenous"],
         "massacre", 10),

        # MMIW / MMIP
        (["missing and murdered indigenous", "mmiw", "mmip",
          "missing indigenous woman", "missing native woman",
          "murdered indigenous woman", "murdered native woman",
          "missing and murdered native", "indigenous missing",
          "native american missing", "tribal member missing",
          "reservation missing", "indian country missing"],
         "mmiw", 10),

        # Hate crimes
        (["hate crime", "racially motivated", "racial slur",
          "white supremacist", "neo-nazi", "kkk", "ku klux klan",
          "racial bias", "motivated by race", "anti-black",
          "racial hatred", "racial violence", "bias crime",
          "ethnic intimidation", "racial intimidation",
          "church bombing", "church burning", "cross burning"],
         "hate_crime", 9),

        # Fallback police
        (["section 1983", "civil rights violation", "color of law",
          "excessive force", "police misconduct", "wrongful death",
          "law enforcement killing"],
         "police_killing", 7),
    ]

    def contribute(self, case: dict) -> dict:
        """Re-classify record_type using weighted context analysis."""
        try:
            text = (
                (case.get("summary") or "") + " " +
                (case.get("source_name") or "") + " " +
                (case.get("victim_race") or "")
            ).lower()

            best_type  = case.get("record_type", "hate_crime")
            best_score = 0

            for keywords, rtype, weight in self.RULES:
                for kw in keywords:
                    if kw in text:
                        if weight > best_score:
                            best_score = weight
                            best_type  = rtype
                        break

            if best_type != case.get("record_type") and best_score >= 7:
                self.log(
                    f"Reclassified '{case.get('record_type')}' → "
                    f"'{best_type}' (confidence {best_score}/10) "
                    f"— {case.get('case_id','?')}"
                )
                case["record_type"] = best_type
                case.setdefault("team_notes", []).append(
                    f"Zara ✶: reclassified to {best_type} (confidence {best_score}/10)"
                )

            self._record_contribution()
        except Exception as e:
            self._record_error(e)
        return case

    def process_batch(self, cases: list) -> list:
        self.log(f"Classifying {len(cases)} cases...")
        before = {c.get("case_id"): c.get("record_type") for c in cases}
        result = [self.contribute(c) for c in cases]
        corrected = sum(
            1 for c in result
            if c.get("record_type") != before.get(c.get("case_id"))
        )
        self.log(f"Done. {corrected} reclassifications.")
        return result

    def diagnose(self) -> dict:
        return {
            "member": self.name,
            "checks": [
                "Verify record types in database.py match Zara's RULES",
                "Check each source fetch function for type inference drift",
                "Review any records defaulting to hate_crime — may need new rules",
                "If MMIW records misclassified: check mmiw keyword list",
                "If police killings missed: check officer-involved language variants",
            ]
        }
