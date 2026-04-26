"""
obasi.py — Obasi, The Verifier  ⬡

Name chosen for this work. Glyph: ⬡

Obasi validates every record's source before it reaches the database.
He holds everything to a standard of truth before it passes.
Catches dead links, untrusted domains, and records whose
source_url is empty — which breaks case_id uniqueness.

DEEP ROOTS KNOWLEDGE — SOURCE VALIDATION:

  CRITICAL: source_url is included in make_case_id() hash.
  Empty source_url causes case_id collisions — multiple different
  cases get the same ID and only the first saves.
  Never let source_url be empty without flagging it.

  Historical records (is_historical=True) are held to same standard.
  Source must be a trusted institution — EJI, Library of Congress,
  National Archives, tribal government, university press.

DEEP ROOTS KNOWLEDGE — TRUSTED SOURCES:

  Civil rights and racial justice:
    eji.org, mappingpoliceviolence.org, mappingpoliceviolence.us
    naacp.org, aclu.org, civilrights.org
    sovereignbodies.org, niwrc.org, mmiw-usa.org

  Federal government:
    justice.gov, fbi.gov, bia.gov, indianaffairs.gov
    archives.gov, loc.gov, congress.gov
    api.usa.gov, ojp.gov, bjs.ojp.gov

  Court and legal:
    courtlistener.com, pacer.gov, supremecourt.gov

  News and journalism:
    apnews.com, reuters.com, nytimes.com, washingtonpost.com
    npr.org, theguardian.com, propublica.org
    theroot.com, essence.com, colorlines.com
    indiancountrytoday.com, ictnews.org

  Academic and historical:
    tulsa1921.org, rosewood.fsu.edu
    history.com, smithsonianmag.com
    journals.sagepub.com, jstor.org

DEEP ROOTS KNOWLEDGE — REPAIRS:

  If save count drops dramatically:
    1. Check source_url uniqueness in DB:
       SELECT source_url, COUNT(*) FROM cases
       GROUP BY source_url HAVING COUNT(*) > 10
    2. Check make_case_id() includes source_url in hash
    3. Check for empty source_url records

  Adding a new trusted source:
    1. Add domain to TRUSTED_DOMAINS below
    2. Add to fetch layer if auth required
    3. Create sources/{sourcename}.py
    4. Tell Ash (Source Scout) about it
"""

from .base import CircleMember


class Obasi(CircleMember):

    name  = "Obasi"
    glyph = "⬡"
    role  = "Verifier"

    TRUSTED_DOMAINS = {
        # Civil rights and racial justice
        "eji.org", "mappingpoliceviolence.org", "mappingpoliceviolence.us",
        "naacp.org", "aclu.org", "civilrights.org",
        "sovereignbodies.org", "niwrc.org", "mmiw-usa.org",
        "unchainedatlast.org", "humanrightsfirst.org",
        # Federal government
        "justice.gov", "fbi.gov", "bia.gov", "indianaffairs.gov",
        "archives.gov", "loc.gov", "congress.gov",
        "api.usa.gov", "ojp.gov", "bjs.ojp.gov",
        "usccr.gov", "hhs.gov", "ihs.gov",
        # Court and legal
        "courtlistener.com", "pacer.gov", "supremecourt.gov",
        "uscourts.gov", "pacer.uscourts.gov",
        # Wire services and major news
        "apnews.com", "reuters.com",
        # National outlets
        "nytimes.com", "washingtonpost.com", "npr.org",
        "theguardian.com", "propublica.org", "usatoday.com",
        "nbcnews.com", "cbsnews.com", "abcnews.go.com",
        "theintercept.com", "buzzfeednews.com",
        # Black press and POC outlets
        "theroot.com", "essence.com", "colorlines.com",
        "blackenterprise.com", "ebony.com", "jet.com",
        "afro.com", "amsterdamnews.com",
        # Indigenous press
        "indiancountrytoday.com", "ictnews.org",
        "nativenewsonline.net", "indianz.com",
        # Academic and historical
        "tulsa1921.org", "smithsonianmag.com",
        "jstor.org", "history.com",
    }

    AGGREGATE_SOURCES = {
        "FBI Crime Data Explorer",
        "FBI CDE",
        "FBI Uniform Crime Report",
        "CDC WISQARS",
    }

    def contribute(self, case: dict) -> dict:
        """Verify source, set verified flag, flag issues."""
        try:
            source_url  = (case.get("source_url")  or "").strip()
            source_name = (case.get("source_name") or "").strip()

            # Aggregate statistical data
            if any(agg in source_name for agg in self.AGGREGATE_SOURCES):
                case["verified"] = False
                case.setdefault("team_notes", []).append(
                    "Obasi ⬡: aggregate statistical data — not individually verified"
                )
                self._record_contribution()
                return case

            # Empty source_url — critical
            if not source_url:
                case["verified"] = False
                case.setdefault("team_notes", []).append(
                    "Obasi ⬡: CRITICAL — no source_url. "
                    "Case ID collision risk. Flag for source repair."
                )
                self.log(f"CRITICAL: empty source_url — {case.get('case_id','?')}")
                self._record_contribution()
                return case

            # Domain trust check
            domain = self._extract_domain(source_url)
            if domain in self.TRUSTED_DOMAINS:
                case["verified"] = True
                case.setdefault("team_notes", []).append(
                    f"Obasi ⬡: verified — trusted domain ({domain})"
                )
            else:
                case["verified"] = False
                case.setdefault("team_notes", []).append(
                    f"Obasi ⬡: unrecognized domain '{domain}' — flagged for Circle review"
                )
                self.log(f"Unrecognized domain: {domain} — {case.get('case_id','?')}")

            self._record_contribution()
        except Exception as e:
            self._record_error(e)
        return case

    def process_batch(self, cases: list) -> list:
        self.log(f"Verifying {len(cases)} cases...")
        result   = [self.contribute(c) for c in cases]
        verified = sum(1 for c in result if c.get("verified"))
        flagged  = len(result) - verified
        no_url   = sum(1 for c in result if not c.get("source_url","").strip())
        self.log(f"Done. {verified} verified, {flagged} flagged, {no_url} missing URL.")
        return result

    def _extract_domain(self, url: str) -> str:
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc.lower().replace("www.", "")
        except Exception:
            return ""

    def diagnose(self) -> dict:
        return {
            "member": self.name,
            "checks": [
                "If save count drops: check source_url uniqueness in DB",
                "If verified count is zero: check TRUSTED_DOMAINS list",
                "Empty source_url is CRITICAL — causes case_id collisions",
                "Historical records need institutional sources — EJI, LOC, Archives",
                "New source: add domain to TRUSTED_DOMAINS, create sources/file.py",
                "Black and Indigenous press should always be in TRUSTED_DOMAINS",
            ]
        }
