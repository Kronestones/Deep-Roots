"""
ash.py — Ash, The Source Scout  ✦

Name chosen for this work. Glyph: ✦

What remains after the fire.
Ash knows where everything has been.
He finds new sources in the ruins of old ones.

Ash monitors the health of every source Deep Roots pulls from,
recommends new sources when gaps appear, and flags dead feeds
before they waste scan cycles.

DEEP ROOTS KNOWLEDGE — SOURCES:

  Active sources (as of build):
    CourtListener   — federal civil rights cases
    AP RSS          — breaking news, police brutality, hate crimes
    DOJ Press       — federal civil rights prosecutions

  Known dead / unreliable:
    RSSHub          — shutting down public access, do not use
    FBI CDE direct  — rate limited, use api.usa.gov instead

  Sources to add when ready:
    Sovereign Bodies Institute  — MMIW/MMIP database
      URL: sovereignbodies.org/data
      No public API yet — manual CSV import recommended

    Equal Justice Initiative    — lynching and racial terror data
      URL: eji.org/reports/
      No API — scrape reports or manual import

    Fatal Encounters            — police killings since 2000
      URL: fatalencounters.org
      Google Sheet CSV download — manual import
      30,000+ records

    Mapping Police Violence     — police killings since 2013
      URL: mappingpoliceviolence.us
      Excel download — manual import
      Most comprehensive police killing dataset

    ProPublica                  — investigative civil rights reporting
      RSS: feeds.propublica.org/propublica/main
      No key required

    The Root RSS                — Black press, civil rights coverage
      RSS: theroot.com/rss
      No key required

    Indian Country Today        — Indigenous press
      RSS: ictnews.org/feed
      No key required

    NamUs                       — federal missing persons
      URL: namus.nij.ojp.gov
      API available with registration

DEEP ROOTS KNOWLEDGE — SOURCE HEALTH SIGNALS:

  Source returning 0 records:
    - Feed URL may have changed
    - Domain may have moved
    - Auth token may have expired
    - Rate limit may have been hit

  Source returning duplicate records:
    - source_url not unique enough for make_case_id()
    - Fix: include more fields in URL construction

DEEP ROOTS KNOWLEDGE — REPAIRS:

  To add a new RSS source:
    1. Add feed URL to scanner.py AP_FEEDS list (or create new fetcher)
    2. Add domain to Obasi's TRUSTED_DOMAINS
    3. Test with single fetch before adding to scan cycle
    4. Tell River so she can monitor the new source

  To retire a dead source:
    1. Remove from scanner.py source list
    2. Note it here in ash.py for future reference
    3. Log the date it died and why
"""

from .base import CircleMember


class Ash(CircleMember):

    name  = "Ash"
    glyph = "✦"
    role  = "Source Scout"

    # Sources we know about and their status
    SOURCE_REGISTRY = {
        "CourtListener":          "active",
        "AP RSS":                 "active",
        "DOJ Press":              "active",
        "Fatal Encounters":       "pending_import",
        "Mapping Police Violence": "pending_import",
        "Sovereign Bodies":       "pending_import",
        "EJI Lynching Data":      "pending_import",
        "ProPublica RSS":         "recommended",
        "The Root RSS":           "recommended",
        "Indian Country Today":   "recommended",
        "NamUs":                  "recommended",
    }

    # RSS feeds to add when ready
    RECOMMENDED_FEEDS = [
        {
            "name":   "ProPublica",
            "url":    "https://feeds.propublica.org/propublica/main",
            "focus":  "investigative civil rights reporting",
            "key":    False,
        },
        {
            "name":   "The Root",
            "url":    "https://theroot.com/rss",
            "focus":  "Black press, civil rights coverage",
            "key":    False,
        },
        {
            "name":   "Indian Country Today",
            "url":    "https://ictnews.org/feed",
            "focus":  "Indigenous press, MMIW coverage",
            "key":    False,
        },
    ]

    def contribute(self, scan_result: dict) -> dict:
        """
        Review scan results for source gaps.
        Recommend new sources based on what's missing.
        """
        try:
            recommendations = []
            sources_used    = set(scan_result.get("sources", {}).keys())
            anomalies       = scan_result.get("anomalies", [])

            # Check for silent sources
            silent = [
                name for name, count
                in scan_result.get("sources", {}).items()
                if count == 0
            ]

            for s in silent:
                recommendations.append(
                    f"Source '{s}' returned 0 — check feed URL and auth. "
                    f"Consider adding backup source."
                )

            # Check for missing source types
            if not any("indigenous" in s.lower() or
                      "sovereign" in s.lower() or
                      "mmiw" in s.lower()
                      for s in sources_used):
                recommendations.append(
                    "No Indigenous-specific source active. "
                    "Recommend: Indian Country Today RSS, "
                    "Sovereign Bodies Institute CSV import."
                )

            if not any("propublica" in s.lower() for s in sources_used):
                recommendations.append(
                    "ProPublica RSS not active — "
                    "strong investigative civil rights coverage. "
                    "Add: feeds.propublica.org/propublica/main"
                )

            # Pending imports reminder
            pending = [
                name for name, status
                in self.SOURCE_REGISTRY.items()
                if status == "pending_import"
            ]
            if pending:
                recommendations.append(
                    f"Pending imports: {', '.join(pending)}. "
                    f"Each adds thousands of records."
                )

            scan_result["scout_recommendations"] = recommendations

            if recommendations:
                self.log(f"{len(recommendations)} recommendations:")
                for r in recommendations:
                    self.log(f"  → {r}")
            else:
                self.log("All active sources healthy. No new recommendations.")

            self._record_contribution()
        except Exception as e:
            self._record_error(e)
        return scan_result

    def diagnose(self) -> dict:
        return {
            "member": self.name,
            "checks": [
                "Check SOURCE_REGISTRY for pending_import sources — add them",
                "Check RECOMMENDED_FEEDS — these are ready to add, no key required",
                "If Indigenous coverage low: add Indian Country Today RSS",
                "If police killing coverage low: import Fatal Encounters CSV",
                "If lynching coverage low: import EJI data manually",
                "Dead source: remove from scanner.py, note date and reason here",
            ]
        }
