"""
scanner.py — Repo

Pulls from public records sources documenting state violence
against Black, Indigenous, and POC communities.

Sources:
    CourtListener  — federal civil rights cases, free REST API
    AP News RSS    — police brutality, hate crimes, civil rights
    DOJ Press      — federal civil rights prosecutions
    MPV            — Mapping Police Violence public data

Founded by Krone the Architect
Repo · 2026
"""

import time
import hashlib
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

# Team engine — imported lazily to avoid circular imports
_engine = None

def _get_engine():
    global _engine
    if _engine is None:
        try:
            from repo.team.engine import TeamEngine
            _engine = TeamEngine()
        except Exception as e:
            print(f"[Repo] Team engine unavailable: {e}")
    return _engine


# ── State coords fallback ─────────────────────────────────────────────────────

STATE_COORDS = {
    "AL": (32.806671, -86.791130), "AK": (61.370716,-152.404419),
    "AZ": (33.729759,-111.431221), "AR": (34.969704, -92.373123),
    "CA": (36.116203,-119.681564), "CO": (39.059811,-105.311104),
    "CT": (41.597782, -72.755371), "DE": (39.318523, -75.507141),
    "FL": (27.766279, -81.686783), "GA": (33.040619, -83.643074),
    "HI": (21.094318,-157.498337), "ID": (44.240459,-114.478828),
    "IL": (40.349457, -88.986137), "IN": (39.849426, -86.258278),
    "IA": (42.011539, -93.210526), "KS": (38.526600, -96.726486),
    "KY": (37.668140, -84.670067), "LA": (31.169960, -91.867805),
    "ME": (44.693947, -69.381927), "MD": (39.063946, -76.802101),
    "MA": (42.230171, -71.530106), "MI": (43.326618, -84.536095),
    "MN": (45.694454, -93.900192), "MS": (32.741646, -89.678696),
    "MO": (38.456085, -92.288368), "MT": (46.921925,-110.454353),
    "NE": (41.125370, -98.268082), "NV": (38.313515,-117.055374),
    "NH": (43.452492, -71.563896), "NJ": (40.298904, -74.521011),
    "NM": (34.840515,-106.248482), "NY": (42.165726, -74.948051),
    "NC": (35.630066, -79.806419), "ND": (47.528912, -99.784012),
    "OH": (40.388783, -82.764915), "OK": (35.565342, -96.928917),
    "OR": (44.572021,-122.070938), "PA": (40.590752, -77.209755),
    "RI": (41.680893, -71.511780), "SC": (33.856892, -80.945007),
    "SD": (44.299782, -99.438828), "TN": (35.747845, -86.692345),
    "TX": (31.054487, -97.563461), "UT": (40.150032,-111.862434),
    "VT": (44.045876, -72.710686), "VA": (37.769337, -78.169968),
    "WA": (47.400902,-121.490494), "WV": (38.491226, -80.954453),
    "WI": (44.268543, -89.616508), "WY": (42.755966,-107.302490),
    "DC": (38.895110, -77.036366),
}

_geocode_cache: dict = {}


def geocode(city: str, state: str) -> tuple:
    key = f"{city},{state}".lower().strip()
    if key in _geocode_cache:
        return _geocode_cache[key]
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q":            f"{city}, {state}, United States",
                "format":       "json",
                "limit":        1,
                "countrycodes": "us",
            },
            headers={"User-Agent": "Repo/1.0 (sentinel.commons@gmail.com)"},
            timeout=5,
        )
        if resp.ok:
            data = resp.json()
            if data:
                lat = float(data[0]["lat"])
                lng = float(data[0]["lon"])
                _geocode_cache[key] = (lat, lng)
                return lat, lng
    except Exception:
        pass
    coords = STATE_COORDS.get(state.upper(), (None, None))
    _geocode_cache[key] = coords
    return coords


def make_case_id(source: str, text: str) -> str:
    h = hashlib.sha1(f"{source}:{text}".encode()).hexdigest()[:8].upper()
    return f"REPO-{h}"


# ── Scope filter ──────────────────────────────────────────────────────────────

_IN_SCOPE_TERMS = [
    "police killing", "police killed", "killed by police",
    "officer-involved", "police brutality", "excessive force",
    "civil rights", "racial violence", "hate crime",
    "lynching", "white supremac", "kkk", "ku klux",
    "police shooting", "unarmed", "black man killed",
    "black woman killed", "indigenous", "native american",
    "missing and murdered", "mmiw", "mmip",
    "george floyd", "breonna taylor", "ahmaud arbery",
    "racial terror", "police misconduct",
]

_OUT_OF_SCOPE = [
    "drug trafficking", "tax fraud", "immigration violation",
    "animal cruelty", "for immediate release", "attorney general announces",
]


def _is_in_scope(record: dict) -> bool:
    text = " ".join([
        str(record.get("title", "")),
        str(record.get("summary", "")),
    ]).lower()
    if any(t in text for t in _OUT_OF_SCOPE):
        return False
    return any(t in text for t in _IN_SCOPE_TERMS)


# ── CourtListener — civil rights cases ───────────────────────────────────────

def fetch_courtlistener() -> list:
    records = []
    queries = [
        "police brutality civil rights",
        "excessive force section 1983",
        "racial violence hate crime",
        "police killing unarmed",
    ]
    for q in queries:
        try:
            resp = requests.get(
                "https://www.courtlistener.com/api/rest/v3/search/",
                params={
                    "q":          q,
                    "type":       "o",
                    "order_by":   "score desc",
                    "stat_Precedential": "on",
                },
                headers={"User-Agent": "Repo/1.0 (sentinel.commons@gmail.com)"},
                timeout=10,
            )
            if not resp.ok:
                continue
            for result in resp.json().get("results", [])[:5]:
                summary = result.get("snippet") or result.get("caseName", "")
                if not summary:
                    continue
                state = "DC"
                records.append({
                    "record_type": "hate_crime",
                    "summary":     summary[:500],
                    "date":        (result.get("dateFiled") or "")[:10],
                    "city":        "Federal",
                    "state":       state,
                    "status":      "charged",
                    "source_url":  f"https://www.courtlistener.com{result.get('absolute_url','')}",
                    "source_name": "CourtListener",
                    "is_historical": False,
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"[Repo] CourtListener error: {e}")
    return records


# ── AP RSS — police brutality / civil rights ──────────────────────────────────

AP_FEEDS = [
    "https://feeds.apnews.com/rss/apf-topnews",
    "https://feeds.apnews.com/rss/apf-usnews",
]

def fetch_ap_rss() -> list:
    records = []
    for feed_url in AP_FEEDS:
        try:
            resp = requests.get(
                feed_url,
                headers={"User-Agent": "Repo/1.0 (sentinel.commons@gmail.com)"},
                timeout=10,
            )
            if not resp.ok:
                continue
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item"):
                title   = item.findtext("title") or ""
                desc    = item.findtext("description") or ""
                link    = item.findtext("link") or ""
                summary = f"{title}. {desc}".strip()
                if not _is_in_scope({"title": title, "summary": desc}):
                    continue
                records.append({
                    "record_type": "police_killing",
                    "summary":     summary[:500],
                    "date":        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "city":        "Unknown",
                    "state":       "US",
                    "status":      "unknown",
                    "source_url":  link,
                    "source_name": "AP News",
                    "is_historical": False,
                })
        except Exception as e:
            print(f"[Repo] AP RSS error: {e}")
    return records


# ── DOJ Press Releases — civil rights ────────────────────────────────────────

DOJ_FEEDS = [
    "https://www.justice.gov/feeds/opa/justice-news.xml",
]

_DOJ_CIVIL_RIGHTS = [
    "civil rights", "police misconduct", "excessive force",
    "hate crime", "racial", "section 1983", "color of law",
]

def fetch_doj() -> list:
    records = []
    for feed_url in DOJ_FEEDS:
        try:
            resp = requests.get(
                feed_url,
                headers={"User-Agent": "Repo/1.0 (sentinel.commons@gmail.com)"},
                timeout=10,
            )
            if not resp.ok:
                continue
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item"):
                title   = item.findtext("title") or ""
                desc    = item.findtext("description") or ""
                link    = item.findtext("link") or ""
                text    = f"{title} {desc}".lower()
                if not any(t in text for t in _DOJ_CIVIL_RIGHTS):
                    continue

                rt = "hate_crime"
                if "police" in text or "excessive force" in text or "color of law" in text:
                    rt = "police_killing"

                records.append({
                    "record_type": rt,
                    "summary":     f"{title}. {desc}".strip()[:500],
                    "date":        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "city":        "Federal",
                    "state":       "DC",
                    "status":      "charged",
                    "source_url":  link,
                    "source_name": "DOJ Office of Public Affairs",
                    "is_historical": False,
                })
        except Exception as e:
            print(f"[Repo] DOJ error: {e}")
    return records


# ── Scanner ───────────────────────────────────────────────────────────────────

class RepoScanner:

    def __init__(self):
        self.last_scan   = None
        self.total_found = 0

    def scan(self) -> list:
        print("[Repo] Starting public records scan...")
        raw = []

        for name, fn in [
            ("CourtListener", fetch_courtlistener),
            ("AP RSS",        fetch_ap_rss),
            ("DOJ",           fetch_doj),
        ]:
            try:
                results = fn()
                print(f"[Repo] {name}: {len(results)} raw records")
                raw.extend(results)
            except Exception as e:
                print(f"[Repo] {name} ERROR: {e}")

        print(f"[Repo] Total raw: {len(raw)}")

        # Deduplicate by summary hash
        seen   = set()
        unique = []
        for r in raw:
            cid = make_case_id(r.get("source_name",""), r.get("summary",""))
            if cid in seen:
                continue
            seen.add(cid)
            r["case_id"] = cid
            unique.append(r)

        print(f"[Repo] After dedup: {len(unique)}")

        # Run team pipeline
        engine = _get_engine()
        if engine:
            try:
                scan_result = {
                    "found":   len(raw),
                    "saved":   0,
                    "sources": {
                        "CourtListener": len([r for r in raw if r.get("source_name") == "CourtListener"]),
                        "AP RSS":        len([r for r in raw if r.get("source_name") == "AP News"]),
                        "DOJ":           len([r for r in raw if r.get("source_name") == "DOJ Office of Public Affairs"]),
                    },
                    "anomalies": [],
                }
                unique, team_report = engine.run(unique, scan_result)
                print(f"[Repo] Team pipeline complete — {len(unique)} cases processed.")
            except Exception as e:
                print(f"[Repo] Team pipeline error: {e}")

        # Geocode
        for r in unique:
            if r.get("city") and r.get("state") and r["state"] != "US":
                lat, lng = geocode(r["city"], r["state"])
                r["lat"] = lat
                r["lng"] = lng
            time.sleep(0.2)

        self.last_scan    = datetime.now(timezone.utc).isoformat()
        self.total_found += len(unique)
        print(f"[Repo] Scan complete. {len(unique)} cases ready.")
        return unique
