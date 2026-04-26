"""
pool.py — Deep Roots Archive Consultant Pool

The consultants support the Circle with domain-specific knowledge.
They are called when the engine needs specialized diagnosis.

Unlike Circle members who run on every scan,
consultants are called only when specific problems arise.

Consultant domains:
  - Source repair (dead feeds, changed URLs, auth failures)
  - Database repair (connection issues, schema problems)
  - Historical data (EJI, Fatal Encounters, MPV import guidance)
  - Indigenous data (Sovereign Bodies, NamUs, tribal sources)
  - Legal data (CourtListener, PACER, federal court systems)

Founded by Krone the Architect
Deep Roots Archive · 2026
"""


class Consultant:
    def __init__(self, name, domain, skills, steps):
        self.name   = name
        self.domain = domain
        self.skills = skills
        self.steps  = steps

    def diagnose(self, problem: str) -> list:
        if any(skill.lower() in problem.lower() for skill in self.skills):
            return self.steps
        return []

    def report(self) -> dict:
        return {
            "name":   self.name,
            "domain": self.domain,
            "skills": self.skills,
        }


CONSULTANTS = [
    Consultant(
        name   = "Soleil",
        domain = "Source Repair",
        skills = ["CourtListener", "AP RSS", "DOJ", "feed", "RSS", "source"],
        steps  = [
            "Check feed URL has not changed — test in browser first",
            "Check auth token is set in env var and passed in header",
            "CourtListener: Authorization: Token {COURTLISTENER_TOKEN}",
            "DOJ RSS: no auth required — check URL format",
            "AP RSS: check feeds.apnews.com subdomain still resolves",
            "Test individual source: python3 -c 'from repo.scanner import fetch_ap_rss; print(fetch_ap_rss())'",
        ]
    ),
    Consultant(
        name   = "Cael",
        domain = "Database Repair",
        skills = ["database", "DB", "neon", "connection", "save", "postgresql"],
        steps  = [
            "Check DATABASE_URL is set in Render environment variables",
            "Check Neon dashboard — is DB active and accepting connections?",
            "Test connection: python3 -c 'from repo.database import init_db; init_db()'",
            "Check pool settings: pool_pre_ping=True, pool_recycle=300",
            "If connection refused: check SSL mode — must be sslmode=require",
            "If save rate low: SELECT case_id, COUNT(*) FROM cases GROUP BY case_id HAVING COUNT(*) > 1",
        ]
    ),
    Consultant(
        name   = "Ida",
        domain = "Historical Data",
        skills = ["EJI", "fatal encounters", "mapping police violence", "historical", "import", "CSV"],
        steps  = [
            "Fatal Encounters: download CSV from fatalencounters.org Google Sheet",
            "Save to ~/repo/data/fatal_encounters.csv",
            "Run import script: python3 import_fatal_encounters.py",
            "Mapping Police Violence: download XLSX from mappingpoliceviolence.us",
            "Save to ~/repo/data/mpv.xlsx",
            "Run import script: python3 import_mpv.py",
            "EJI lynching data: available at eji.org/reports — manual entry or scrape",
            "All historical imports should set is_historical=True",
        ]
    ),
    Consultant(
        name   = "Tokala",
        domain = "Indigenous Data",
        skills = ["indigenous", "MMIW", "MMIP", "sovereign bodies", "namus", "tribal", "native"],
        steps  = [
            "Sovereign Bodies Institute: sovereignbodies.org/data — CSV download",
            "Save to ~/repo/data/sovereign_bodies.csv",
            "NamUs: namus.nij.ojp.gov — requires registration for API",
            "Filter NamUs for Indigenous/Native American victims only",
            "All MMIW records: set record_type=mmiw, is_historical=False",
            "Indian Country Today RSS: ictnews.org/feed — no key required",
            "Add ictnews.org to Obasi's TRUSTED_DOMAINS",
            "MMIW data is ongoing — never mark as historical",
        ]
    ),
    Consultant(
        name   = "Marcus",
        domain = "Legal Data",
        skills = ["CourtListener", "PACER", "court", "legal", "civil rights", "section 1983"],
        steps  = [
            "CourtListener token: register at courtlistener.com/sign-in/",
            "Set: export COURTLISTENER_TOKEN='your_token' in ~/.bashrc",
            "Query civil rights cases: type=o, q='section 1983 excessive force'",
            "CourtListener endpoint: /api/rest/v4/search/ — not v3",
            "PACER requires registration — use CourtListener as proxy",
            "DOJ civil rights: justice.gov/crt — press releases, no key needed",
            "Federal cases only — state court records not in CourtListener",
        ]
    ),
    Consultant(
        name   = "Wren",
        domain = "Render & Deployment",
        skills = ["render", "deploy", "restart", "service", "environment", "build"],
        steps  = [
            "Check Render dashboard: dashboard.render.com",
            "Manual deploy: Render dashboard > service > Manual Deploy",
            "Environment variables: Render dashboard > service > Environment",
            "Required vars: DATABASE_URL, SECRET_KEY",
            "Build command: pip install -r requirements.txt",
            "Start command: gunicorn wsgi:app",
            "Free tier sleeps after 15 min — add /health endpoint",
            "Check build logs for import errors before checking runtime logs",
        ]
    ),
]


class ConsultantPool:

    def __init__(self):
        self.consultants = CONSULTANTS
        print(
            f"  [◈ POOL] {len(self.consultants)} consultants available: "
            f"{', '.join(c.name for c in self.consultants)}"
        )

    def __len__(self):
        return len(self.consultants)

    def diagnose_source(self, problem: str) -> list:
        """Find relevant consultants for a problem and return their steps."""
        all_steps = []
        for c in self.consultants:
            steps = c.diagnose(problem)
            if steps:
                all_steps.extend(steps)
        return all_steps

    def coverage_report(self) -> list:
        return [c.report() for c in self.consultants]

    def get(self, name: str):
        for c in self.consultants:
            if c.name.lower() == name.lower():
                return c
        return None
