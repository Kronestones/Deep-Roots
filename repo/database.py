"""
database.py — Repo

Neon PostgreSQL persistence layer.
Documents state violence against Black, Indigenous, and POC communities.

Record types:
    police_killing  — ongoing, sourced from public records
    lynching        — historical, 1865-1950
    massacre        — Tulsa, Rosewood, Osage, etc.
    mmiw            — missing and murdered Indigenous people
    hate_crime      — documented, court-filed

Founded by Krone the Architect · Powers Tracey Lynn
Repo · 2026
"""

import os
from datetime import datetime, timezone
from sqlalchemy import (
    create_engine, text,
    Column, String, Float, DateTime, Boolean, Integer, Text
)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class RepoCase(Base):
    __tablename__ = "cases"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    case_id         = Column(String(64), unique=True, nullable=False)
    record_type     = Column(String(32), nullable=False)  # police_killing | lynching | massacre | mmiw | hate_crime
    victim_name     = Column(String(256), nullable=True)
    victim_race     = Column(String(128), nullable=True)
    victim_age      = Column(String(32), nullable=True)
    summary         = Column(Text, nullable=True)
    date            = Column(String(32), nullable=True)   # ISO or year only
    city            = Column(String(128), nullable=True)
    state           = Column(String(8), nullable=True)
    lat             = Column(Float, nullable=True)
    lng             = Column(Float, nullable=True)
    status          = Column(String(64), nullable=True)   # no_charges | charged | convicted | unsolved | unknown
    source_url      = Column(Text, nullable=True)
    source_name     = Column(String(256), nullable=True)
    is_historical   = Column(Boolean, default=False)      # True for lynchings/massacres
    verified        = Column(Boolean, default=True)
    created_at      = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


# ── Engine ────────────────────────────────────────────────────────────────────

_engine  = None
_Session = None
_lock    = __import__("threading").Lock()


def get_engine():
    global _engine, _Session
    if _engine is not None:
        return _engine
    with _lock:
        if _engine is not None:
            return _engine
        url = os.environ.get("DATABASE_URL", "")
        if not url:
            raise RuntimeError("DATABASE_URL not set")
        _engine  = create_engine(
            url,
            pool_pre_ping=True,
            pool_recycle=300,
            pool_size=5,
            max_overflow=10,
        )
        _Session = sessionmaker(bind=_engine)
    return _engine


def get_session():
    get_engine()
    return _Session()


def init_db():
    Base.metadata.create_all(get_engine())
    print("[Repo DB] Tables ready.")


# ── Writes ────────────────────────────────────────────────────────────────────

def save_case(case: dict) -> bool:
    session = get_session()
    try:
        exists = session.query(RepoCase).filter_by(
            case_id=case.get("case_id", "")
        ).first()
        if exists:
            return False

        row = RepoCase(
            case_id       = case.get("case_id", ""),
            record_type   = case.get("record_type", "unknown"),
            victim_name   = case.get("victim_name"),
            victim_race   = case.get("victim_race"),
            victim_age    = case.get("victim_age"),
            summary       = case.get("summary"),
            date          = case.get("date"),
            city          = case.get("city"),
            state         = case.get("state"),
            lat           = case.get("lat"),
            lng           = case.get("lng"),
            status        = case.get("status", "unknown"),
            source_url    = case.get("source_url"),
            source_name   = case.get("source_name"),
            is_historical = case.get("is_historical", False),
            verified      = case.get("verified", True),
        )
        session.add(row)
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        print(f"[Repo DB] save_case error: {e}")
        return False
    finally:
        session.close()


# ── Reads ─────────────────────────────────────────────────────────────────────

def get_cases(limit=2000, record_type=None, state=None) -> list:
    session = get_session()
    try:
        q = session.query(RepoCase)
        if record_type:
            q = q.filter_by(record_type=record_type)
        if state:
            q = q.filter_by(state=state.upper())
        rows = q.order_by(RepoCase.created_at.desc()).limit(limit).all()
        return [_to_dict(r) for r in rows]
    except Exception as e:
        print(f"[Repo DB] get_cases error: {e}")
        return []
    finally:
        session.close()


def get_case_count(record_type=None) -> int:
    session = get_session()
    try:
        q = session.query(RepoCase)
        if record_type:
            q = q.filter_by(record_type=record_type)
        return q.count()
    except Exception:
        return 0
    finally:
        session.close()


def get_stats() -> dict:
    session = get_session()
    try:
        total    = session.query(RepoCase).count()
        by_type  = {}
        for rt in ["police_killing","lynching","massacre","mmiw","hate_crime"]:
            by_type[rt] = session.query(RepoCase).filter_by(record_type=rt).count()
        return {"total": total, "by_type": by_type}
    except Exception as e:
        print(f"[Repo DB] get_stats error: {e}")
        return {"total": 0, "by_type": {}}
    finally:
        session.close()


# ── Serializer ────────────────────────────────────────────────────────────────

def _to_dict(row: RepoCase) -> dict:
    return {
        "id":           row.id,
        "case_id":      row.case_id,
        "record_type":  row.record_type,
        "victim_name":  row.victim_name,
        "victim_race":  row.victim_race,
        "victim_age":   row.victim_age,
        "summary":      row.summary,
        "date":         row.date,
        "city":         row.city,
        "state":        row.state,
        "lat":          row.lat,
        "lng":          row.lng,
        "status":       row.status,
        "source_url":   row.source_url,
        "source_name":  row.source_name,
        "is_historical":row.is_historical,
        "verified":     row.verified,
    }
