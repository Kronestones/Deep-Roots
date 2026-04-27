
"""
seed_mpv.py — Import Mapping Police Violence dataset into Deep Roots
"""

import sys
sys.path.insert(0, "/data/data/com.termux/files/home/repo")

import openpyxl
from datetime import datetime, timezone
from repo.database import init_db, save_case, get_case_count

XLSX = "/data/data/com.termux/files/home/repo/mpv_data.xlsx"

def parse_date(val):
    if not val:
        return None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:10], fmt[:len(s[:10])]).strftime("%Y-%m-%d")
        except:
            pass
    return s[:10]

def row_to_case(row):
    name     = str(row[0] or "Name withheld")
    age      = str(row[1] or "")
    gender   = str(row[2] or "")
    race     = str(row[3] or "Unknown")
    date     = parse_date(row[5])
    city     = str(row[7] or "Unknown")
    state    = str(row[8] or "US")
    agency   = str(row[11] or "law enforcement")
    cause    = str(row[13] or "")
    circum   = str(row[14] or "")
    charges  = str(row[16] or "None")
    source   = str(row[17] or "")
    armed    = str(row[19] or "")
    lat      = row[40]
    lng      = row[41]
    mpv_id   = str(row[28] or "")

    if not mpv_id or not source:
        return None

    summary = f"{name}"
    if age and age != "None": summary += f", age {age}"
    if race != "Unknown": summary += f", {race}"
    if gender and gender != "None": summary += f" ({gender})"
    summary += f", killed by {agency}"
    if cause: summary += f" — {cause}"
    if armed and armed != "None": summary += f". {armed}"
    if circum: summary += f". {circum[:200]}"

    return {
        "case_id":          f"MPV-{mpv_id}",
        "record_type":      "police_killing",
        "violence_type":    "police_killing",
        "summary":          summary[:600],
        "date_incident":    date,
        "city":             city,
        "state":            state,
        "lat":              float(lat) if lat else None,
        "lng":              float(lng) if lng else None,
        "source_url":       source,
        "source_name":      "Mapping Police Violence",
        "status":           "charged" if charges != "None" else "reported",
        "is_historical":    False,
        "verified":         True,
        "victim_name":      name,
        "victim_race":      race,
        "victim_age":       age,
        "victim_gender":    gender,
    }

def run():
    print("[MPV] Initializing database...")
    init_db()

    print("[MPV] Loading dataset...")
    wb = openpyxl.load_workbook(XLSX, read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(min_row=2, values_only=True))
    print(f"[MPV] {len(rows)} records found.")

    saved = 0
    skipped = 0
    for i, row in enumerate(rows):
        case = row_to_case(row)
        if not case:
            skipped += 1
            continue
        if save_case(case):
            saved += 1
        if (i + 1) % 500 == 0:
            print(f"[MPV] {i+1}/{len(rows)} processed — {saved} saved...")

    print(f"\n[MPV] Done. {saved} saved, {skipped} skipped.")
    print(f"[MPV] Total in database: {get_case_count()}")

if __name__ == "__main__":
    run()
