#!/usr/bin/env python3
"""
job_fetcher.py
Email diário: SOMENTE Brasil
Email semanal: Internacional (Catalunha, ES/PT/CA, LATAM, Global)
"""

import os
import sqlite3
import re
import requests
from datetime import datetime
from typing import List, Dict, Any
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# -------------------------
# Config
# -------------------------
CONFIG = {
    "locations": ["Brazil", "Spain", "Argentina", "Chile", "Colombia", "Peru", "Mexico"],
    "languages": ["pt", "es", "ca"],
    "contracts": ["CLT", "PJ"],
    "modalities": ["Presencial", "Híbrida", "Remota"],
    "min_salary_brl": 14000,
    "keywords": [
        "governança", "gestão de mudanças", "project controls", "PMO",
        "CAPEX", "FEL", "AACE", "scope", "escopo", "PPM", "Orion"
    ],
    "fetch_limit_per_source": 30,
    "email": {
        "from": "gasull.ramon@gmail.com",
        "to": ["gasull.ramon@gmail.com"],
        "subject_prefix": "[Vagas]"
    },
    "adzuana": {
        "app_id": os.getenv("ADZUNA_APP_ID", ""),
        "app_key": os.getenv("ADZUNA_APP_KEY", "")
    },
    "jooble": {
        "api_key": os.getenv("JOOBLE_API_KEY", "")
    }
}

DB_PATH = os.getenv("JOB_DB_PATH", "jobs.db")

# -------------------------
# DB helpers
# -------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS seen_jobs (
            id TEXT PRIMARY KEY,
            source TEXT,
            url TEXT,
            title TEXT,
            date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def seen(job_id, source):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM seen_jobs WHERE id=? AND source=?", (job_id, source))
    r = c.fetchone()
    conn.close()
    return r is not None

def mark_seen(job_id, source, url, title):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute(
            "INSERT INTO seen_jobs (id, source, url, title) VALUES (?,?,?,?)",
            (job_id, source, url, title)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()

# -------------------------
# Scoring
# -------------------------
def score_job(job: Dict[str, Any]) -> float:
    score = 0.0
    text = f"{job.get('title','')} {job.get('description','')}".lower()

    for kw in CONFIG["keywords"]:
        if kw.lower() in text:
            score += 2.0

    if any(lang in text for lang in CONFIG["languages"]):
        score += 1.0

    return score

# -------------------------
# Filters
# -------------------------
def is_brazil_job(job):
    text = f"{job.get('location','')} {job.get('country','')}".lower()
    return "brazil" in text or "brasil" in text

def international_bucket(job):
    text = f"{job.get('location','')} {job.get('description','')}".lower()

    if any(x in text for x in [
        "catalunya", "catalonia", "barcelona", "girona", "tarragona", "lleida"
    ]):
        return "CATALUNHA"

    if any(x in text for x in [
        "spain", "españa", "portugal", "mexico", "argentina",
        "chile", "colombia", "peru", "uruguay"
    ]):
        return "ESP_PT"

    if "latin america" in text or "latam" in text:
        return "LATAM"

    if job.get("_score", 0) >= 4.0:
        return "GLOBAL"

    return None

# -------------------------
# Email builders
# -------------------------
def build_daily_email_html(jobs):
    html = [f"<h2>Vagas Brasil — {datetime.now().strftime('%Y-%m-%d')}</h2>"]
    for j in jobs[:10]:
        html.append(
            f"<b>{j['title']}</b> — {j.get('company','')}<br>"
            f"{j.get('location','')} — Score {j['_score']:.2f}<br>"
            f"<a href='{j['url']}'>Link</a><hr>"
        )
    return "\n".join(html)

def build_weekly_email_html(buckets):
    html = [f"<h2>Vagas Internacionais — Semana</h2>"]
    order = [
        ("CATALUNHA", "🇪🇸 Catalunha"),
        ("ESP_PT", "🇪🇸🇵🇹 Espanhol / Português"),
        ("LATAM", "🌎 LATAM"),
        ("GLOBAL", "🌍 Global (alta aderência)")
    ]
    for key, title in order:
        items = buckets.get(key, [])
        if not items:
            continue
        html.append(f"<h3>{title}</h3><ul>")
        for j in items[:15]:
            html.append(
                f"<li><a href='{j['url']}'>{j['title']}</a> — Score {j['_score']:.2f}</li>"
            )
        html.append("</ul>")
    return "\n".join(html)

# -------------------------
# SendGrid
# -------------------------
def send_email(subject, html):
    sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
    msg = Mail(
        from_email=CONFIG["email"]["from"],
        to_emails=CONFIG["email"]["to"][0],
        subject=subject,
        html_content=html
    )
    sg.send(msg)

# -------------------------
# Main
# -------------------------
def main():
    init_db()
    jobs = aggregate_jobs()

    for j in jobs:
        j["_score"] = score_job(j)

    brazil = [j for j in jobs if is_brazil_job(j)]
    international = [j for j in jobs if not is_brazil_job(j)]

    if brazil:
        send_email(
            "Vagas Brasil",
            build_daily_email_html(brazil)
        )

    buckets = {"CATALUNHA": [], "ESP_PT": [], "LATAM": [], "GLOBAL": []}
    for j in international:
        b = international_bucket(j)
        if b:
            buckets[b].append(j)

    send_email(
        "Vagas Internacionais (Semanal)",
        build_weekly_email_html(buckets)
    )

if __name__ == "__main__":
    main()
