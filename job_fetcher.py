#!/usr/bin/env python3
"""
job_fetcher.py
Busca vagas e envia e-mails:
- Diário: somente Brasil
- Semanal: Internacional (Catalunha, LATAM, ES/PT)
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
    "keywords": [
        "governança", "gestão de mudanças", "project controls", "PMO",
        "CAPEX", "FEL", "AACE", "escopo", "scope", "PPM", "Orion"
    ],
    "fetch_limit_per_source": 30,
    "email": {
        "from": "gasull.ramon@gmail.com",
        "to": ["gasull.ramon@gmail.com"]
    },
    "adzuna": {
        "app_id": os.getenv("ADZUNA_APP_ID", ""),
        "app_key": os.getenv("ADZUNA_APP_KEY", "")
    }
}

DB_PATH = "jobs.db"


# -------------------------
# DB
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


def seen(job_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM seen_jobs WHERE id=?", (job_id,))
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

    if job.get("country") == "Brazil":
        score += 2.0

    if job.get("language") in CONFIG["languages"]:
        score += 1.0

    return score


# -------------------------
# FETCHERS (⚠️ ANTES DO AGGREGATE)
# -------------------------
def fetch_adzuna(country_code="br", limit=20):
    app_id = CONFIG["adzuna"]["app_id"]
    app_key = CONFIG["adzuna"]["app_key"]

    if not app_id or not app_key:
        return []

    url = f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/1"
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": limit,
        "what": " ".join(CONFIG["keywords"])
    }

    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    jobs = []
    for item in data.get("results", []):
        jobs.append({
            "id": f"adzuna-{item.get('id')}",
            "source": "adzuna",
            "title": item.get("title"),
            "company": item.get("company", {}).get("display_name"),
            "location": item.get("location", {}).get("display_name"),
            "description": item.get("description"),
            "url": item.get("redirect_url"),
            "country": country_code.upper(),
            "language": ""
        })
    return jobs


# -------------------------
# AGGREGATOR (AGORA NO LUGAR CERTO)
# -------------------------
def aggregate_jobs():
    all_jobs = []

    adzuna_map = {
        "Brazil": "br",
        "Spain": "es",
        "Argentina": "ar",
        "Chile": "cl",
        "Colombia": "co",
        "Peru": "pe",
        "Mexico": "mx"
    }

    for country, code in adzuna_map.items():
        jobs = fetch_adzuna(country_code=code, limit=CONFIG["fetch_limit_per_source"])
        for j in jobs:
            j["country"] = country
            all_jobs.append(j)

    unique = {}
    for j in all_jobs:
        unique[j["id"]] = j

    jobs = list(unique.values())

    for j in jobs:
        j["_score"] = score_job(j)

    jobs.sort(key=lambda x: x["_score"], reverse=True)
    return jobs


# -------------------------
# EMAIL
# -------------------------
def send_email(subject: str, html: str):
    api_key = os.getenv("SENDGRID_API_KEY")
    if not api_key:
        raise RuntimeError("SENDGRID_API_KEY ausente")

    msg = Mail(
        from_email=CONFIG["email"]["from"],
        to_emails=CONFIG["email"]["to"][0],
        subject=subject,
        html_content=html
    )

    sg = SendGridAPIClient(api_key)
    sg.send(msg)


def build_html(title: str, jobs: List[Dict[str, Any]]):
    html = [f"<h2>{title}</h2><ul>"]
    for j in jobs[:25]:
        html.append(
            f"<li><a href='{j['url']}'>{j['title']}</a> — "
            f"{j.get('company','')} — Score {j['_score']:.1f}</li>"
        )
    html.append("</ul>")
    return "\n".join(html)


# -------------------------
# MAIN
# -------------------------
def main():
    init_db()
    jobs = aggregate_jobs()

    new_jobs = []
    for j in jobs:
        if not seen(j["id"]):
            mark_seen(j["id"], j["source"], j["url"], j["title"])
            new_jobs.append(j)

    brazil = [j for j in new_jobs if j["country"] == "Brazil"]
    international = [j for j in new_jobs if j["country"] != "Brazil"]

    if brazil:
        send_email(
            f"Vagas Brasil — {datetime.now().strftime('%Y-%m-%d')}",
            build_html("Brasil", brazil)
        )

    if international:
        send_email(
            "Vagas Internacionais (Semanal)",
            build_html("Internacional", international)
        )


if __name__ == "__main__":
    main()
