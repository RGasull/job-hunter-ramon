#!/usr/bin/env python3
"""
job_fetcher.py
Busca vagas e envia um e-mail diário com as top 3 + lista por país.
Configurar via environment variables (recommended) or config.json.
"""

import os
import json
import sqlite3
import smtplib
import ssl
import time
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup
import re

# -------------------------
# Config (edit or use env)
# -------------------------
CONFIG = {
    "locations": ["Brazil", "Spain", "Argentina", "Chile", "Colombia", "Peru", "Mexico"],
    "languages": ["pt", "es", "ca"],  # português, espanhol, catalão
    "contracts": ["CLT", "PJ"],
    "modalities": ["Presencial", "Híbrida", "Remota"],
    "min_salary_brl": 14000,
    "keywords": ["governança", "gestão de mudanças", "project controls", "PMO", "CAPEX", "FEL", "AACE", "scope", "escopo", "PPM", "Orion"],
    "fetch_limit_per_source": 30,
    "email": {
        "from": "seu@dominio.com",
        "to": ["seu@dominio.com"],
        "subject_prefix": "[Vagas]"
    },
    "adzuana": {
        "app_id": os.getenv("ADZUNA_APP_ID", ""),
        "app_key": os.getenv("ADZUNA_APP_KEY", "")
    },
    "jooble": {
        "api_key": os.getenv("JOOBLE_API_KEY", "")
    },
    # SMTP settings (SendGrid or your provider)
    "smtp": {
        "host": os.getenv("SMTP_HOST", "smtp.sendgrid.net"),
        "port": int(os.getenv("SMTP_PORT", "587")),
        "user": os.getenv("SMTP_USER", ""),
        "password": os.getenv("SMTP_PASS", "")
    }
}

# -------------------------
# Simple DB to avoid duplicates
# -------------------------
DB_PATH = os.getenv("JOB_DB_PATH", "jobs.db")

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
        c.execute("INSERT INTO seen_jobs (id, source, url, title) VALUES (?,?,?,?)", (job_id, source, url, title))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()

# -------------------------
# Helpers: scoring and normalization
# -------------------------
def score_job(job: Dict[str,Any], config=CONFIG) -> float:
    """Return a heuristic score for ranking."""
    score = 0.0
    title = job.get("title","").lower()
    desc = job.get("description","").lower()
    salary = job.get("salary", 0) or 0
    for kw in config["keywords"]:
        if kw.lower() in title or kw.lower() in desc:
            score += 2.0
    if any(lang in job.get("language","").lower() for lang in config["languages"]):
        score += 1.0
    if job.get("contract") and any(c.lower() in job.get("contract","").lower() for c in config["contracts"]):
        score += 0.5
    if job.get("modality") and any(m.lower() in job.get("modality","").lower() for m in config["modalities"]):
        score += 0.5
    # salary normalization: convert local salary to BRL is complex -- assume numeric in BRL when present
    try:
        if salary and float(salary) >= config["min_salary_brl"]:
            score += 2.0
    except Exception:
        pass
    # older jobs slightly lower
    published_at = job.get("published_at")
    if published_at:
        try:
            dt = datetime.fromisoformat(published_at)
            days = (datetime.now() - dt).days
            score -= min(days * 0.05, 1.0)
        except Exception:
            pass
    return score

# -------------------------
# Source: Adzuna example
# -------------------------
def fetch_adzuna(country_code="br", what=None, where=None, limit=20):
    """
    Adzuna example. Country codes: 'br', 'es', 'ar', etc.
    Requires ADZUNA_APP_ID and ADZUNA_APP_KEY.
    """
    app_id = CONFIG["adzuana"]["app_id"]
    app_key = CONFIG["adzuana"]["app_key"]
    if not app_id or not app_key:
        return []
    base = f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/1"
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": limit,
        "what": what or " ".join(CONFIG["keywords"]),
        "where": where or ""
    }
    try:
        r = requests.get(base, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        results = []
        for item in data.get("results", []):
            job = {
                "id": f"adzuna-{item.get('id')}",
                "source": "adzuna",
                "title": item.get("title"),
                "company": item.get("company", {}).get("display_name"),
                "location": item.get("location", {}).get("display_name"),
                "description": item.get("description"),
                "url": item.get("redirect_url"),
                "salary": item.get("salary_max") or item.get("salary_min"),
                "published_at": item.get("created"),
                "language": "",  # Adzuna doesn't provide language
                "contract": "",
                "modality": ""
            }
            results.append(job)
        return results
    except Exception as e:
        print("Adzuna error:", e)
        return []

# -------------------------
# Source: Jooble example
# -------------------------
def fetch_jooble(country="br", keywords=None, limit=20):
    api_key = CONFIG["jooble"]["api_key"]
    if not api_key:
        return []
    url = "https://jooble.org/api/"
    payload = {
        "keywords": keywords or " ".join(CONFIG["keywords"]),
        "location": country,
        "page": 1
    }
    headers = {"Content-Type": "application/json"}
    try:
        r = requests.post(url + api_key, json=payload, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        results = []
        for item in data.get("jobs", [])[:limit]:
            job = {
                "id": f"jooble-{hash(item.get('link','') )}",
                "source": "jooble",
                "title": item.get("title"),
                "company": item.get("company"),
                "location": item.get("location"),
                "description": item.get("snippet") or item.get("description"),
                "url": item.get("link"),
                "salary": item.get("salary"),
                "published_at": item.get("date"),
                "language": "",
                "contract": "",
                "modality": ""
            }
            results.append(job)
        return results
    except Exception as e:
        print("Jooble error:", e)
        return []

# -------------------------
# Example fallback scraping (simple) - adapt per site
# -------------------------
def fetch_remoteok(keywords=None, limit=20):
    # RemoteOK provides JSON; but respects robots. This is illustrative.
    try:
        r = requests.get("https://remoteok.com/remote-jobs.json", timeout=15)
        data = r.json()
        results = []
        for item in data[1:limit+1]:
            title = item.get("position") or item.get("title")
            description = item.get("description","")
            results.append({
                "id": f"remoteok-{item.get('id')}",
                "source": "remoteok",
                "title": title,
                "company": item.get("company"),
                "location": item.get("location"),
                "description": description,
                "url": item.get("url"),
                "salary": None,
                "published_at": item.get("date"),
                "language": "en",
                "contract": "",
                "modality": "Remota"
            })
        return results
    except Exception as e:
        print("RemoteOK error:", e)
        return []

# -------------------------
# Aggregator
# -------------------------
def aggregate_jobs():
    all_jobs = []
    # For each target country, fetch from sources
    # Map country names to adzuna codes (example)
    adzuna_map = {"Brazil":"br", "Spain":"es", "Argentina":"ar", "Chile":"cl", "Colombia":"co", "Peru":"pe", "Mexico":"mx"}
    for country in CONFIG["locations"]:
        # Adzuna
        code = adzuna_map.get(country, "us")
        adz = fetch_adzuna(country_code=code, limit=CONFIG["fetch_limit_per_source"])
        for j in adz:
            j["country"] = country
            all_jobs.append(j)
        # Jooble
        jb = fetch_jooble(country=country, keywords=" ".join(CONFIG["keywords"]), limit=CONFIG["fetch_limit_per_source"])
        for j in jb:
            j["country"] = country
            all_jobs.append(j)
    # Add remote/global sources
    all_jobs += fetch_remoteok(limit=30)
    # de-duplicate by url/title
    seen_urls = set()
    unique = []
    for j in all_jobs:
        url = j.get("url") or j.get("title")
        if url and url in seen_urls:
            continue
        seen_urls.add(url)
        unique.append(j)
    # filter/score
    scored = []
    for j in unique:
        s = score_job(j)
        j["_score"] = s
        scored.append(j)
    scored.sort(key=lambda x: x["_score"], reverse=True)
    return scored

# -------------------------
# Email report
# -------------------------
def build_email_html(jobs: List[Dict[str,Any]]):
    # Top 3 detailed
    top3 = jobs[:3]
    rest = jobs[3:]
    html = []
    html.append(f"<h2>Relatório diário de vagas — {datetime.now().strftime('%Y-%m-%d')}</h2>")
    html.append("<h3>Top 3 mais aderentes</h3>")
    for j in top3:
        html.append(f"<b>{j.get('title')}</b> — {j.get('company') or ''} — <i>{j.get('country')}</i><br>")
        html.append(f"Local: {j.get('location')} | Modalidade: {j.get('modality') or '—'} | Contrato: {j.get('contract') or '—'}<br>")
        html.append(f"Salário: {j.get('salary') or '—'} | Score: {j.get('_score'):.2f}<br>")
        html.append(f"<a href='{j.get('url')}'>Link da vaga</a><br>")
        html.append(f"<p>{(j.get('description') or '')[:500]}...</p><hr>")
    # Group rest by country
    html.append("<h3>Outras vagas por país</h3>")
    by_country = {}
    for j in rest:
        c = j.get("country", "Global")
        by_country.setdefault(c, []).append(j)
    for country, items in by_country.items():
        html.append(f"<h4>{country} — {len(items)} vagas</h4><ul>")
        for it in items[:20]:  # show first 20 por país
            html.append(f"<li><a href='{it.get('url')}'>{it.get('title')}</a> — {it.get('company') or ''} — {it.get('location')} — Score: {it.get('_score'):.2f}</li>")
        html.append("</ul>")
    html.append("<p>Fim do relatório.</p>")
    return "\n".join(html)

def send_email(subject, html_body):
    import os
    import ssl
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.utils import formataddr

    email_cfg = CONFIG["email"]
    smtp_cfg = CONFIG["smtp"]

    # Senha vem do GitHub Secret
    smtp_password = os.environ["SMTP_PASS"]

    # Monta mensagem
    msg = MIMEMultipart("alternative")

    subject_prefix = email_cfg.get("subject_prefix", "")
    msg["Subject"] = f"{subject_prefix}{subject}".strip()

    from_name = email_cfg.get("from_name", "")
    from_email = email_cfg["from"]

    from_addr = formataddr((from_name, from_email))

    msg["From"] = from_addr
    msg["To"] = ", ".join(email_cfg["to"])

    msg.attach(MIMEText(html_body, "html", "utf-8"))

    # Envio SMTP (SendGrid exige envelope = sender verificado)
    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_cfg["host"], smtp_cfg["port"]) as server:
        server.starttls(context=context)
        server.login(smtp_cfg["user"], smtp_password)
        server.sendmail(
            from_email,              # envelope sender
            email_cfg["to"],
            msg.as_string()
        )



# -------------------------
# Main
# -------------------------
def main():
    init_db()
    print("Fetching jobs...")
    jobs = aggregate_jobs()
    # Mark seen and filter duplicates
    new_jobs = []
    for j in jobs:
        jid = j.get("id") or re.sub(r'[^a-z0-9]','', j.get("url","")[:100].lower())
        source = j.get("source","unknown")
        if seen(jid, source):
            continue
        mark_seen(jid, source, j.get("url"), j.get("title"))
        new_jobs.append(j)
    if not new_jobs:
        print("Nenhuma nova vaga encontrada hoje.")
    else:
        print(f"Encontradas {len(new_jobs)} vagas novas. Preparando email...")
        html = build_email_html(new_jobs)
        send_email(f"Vagas {datetime.now().strftime('%Y-%m-%d')}", html)
        print("Email enviado.")

if __name__ == "__main__":
    main()
