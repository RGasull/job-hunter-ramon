def score_job(job, config):
    score = 0

    title = (job.get("title") or "").lower()
    desc = (job.get("description") or "").lower()
    country = job.get("country", "")

    keywords = [k.lower() for k in config["keywords"]]

    # Match técnico
    for kw in keywords:
        if kw in title:
            score += 4
        elif kw in desc:
            score += 2

    # Senioridade
    senior_terms = ["senior", "sênior", "lead", "manager", "gerente", "head", "coord"]
    if any(t in title for t in senior_terms):
        score += 4

    # HeadHunter
    if job.get("source") in ["Michael Page", "Hays", "Robert Half"]:
        score += 5

    # País prioritário
    if country == "Brazil":
        score += 4
    elif country == "Spain":
        score += 2

    return score
