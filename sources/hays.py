import requests
from bs4 import BeautifulSoup

BASE_URLS = {
    "Brazil": "https://www.hays.com.br/jobs",
    "Spain": "https://www.hays.es/jobs"
}

def fetch_hays(country="Brazil", limit=20):
    url = BASE_URLS.get(country)
    if not url:
        return []

    r = requests.get(url, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = []

    for card in soup.select("article.c-job")[:limit]:
        title = card.select_one("h3").get_text(strip=True)
        link = card.select_one("a")["href"]
        location = card.select_one(".c-job__location").get_text(strip=True)

        jobs.append({
            "id": f"hays-{link}",
            "source": "Hays",
            "title": title,
            "company": "Hays",
            "location": location,
            "country": country,
            "url": link,
            "modality": "",
            "contract": "",
            "description": ""
        })

    return jobs
