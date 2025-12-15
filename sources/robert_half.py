import requests
from bs4 import BeautifulSoup

BASE_URLS = {
    "Brazil": "https://www.roberthalf.com.br/trabalhos",
    "Spain": "https://www.roberthalf.es/trabajos"
}

def fetch_robert_half(country="Brazil", limit=20):
    url = BASE_URLS.get(country)
    if not url:
        return []

    r = requests.get(url, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = []

    for card in soup.select("div.job-card")[:limit]:
        title = card.select_one("h2").get_text(strip=True)
        link = card.select_one("a")["href"]
        location = card.select_one(".job-location").get_text(strip=True)

        jobs.append({
            "id": f"rh-{link}",
            "source": "Robert Half",
            "title": title,
            "company": "Robert Half",
            "location": location,
            "country": country,
            "url": link,
            "modality": "",
            "contract": "",
            "description": ""
        })

    return jobs
