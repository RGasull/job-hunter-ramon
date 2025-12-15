import requests
from bs4 import BeautifulSoup

BASE_URLS = {
    "Brazil": "https://www.michaelpage.com.br/jobs",
    "Spain": "https://www.michaelpage.es/jobs"
}

def fetch_michael_page(country="Brazil", limit=20):
    url = BASE_URLS.get(country)
    if not url:
        return []

    r = requests.get(url, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = []

    for card in soup.select("div.job-search-results__item")[:limit]:
        title = card.select_one("h3").get_text(strip=True)
        link = "https://www.michaelpage.com" + card.select_one("a")["href"]
        location = card.select_one(".job-location").get_text(strip=True)

        jobs.append({
            "id": f"mp-{link}",
            "source": "Michael Page",
            "title": title,
            "company": "Michael Page",
            "location": location,
            "country": country,
            "url": link,
            "modality": "",
            "contract": "",
            "description": ""
        })

    return jobs
