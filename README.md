# Product Spider

A full-stack web scraping tool — FastAPI backend + Scrapy spider + browser UI.

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start the server
python server.py

# 3. Open your browser
# http://localhost:8000
```

The UI is served at `http://localhost:8000`.

## Project structure

```
product_spider/
├── server.py                   ← FastAPI server + SSE streaming
├── run_scrapy.py               ← Scrapy subprocess runner
├── index.html                  ← Frontend UI
├── requirements.txt
└── spider_project/
    ├── settings.py             ← Scrapy settings
    └── spiders/
        └── product_spider.py   ← The Scrapy spider
```

## How it works

1. You fill in the search query and settings in the browser UI.
2. The UI posts a job to `POST /api/jobs`.
3. The server fetches Google results (via SerpAPI if a key is provided, or directly otherwise).
4. A Scrapy spider crawls each URL, detects product pages by buy signals, and extracts structured data.
5. Results stream back to the browser in real time via Server-Sent Events (`GET /api/jobs/{id}/stream`).
6. You can export results as CSV or JSON.

## SerpAPI

Sign up at https://serpapi.com (free tier: 100 searches/month).
Paste your key into the UI. Without a key, the spider falls back to scraping Google directly — this may get blocked.

## Tips

- Start with 1–2 Google pages for testing.
- Use "gentle" threads (5–10) to avoid rate limiting.
- Add specific buy signals for your niche (e.g. "add to basket" for UK sites).
- Add review/blog domains to the skip list to filter noise.
