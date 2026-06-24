"""
scraper.py — Price & availability comparison engine.
Searches the web for each product from an uploaded catalog file,
extracts competitor pricing and stock, streams results back.
"""
import asyncio
import json
import re
import uuid
from datetime import datetime
from typing import Optional, List

import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter(prefix="/api/scraper")
scrape_jobs: dict = {}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Price patterns: handles $, €, £, plain numbers with decimals/commas
PRICE_RE = re.compile(
    r'(?:[\$€£])\s*[\d][,\d]*\.?\d{0,2}'
    r'|[\d][,\d]*\.?\d{0,2}\s*(?:€|EUR|USD|GBP|DZD|MAD|TND|DA)'
    r'|\b\d{2,6}(?:[.,]\d{2})?\b'
)


# ── Models ────────────────────────────────────────────────────────────────────

class Product(BaseModel):
    title: str
    sku: str = ""
    vendor: str = ""
    barcode: str = ""
    product_type: str = ""
    status: str = ""
    your_price: Optional[float] = None
    your_qty: Optional[int] = None


class ScrapeConfig(BaseModel):
    products: List[Product]
    engine: str = "duckduckgo"
    serpapi_key: Optional[str] = None


class ScrapeJob:
    def __init__(self, job_id: str, config: ScrapeConfig):
        self.job_id = job_id
        self.config = config
        self.events: list = []
        self.results: list = []
        self.done = False
        self.status = "pending"
        self.start_time = datetime.now()

    def push(self, event: dict):
        self.events.append(event)

    def elapsed(self):
        return round((datetime.now() - self.start_time).total_seconds(), 1)


# ── Scraping helpers ──────────────────────────────────────────────────────────

def _extract_price_from_text(text: str) -> Optional[str]:
    """Pull the first plausible price out of arbitrary text."""
    matches = PRICE_RE.findall(text)
    for m in matches:
        m = m.strip()
        # Reject obvious non-prices (years, IDs, etc.)
        raw = re.sub(r'[^\d]', '', m)
        if raw and 10 <= int(raw) <= 9_999_999:
            return m
    return None


def _scrape_page_for_price(url: str) -> dict:
    """Fetch a URL and extract price + availability."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        soup = BeautifulSoup(resp.text, "lxml")

        found_price = None
        found_availability = "Unknown"

        # 1. Schema.org
        el = soup.find(attrs={"itemprop": "price"})
        if el:
            val = el.get("content") or el.get_text()
            found_price = val.strip() if val else None

        # 2. Common CSS classes
        if not found_price:
            for sel in [
                '[class*="price"]', '[id*="price"]',
                '[class*="prix"]', '[id*="prix"]',
                '[class*="amount"]', '[class*="cost"]',
            ]:
                for node in soup.select(sel)[:6]:
                    txt = node.get_text(" ", strip=True)
                    if len(txt) < 40:
                        p = _extract_price_from_text(txt)
                        if p:
                            found_price = p
                            break
                if found_price:
                    break

        # 3. Regex fallback on full page text
        if not found_price:
            found_price = _extract_price_from_text(resp.text[:50_000])

        # Availability
        pg = resp.text.lower()
        if any(x in pg for x in ["in stock", "en stock", "disponible", "available"]):
            found_availability = "In Stock"
        elif any(x in pg for x in ["out of stock", "rupture", "épuisé", "unavailable"]):
            found_availability = "Out of Stock"

        return {"found_price": found_price, "found_availability": found_availability}
    except Exception:
        return {"found_price": None, "found_availability": "Error"}


async def _search_urls(query: str, engine: str, max_results: int = 5) -> list:
    """Return (url, snippet_text) pairs from a search engine."""
    results = []
    try:
        if engine in ("duckduckgo", "all"):
            from ddgs import DDGS
            raw = await asyncio.to_thread(
                lambda: list(DDGS().text(query, max_results=max_results))
            )
            for r in raw:
                results.append({
                    "url": r.get("href", ""),
                    "snippet": r.get("body", "") + " " + r.get("title", ""),
                    "title": r.get("title", ""),
                })
    except Exception:
        pass

    if not results and engine in ("bing", "all"):
        try:
            resp = await asyncio.to_thread(
                lambda: requests.get(
                    "https://www.bing.com/search",
                    params={"q": query, "count": max_results},
                    headers=HEADERS, timeout=10,
                )
            )
            soup = BeautifulSoup(resp.text, "lxml")
            for li in soup.select("li.b_algo")[:max_results]:
                a = li.select_one("h2 a")
                cap = li.select_one(".b_caption p")
                if a:
                    results.append({
                        "url": a.get("href", ""),
                        "snippet": cap.get_text() if cap else "",
                        "title": a.get_text(),
                    })
        except Exception:
            pass

    return results


async def _find_price_for_product(product: Product, engine: str) -> dict:
    """Return best found price + meta for one product."""
    # Build query — SKU is most specific
    if product.sku:
        query = f'"{product.sku}" prix achat OR buy price'
    else:
        query = f'"{product.title}" {product.vendor} prix achat OR price buy'

    results = await _search_urls(query, engine, max_results=6)

    # 1. Try snippet prices first (fast, no extra HTTP)
    for r in results:
        p = _extract_price_from_text(r["snippet"])
        if p:
            return {
                "found_price": p,
                "found_availability": "Unknown",
                "found_url": r["url"],
                "found_source": r["title"],
            }

    # 2. Visit top 3 URLs
    for r in results[:3]:
        url = r["url"]
        if not url.startswith("http"):
            continue
        data = await asyncio.to_thread(_scrape_page_for_price, url)
        if data["found_price"]:
            return {
                "found_price": data["found_price"],
                "found_availability": data["found_availability"],
                "found_url": url,
                "found_source": r["title"],
            }

    return {
        "found_price": None,
        "found_availability": "Not found",
        "found_url": "",
        "found_source": "",
    }


# ── Job runner ────────────────────────────────────────────────────────────────

async def _run_scrape_job(job: ScrapeJob):
    job.status = "running"
    products = job.config.products
    engine = job.config.engine

    job.push({"type": "status", "msg": f"Starting price search for {len(products)} products…"})

    for i, product in enumerate(products):
        job.push({
            "type": "progress",
            "current": i,
            "total": len(products),
            "msg": f"[{i+1}/{len(products)}] Searching: {product.title[:60]}…",
        })

        try:
            data = await _find_price_for_product(product, engine)
        except Exception as e:
            data = {"found_price": None, "found_availability": "Error",
                    "found_url": "", "found_source": str(e)}

        # Compute price delta
        delta = None
        delta_pct = None
        if product.your_price and data["found_price"]:
            raw = re.sub(r'[^\d.,]', '', data["found_price"]).replace(',', '.')
            try:
                fp = float(raw)
                delta = round(fp - product.your_price, 2)
                delta_pct = round((delta / product.your_price) * 100, 1)
            except Exception:
                pass

        result = {
            "title": product.title,
            "sku": product.sku,
            "vendor": product.vendor,
            "product_type": product.product_type,
            "status": product.status,
            "your_price": product.your_price,
            "your_qty": product.your_qty,
            "found_price": data["found_price"],
            "found_availability": data["found_availability"],
            "found_url": data["found_url"],
            "found_source": data["found_source"],
            "delta": delta,
            "delta_pct": delta_pct,
        }
        job.results.append(result)
        job.push({"type": "result", "data": result, "index": i})

        await asyncio.sleep(0.3)  # gentle pacing

    job.status = "done"
    job.push({
        "type": "done",
        "total": len(job.results),
        "elapsed": job.elapsed(),
        "results": job.results,
    })
    job.done = True


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/jobs")
async def create_scrape_job(config: ScrapeConfig):
    if not config.products:
        return {"error": "No products provided"}
    job_id = str(uuid.uuid4())
    job = ScrapeJob(job_id, config)
    scrape_jobs[job_id] = job
    asyncio.create_task(_run_scrape_job(job))
    return {"job_id": job_id, "total": len(config.products)}


@router.get("/jobs/{job_id}/stream")
async def stream_scrape_job(job_id: str):
    from fastapi import HTTPException
    if job_id not in scrape_jobs:
        raise HTTPException(404, "Job not found")
    job = scrape_jobs[job_id]

    async def gen():
        sent = 0
        while True:
            while sent < len(job.events):
                yield f"data: {json.dumps(job.events[sent])}\n\n"
                sent += 1
            if job.done:
                break
            await asyncio.sleep(0.1)
        yield 'data: {"type":"stream_end"}\n\n'

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


@router.get("/jobs/{job_id}/results")
async def get_scrape_results(job_id: str, fmt: str = "json"):
    from fastapi import HTTPException
    if job_id not in scrape_jobs:
        raise HTTPException(404, "Job not found")
    job = scrape_jobs[job_id]

    if fmt == "csv":
        import csv, io
        out = io.StringIO()
        if not job.results:
            from fastapi.responses import Response
            return Response(content="No results", media_type="text/csv")
        fields = list(job.results[0].keys())
        w = csv.DictWriter(out, fieldnames=fields)
        w.writeheader()
        w.writerows(job.results)
        return StreamingResponse(
            io.BytesIO(out.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=price_comparison.csv"},
        )

    return {"results": job.results, "status": job.status, "elapsed": job.elapsed()}
