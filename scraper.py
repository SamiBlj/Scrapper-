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
from urllib.parse import urlparse

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

# Only match prices that have explicit currency markers OR proper decimal format (e.g. 12.99 / 1,299.99)
# No bare integers — that was causing random numbers to be picked up as prices.
PRICE_RE = re.compile(
    r'(?:[\$€£¥₹₩]\s*\d[\d\s,]*\.?\d{0,2})'           # $12.99  €1 299
    r'|(?:\d[\d\s,]*\.?\d{0,2}\s*(?:€|EUR|USD|GBP|DZD|MAD|TND|DA|DH|دج))'  # 12.99€  1299 DA
    r'|(?:\d{1,3}(?:[,\s]\d{3})+(?:\.\d{1,2})?)'        # 1,299.99  1 299.99
    r'|(?:\d+\.\d{2})',                                   # 12.99 — decimal prices only
)

# Words that disqualify a number from being a price (found nearby in text)
_NON_PRICE_CONTEXT = re.compile(
    r'(?:item|items|qty|quantity|review|rating|star|model|ref|sku|year|'
    r'page|result|stock|count|sold|weight|kg|g\b|lb|oz|cm|mm|inch)',
    re.IGNORECASE,
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
    target_url: Optional[str] = None   # optional competitor site to restrict search to


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


# ── Price extraction helpers ──────────────────────────────────────────────────

def _parse_price_value(text: str) -> Optional[float]:
    """Convert a raw price string to a float, or None if it can't be parsed."""
    raw = re.sub(r'[^\d.,]', '', text.strip())
    if not raw:
        return None
    # Handle formats like 1,299.99 or 1.299,99
    if ',' in raw and '.' in raw:
        if raw.rfind(',') > raw.rfind('.'):
            raw = raw.replace('.', '').replace(',', '.')
        else:
            raw = raw.replace(',', '')
    elif ',' in raw:
        # Could be decimal comma (European) or thousands separator
        parts = raw.split(',')
        if len(parts) == 2 and len(parts[1]) <= 2:
            raw = raw.replace(',', '.')
        else:
            raw = raw.replace(',', '')
    try:
        val = float(raw)
        # Sanity check: prices should be between 0.01 and 9,999,999
        if 0.01 <= val <= 9_999_999:
            return round(val, 2)
    except ValueError:
        pass
    return None


def _extract_price_from_text(text: str) -> Optional[str]:
    """Pull the best price candidate from arbitrary text."""
    matches = PRICE_RE.findall(text)
    for m in matches:
        m = m.strip()
        val = _parse_price_value(m)
        if val is not None:
            return m
    return None


def _extract_from_jsonld(soup: BeautifulSoup) -> Optional[str]:
    """Parse JSON-LD structured data — most reliable source for e-commerce prices."""
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            raw = script.string or ''
            data = json.loads(raw)
            if isinstance(data, list):
                data = data[0] if data else {}

            # Offer can be nested or at top level
            offers = data.get('offers') or data.get('Offers') or {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}

            price = (
                offers.get('price') or offers.get('Price') or
                data.get('price') or data.get('Price')
            )
            if price is not None:
                val = _parse_price_value(str(price))
                if val is not None:
                    currency = offers.get('priceCurrency', '')
                    return f"{val} {currency}".strip()
        except Exception:
            continue
    return None


def _extract_from_meta(soup: BeautifulSoup) -> Optional[str]:
    """Check Open Graph and standard price meta tags."""
    for prop in ['product:price:amount', 'og:price:amount', 'twitter:data1']:
        el = soup.find('meta', property=prop) or soup.find('meta', attrs={'name': prop})
        if el and el.get('content'):
            val = _parse_price_value(el['content'])
            if val is not None:
                return str(val)
    return None


def _extract_from_schema_itemprop(soup: BeautifulSoup) -> Optional[str]:
    """Extract price from itemprop="price" — second most reliable source."""
    el = soup.find(attrs={'itemprop': 'price'})
    if el:
        # Prefer the `content` attribute (machine-readable)
        content = el.get('content')
        if content:
            val = _parse_price_value(content)
            if val is not None:
                return str(val)
        # Fall back to text content
        text = el.get_text(strip=True)
        if text:
            val = _parse_price_value(text)
            if val is not None:
                return text
    return None


def _extract_from_css_classes(soup: BeautifulSoup) -> Optional[str]:
    """Try common price CSS class patterns, with validation."""
    selectors = [
        '[class*="sale-price"]', '[class*="sale_price"]',
        '[class*="product-price"]', '[class*="product_price"]',
        '[class*="current-price"]', '[class*="current_price"]',
        '[class*="final-price"]', '[class*="final_price"]',
        '[id*="product-price"]', '[id*="productPrice"]',
        '[class*="prix"]', '[id*="prix"]',
        '[class*="price__amount"]', '[class*="price-item"]',
        '[class*="price"]', '[id*="price"]',
        '[class*="amount"]',
    ]
    for sel in selectors:
        for node in soup.select(sel)[:8]:
            txt = node.get_text(' ', strip=True)
            if not txt or len(txt) > 60:
                continue
            # Skip nodes that look like a container (have many children)
            if len(node.find_all()) > 4:
                continue
            p = _extract_price_from_text(txt)
            if p:
                val = _parse_price_value(p)
                if val is not None:
                    return p
    return None


def _scrape_page_for_price(url: str) -> dict:
    """Fetch a URL and extract price + availability using a tiered strategy."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=14)
        if resp.status_code >= 400:
            return {"found_price": None, "found_availability": "Error"}

        soup = BeautifulSoup(resp.text, "lxml")

        # Tier 1: JSON-LD structured data (most reliable)
        found_price = _extract_from_jsonld(soup)

        # Tier 2: Open Graph / meta price tags
        if not found_price:
            found_price = _extract_from_meta(soup)

        # Tier 3: itemprop="price"
        if not found_price:
            found_price = _extract_from_schema_itemprop(soup)

        # Tier 4: CSS class heuristics
        if not found_price:
            found_price = _extract_from_css_classes(soup)

        # Tier 5: Regex on the first 80k chars — but only accept strings with a currency symbol
        if not found_price:
            # Restrict to currency-anchored matches only (no bare decimals)
            currency_re = re.compile(
                r'(?:[\$€£¥₹₩]\s*\d[\d\s,]*\.?\d{0,2})'
                r'|(?:\d[\d\s,]*\.?\d{0,2}\s*(?:€|EUR|USD|GBP|DZD|MAD|TND|DA|DH))',
            )
            m = currency_re.search(resp.text[:80_000])
            if m:
                val = _parse_price_value(m.group())
                if val is not None:
                    found_price = m.group().strip()

        # Availability detection
        pg = resp.text.lower()
        found_availability = "Unknown"
        if any(x in pg for x in ["instock", "in stock", "en stock", "disponible",
                                  "in-stock", "available", "add to cart", "buy now"]):
            found_availability = "In Stock"
        elif any(x in pg for x in ["outofstock", "out of stock", "out-of-stock",
                                    "rupture", "épuisé", "unavailable", "sold out"]):
            found_availability = "Out of Stock"
        elif any(x in pg for x in ["limited stock", "low stock", "hurry", "only"]):
            found_availability = "Low Stock"

        return {"found_price": found_price, "found_availability": found_availability}
    except Exception:
        return {"found_price": None, "found_availability": "Error"}


def _normalize_domain(url: str) -> str:
    """Extract bare domain from a URL string (strips www.)."""
    try:
        parsed = urlparse(url if "://" in url else "https://" + url)
        return parsed.netloc.replace("www.", "").lower()
    except Exception:
        return ""


def _ddg_html_search(query: str, max_results: int) -> list[dict]:
    """
    Scrape DDG's plain-HTML endpoint directly — avoids the ddgs library's
    unfixable 'Separator is not found' chunked-transfer parser bug.
    Returns list of {url, title, snippet} dicts.
    """
    import urllib.parse
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    results: list[dict] = []
    next_form: dict = {}

    for page in range((max_results + 9) // 10):
        try:
            import time
            if page == 0:
                resp = requests.post(
                    "https://html.duckduckgo.com/html/",
                    data={"q": query, "b": "", "kl": ""},
                    headers=headers, timeout=15, allow_redirects=True,
                )
            else:
                resp = requests.post(
                    "https://html.duckduckgo.com/html/",
                    data=next_form,
                    headers=headers, timeout=15, allow_redirects=True,
                )
            soup = BeautifulSoup(resp.text, "lxml")
            for result in soup.select(".result"):
                a = result.select_one("a.result__a")
                snip = result.select_one(".result__snippet")
                if not a:
                    continue
                href = a.get("href", "")
                if "uddg=" in href:
                    href = urllib.parse.unquote(href.split("uddg=")[-1].split("&")[0])
                if href.startswith("http"):
                    results.append({
                        "url": href,
                        "title": a.get_text(strip=True),
                        "snippet": snip.get_text(strip=True) if snip else "",
                    })
            if len(results) >= max_results:
                break
            nav_form = soup.select_one("form[action='/html/']")
            if not nav_form:
                break
            next_form = {
                inp.get("name"): inp.get("value", "")
                for inp in nav_form.select("input[name]")
            }
            if not next_form.get("dc"):
                break
            time.sleep(1)
        except Exception:
            break

    return results[:max_results]


async def _search_site_for_product(query: str, domain: str, engine: str,
                                   max_results: int = 6) -> list:
    """Search exclusively within a specific domain using site: operator."""
    site_query = f'site:{domain} {query}'
    results = []

    if engine in ("duckduckgo", "all"):
        try:
            raw = await asyncio.to_thread(_ddg_html_search, site_query, max_results)
            for r in raw:
                if domain in r["url"]:
                    results.append({"url": r["url"], "snippet": r["snippet"], "title": r["title"]})
        except Exception:
            pass

    if not results:
        try:
            resp = await asyncio.to_thread(
                lambda: requests.get(
                    "https://www.bing.com/search",
                    params={"q": site_query, "count": max_results},
                    headers=HEADERS, timeout=10,
                )
            )
            soup = BeautifulSoup(resp.text, "lxml")
            for li in soup.select("li.b_algo")[:max_results]:
                a = li.select_one("h2 a")
                cap = li.select_one(".b_caption p")
                if a:
                    href = a.get("href", "")
                    if domain in href:
                        results.append({
                            "url": href,
                            "snippet": cap.get_text() if cap else "",
                            "title": a.get_text(),
                        })
        except Exception:
            pass

    return results


async def _search_urls(query: str, engine: str, max_results: int = 6) -> list:
    """Return result dicts from a search engine (open web, no site restriction)."""
    results = []

    if engine in ("duckduckgo", "all"):
        try:
            results = await asyncio.to_thread(_ddg_html_search, query, max_results)
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


async def _find_price_for_product(product: Product, engine: str,
                                   target_url: Optional[str] = None) -> dict:
    """Return best found price + meta for one product.

    If target_url is set, ALL lookups are restricted to that site — no external
    URLs are ever visited. The search query uses `site:<domain>` and only URLs
    whose hostname matches the domain are scraped.

    Strategy:
    1. Build a specific search query (SKU preferred, then title + vendor)
    2. Search the target site (or open web if no target)
    3. Visit matching pages and extract using structured data
    4. Snippets are only used as a last resort
    """
    # Build the most specific query possible
    parts = []
    if product.sku:
        parts.append(f'"{product.sku}"')
    if product.title:
        parts.append(f'"{product.title}"')
    if product.vendor and product.vendor not in product.title:
        parts.append(product.vendor)
    query = " ".join(parts)

    if target_url:
        domain = _normalize_domain(target_url)
        results = await _search_site_for_product(query, domain, engine, max_results=6)

        # If search returned nothing, fall back to visiting the root/target URL directly
        if not results:
            results = [{"url": target_url, "snippet": "", "title": domain}]

        # Only visit URLs that belong to the target domain
        for r in results[:5]:
            url = r.get("url", "")
            if not url.startswith("http"):
                continue
            if domain not in _normalize_domain(url):
                continue  # never leave the target site
            data = await asyncio.to_thread(_scrape_page_for_price, url)
            if data["found_price"]:
                return {
                    "found_price": data["found_price"],
                    "found_availability": data["found_availability"],
                    "found_url": url,
                    "found_source": r.get("title", ""),
                }
    else:
        # Open web search
        results = await _search_urls(query + " price buy", engine, max_results=6)

        for r in results[:4]:
            url = r.get("url", "")
            if not url.startswith("http"):
                continue
            data = await asyncio.to_thread(_scrape_page_for_price, url)
            if data["found_price"]:
                return {
                    "found_price": data["found_price"],
                    "found_availability": data["found_availability"],
                    "found_url": url,
                    "found_source": r.get("title", ""),
                }

        # Last resort: snippet text with currency symbol only
        currency_re = re.compile(
            r'(?:[\$€£¥₹₩]\s*\d[\d\s,]*\.?\d{0,2})'
            r'|(?:\d[\d\s,]*\.?\d{0,2}\s*(?:€|EUR|USD|GBP|DZD|MAD|TND|DA|DH))',
        )
        for r in results:
            m = currency_re.search(r.get("snippet", ""))
            if m:
                val = _parse_price_value(m.group())
                if val is not None:
                    return {
                        "found_price": m.group().strip(),
                        "found_availability": "Unknown",
                        "found_url": r.get("url", ""),
                        "found_source": r.get("title", ""),
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
    target_url = job.config.target_url or None

    site_label = f" on {target_url}" if target_url else ""
    job.push({"type": "status",
              "msg": f"Starting price search for {len(products)} products{site_label}…"})

    for i, product in enumerate(products):
        job.push({
            "type": "progress",
            "current": i,
            "total": len(products),
            "msg": f"[{i+1}/{len(products)}] Searching: {product.title[:60]}…",
        })

        try:
            data = await _find_price_for_product(product, engine, target_url)
        except Exception as e:
            data = {"found_price": None, "found_availability": "Error",
                    "found_url": "", "found_source": str(e)}

        # Compute price delta
        delta = None
        delta_pct = None
        if product.your_price and data["found_price"]:
            val = _parse_price_value(data["found_price"])
            if val is not None:
                delta = round(val - product.your_price, 2)
                delta_pct = round((delta / product.your_price) * 100, 1)

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

        await asyncio.sleep(0.5)  # gentle pacing to avoid rate limiting

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
