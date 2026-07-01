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


# ── Currency conversion ───────────────────────────────────────────────────────
#
# HOW IT WORKS
# ────────────
# All rates below are stored as "1 unit of CURRENCY = N MAD".
# To convert between any two currencies we pivot through MAD:
#
#   rate(A → B) = MAD_RATES[A] / MAD_RATES[B]
#
# Example: 100 USD → MAD  →  100 × (10.05 / 1.0)  = 1005 MAD
# Example: 100 USD → EUR  →  100 × (10.05 / 11.20) = ~89.7 EUR
#
# TO UPDATE RATES: Go to https://www.bkam.ma (Bank Al-Maghrib) or
# https://www.xe.com and look up the mid-rate for each currency vs MAD.
# Update the numbers below — no code changes needed anywhere else.
# ─────────────────────────────────────────────────────────────────────────────

# 1 unit of each currency expressed in MAD  (update periodically)
MAD_RATES: dict[str, float] = {
    "MAD": 1.0,
    "USD": 10.05,
    "EUR": 11.20,
    "GBP": 13.10,
    "CHF": 11.60,
    "CAD": 7.35,
    "AUD": 6.55,
    "JPY": 0.069,   # 1 JPY = 0.069 MAD  (i.e. 100 JPY ≈ 6.9 MAD)
    "CNY": 1.39,
    "SAR": 2.68,
    "AED": 2.74,
    "KWD": 32.70,
    "QAR": 2.76,
    "BHD": 26.65,
    "OMR": 26.10,
    "DZD": 0.075,
    "TND": 3.25,
    "TRY": 0.29,
    "INR": 0.12,
    "KRW": 0.0073,
}

_SYMBOL_TO_CODE = {
    '$': 'USD', '€': 'EUR', '£': 'GBP', '¥': 'JPY',
    '₹': 'INR', '₩': 'KRW',
}
_TEXT_TO_CODE = {
    'USD': 'USD', 'EUR': 'EUR', 'GBP': 'GBP', 'JPY': 'JPY',
    'MAD': 'MAD', 'DH': 'MAD', 'DHS': 'MAD',
    'DZD': 'DZD', 'DA': 'DZD',
    'TND': 'TND',
    'SAR': 'SAR', 'AED': 'AED', 'QAR': 'QAR', 'KWD': 'KWD',
    'CNY': 'CNY', 'CAD': 'CAD', 'AUD': 'AUD', 'CHF': 'CHF',
    'TRY': 'TRY', 'INR': 'INR', 'KRW': 'KRW',
}


def _detect_currency(price_str: str) -> str:
    """Detect the currency code from a raw price string."""
    for sym, code in _SYMBOL_TO_CODE.items():
        if sym in price_str:
            return code
    upper = price_str.upper()
    for text, code in _TEXT_TO_CODE.items():
        if text in upper:
            return code
    return 'USD'  # default — most international e-commerce prices are in USD


def _get_rate(from_curr: str, to_curr: str) -> float:
    """Convert between any two currencies by pivoting through MAD."""
    f = from_curr.upper()
    t = to_curr.upper()
    if f == t:
        return 1.0
    from_in_mad = MAD_RATES.get(f)
    to_in_mad   = MAD_RATES.get(t)
    if from_in_mad and to_in_mad:
        return round(from_in_mad / to_in_mad, 6)
    return 1.0  # unknown currency — no conversion


def _convert_price(price_str: str, target_currency: str) -> tuple[Optional[float], str]:
    """
    Parse a raw price string, detect its currency, convert to target_currency.
    Returns (converted_value, detected_source_currency).
    """
    src = _detect_currency(price_str)
    val = _parse_price_value(price_str)
    if val is None:
        return None, src
    rate = _get_rate(src, target_currency)
    return round(val * rate, 2), src


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
    target_currency: str = "MAD"       # currency to convert all prices into


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


def _extract_page_product_name(soup: BeautifulSoup) -> str:
    """Best-effort extraction of the product name shown on a page."""
    # JSON-LD first
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string or '')
            if isinstance(data, list):
                data = data[0] if data else {}
            name = data.get('name') or data.get('Name')
            if name and isinstance(name, str) and len(name) > 2:
                return name.strip()
        except Exception:
            pass
    # itemprop="name"
    el = soup.find(attrs={'itemprop': 'name'})
    if el:
        txt = el.get('content') or el.get_text(strip=True)
        if txt:
            return txt.strip()
    # og:title
    og = soup.find('meta', property='og:title')
    if og and og.get('content'):
        return og['content'].strip()
    # <title> tag (often "Product Name | Site")
    title_tag = soup.find('title')
    if title_tag:
        raw = title_tag.get_text(strip=True)
        # strip everything after the first | or – delimiter
        for sep in ('|', '–', '-', '::'):
            if sep in raw:
                raw = raw.split(sep)[0].strip()
                break
        return raw
    # h1 fallback
    h1 = soup.find('h1')
    return h1.get_text(strip=True) if h1 else ""


def _name_matches(page_name: str, expected_title: str, threshold: float = 0.35) -> bool:
    """
    Return True if page_name is a plausible match for expected_title.

    Strategy: tokenise both strings (lowercase, drop short stop-words),
    then check what fraction of the expected title's significant tokens
    appear anywhere in the page name. Threshold is intentionally low (35%)
    so partial / translated names still pass — we just want to rule out
    completely wrong products.
    """
    if not page_name or not expected_title:
        return True  # can't verify → don't reject

    _STOPS = {'the', 'a', 'an', 'and', 'or', 'of', 'for', 'with', 'in', 'on',
              'to', 'by', 'at', 'de', 'du', 'le', 'la', 'les', 'et', 'pour'}

    def tokenise(s: str) -> set:
        tokens = re.findall(r'[a-z0-9]+', s.lower())
        return {t for t in tokens if len(t) > 2 and t not in _STOPS}

    expected_tokens = tokenise(expected_title)
    if not expected_tokens:
        return True

    page_tokens = tokenise(page_name)
    overlap = expected_tokens & page_tokens
    score = len(overlap) / len(expected_tokens)
    return score >= threshold


def _scrape_page_for_price(url: str) -> dict:
    """Fetch a URL and extract price, availability and product name."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=14)
        if resp.status_code >= 400:
            return {"found_price": None, "found_availability": "Error", "page_name": ""}

        soup = BeautifulSoup(resp.text, "lxml")

        page_name = _extract_page_product_name(soup)

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

        # Tier 5: Regex on the first 80k chars — currency-anchored only
        if not found_price:
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

        return {"found_price": found_price, "found_availability": found_availability,
                "page_name": page_name}
    except Exception:
        return {"found_price": None, "found_availability": "Error", "page_name": ""}


def _normalize_domain(url: str) -> str:
    """Extract bare domain from a URL string (strips www.)."""
    try:
        parsed = urlparse(url if "://" in url else "https://" + url)
        return parsed.netloc.replace("www.", "").lower()
    except Exception:
        return ""


async def _search_site_for_product(query: str, domain: str, engine: str,
                                   max_results: int = 64) -> list:
    """Search exclusively within a specific domain using site: operator."""
    site_query = f'site:{domain} {query}'
    results = []

    if engine in ("duckduckgo", "all"):
        from ddgs import DDGS
        def _ddg_site():
            out = []
            try:
                for r in DDGS().text(site_query, max_results=max_results):
                    href = r.get("href", "")
                    if href.startswith("http") and domain in href:
                        out.append({"url": href,
                                    "snippet": r.get("body", "") + " " + r.get("title", ""),
                                    "title": r.get("title", "")})
            except Exception:
                pass
            return out
        results = await asyncio.to_thread(_ddg_site)

    if not results:
        try:
            resp = await asyncio.to_thread(
                lambda: requests.get(
                    "https://www.bing.com/search",
                    params={"q": site_query, "count": max_results},
                    headers=HEADERS, timeout=20,
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


async def _search_urls(query: str, engine: str, max_results: int) -> list:
    """Return result dicts from a search engine (open web, no site restriction)."""
    results = []

    if engine in ("duckduckgo", "all"):
        from ddgs import DDGS
        def _ddg_web():
            out = []
            try:
                for r in DDGS().text(query, max_results=30):
                    href = r.get("href", "")
                    if href.startswith("http"):
                        out.append({"url": href,
                                    "snippet": r.get("body", "") + " " + r.get("title", ""),
                                    "title": r.get("title", "")})
            except Exception:
                pass
            return out
        results = await asyncio.to_thread(_ddg_web)

    if not results and engine in ("bing", "all"):
        try:
            resp = await asyncio.to_thread(
                lambda: requests.get(
                    "https://www.bing.com/search",
                    params={"q": query, "count": max_results},
                    headers=HEADERS, timeout=20,
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


async def _scrape_and_verify(url: str, expected_title: str, domain_lock: str = "") -> dict:
    """
    Scrape url, verify the page's product name fuzzy-matches expected_title.
    Returns the scrape dict with an extra 'verified' bool, or empty dict on mismatch.
    domain_lock: if set, reject URLs not on that domain.
    """
    if not url.startswith("http"):
        return {}
    if domain_lock and domain_lock not in _normalize_domain(url):
        return {}
    data = await asyncio.to_thread(_scrape_page_for_price, url)
    if not _name_matches(data.get("page_name", ""), expected_title):
        return {}   # wrong product — skip
    return data


async def _find_price_for_product(product: Product, engine: str,
                                   target_url: Optional[str] = None) -> dict:
    """
    Find the price of a product by SKU, verified against the product title.

    Flow
    ----
    Phase 1 — SKU search (primary):
      • Query = "<SKU>" [+ site:<domain> if target_url is set]
      • Visit each result, scrape, check if page name ≈ product title
      • First passing page → done

    Phase 2 — title fallback (only if SKU search fails or no SKU):
      • Query = "<title>" [+ vendor]
      • Same scrape + verify loop

    target_url constraint: when provided, EVERY search uses site:<domain>
    and only URLs on that domain are ever visited.
    """
    domain = _normalize_domain(target_url) if target_url else ""
    NOT_FOUND = {"found_price": None, "found_availability": "Not found",
                 "found_url": "", "found_source": ""}

    async def _try_results(results: list, limit: int = 6) -> Optional[dict]:
        for r in results[:limit]:
            url = r.get("url", "")
            data = await _scrape_and_verify(url, product.title, domain_lock=domain)
            if data and data.get("found_price"):
                return {
                    "found_price": data["found_price"],
                    "found_availability": data["found_availability"],
                    "found_url": url,
                    "found_source": r.get("title", ""),
                    "page_name": data.get("page_name", ""),
                }
        return None

    # ── Phase 1: SKU-only search ──────────────────────────────────────────────
    if product.sku:
        sku_query = f'"{product.sku}"'
        if domain:
            results = await _search_site_for_product(sku_query, domain, engine, max_results=10)
        else:
            results = await _search_urls(sku_query, engine, max_results=10)

        hit = await _try_results(results)
        if hit:
            return hit

        # If target URL is set and search returned nothing, try the base URL directly
        if domain and not results and target_url:
            data = await _scrape_and_verify(target_url, product.title, domain_lock=domain)
            if data and data.get("found_price"):
                return {"found_price": data["found_price"],
                        "found_availability": data["found_availability"],
                        "found_url": target_url, "found_source": domain,
                        "page_name": data.get("page_name", "")}

    # ── Phase 2: title + vendor search (fallback) ─────────────────────────────
    title_parts = []
    if product.title:
        title_parts.append(f'"{product.title}"')
    if product.vendor and product.vendor not in product.title:
        title_parts.append(product.vendor)
    title_query = " ".join(title_parts)

    if title_query:
        if domain:
            results = await _search_site_for_product(title_query, domain, engine, max_results=10)
        else:
            results = await _search_urls(title_query + " price buy", engine, max_results=10)

        hit = await _try_results(results)
        if hit:
            return hit

        # Last resort for open-web: price from snippet (no page visit)
        if not domain:
            currency_re = re.compile(
                r'(?:[\$€£¥₹₩]\s*\d[\d\s,]*\.?\d{0,2})'
                r'|(?:\d[\d\s,]*\.?\d{0,2}\s*(?:€|EUR|USD|GBP|DZD|MAD|TND|DA|DH))',
            )
            for r in results:
                m = currency_re.search(r.get("snippet", ""))
                if m:
                    val = _parse_price_value(m.group())
                    if val is not None:
                        return {"found_price": m.group().strip(),
                                "found_availability": "Unknown",
                                "found_url": r.get("url", ""),
                                "found_source": r.get("title", ""),
                                "page_name": ""}

    return NOT_FOUND


# ── Job runner ────────────────────────────────────────────────────────────────

async def _run_scrape_job(job: ScrapeJob):
    job.status = "running"
    products = job.config.products
    engine = job.config.engine
    target_url = job.config.target_url or None
    target_currency = job.config.target_currency or "MAD"

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

        # Currency conversion
        converted_price = None
        source_currency = None
        if data["found_price"]:
            converted_price, source_currency = _convert_price(
                data["found_price"], target_currency
            )

        # Compute price delta in target currency
        delta = None
        delta_pct = None
        if product.your_price and converted_price is not None:
            delta = round(converted_price - product.your_price, 2)
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
            "found_price_converted": converted_price,
            "source_currency": source_currency,
            "target_currency": target_currency,
            "found_availability": data["found_availability"],
            "found_url": data["found_url"],
            "found_source": data["found_source"],
            "found_name": data.get("page_name", ""),
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
