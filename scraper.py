"""
scraper.py — Price & availability comparison engine.
Searches the web for each product from an uploaded catalog file,
extracts competitor pricing and stock, streams results back.
"""
import asyncio
import json
import logging
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

log = logging.getLogger("scraper")

# ANSI colours for terminal readability
_C = {
    "reset": "\033[0m",  "bold": "\033[1m",
    "green": "\033[92m", "red":   "\033[91m",
    "yellow":"\033[93m", "cyan":  "\033[96m",
    "grey":  "\033[90m", "blue":  "\033[94m",
}

def _t(msg: str, colour: str = "reset") -> str:
    return f"{_C.get(colour,'')}{msg}{_C['reset']}"

def _log_scrape(label: str, url: str, result: dict):
    price = result.get("found_price")
    avail = result.get("found_availability", "?")
    name  = result.get("page_name", "")
    tier  = result.get("tier", "")
    if price:
        print(
            f"  {_t('✔', 'green')} {_t(label,'bold')} | "
            f"price={_t(price,'green')} | avail={avail}"
            + (f" | tier={tier}" if tier else "")
            + (f"\n    name : {name}" if name else "")
            + f"\n    url  : {_t(url,'grey')}"
        )
    else:
        print(
            f"  {_t('✘', 'red')} {_t(label,'bold')} | no price | avail={avail}"
            + f"\n    url  : {_t(url,'grey')}"
        )

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# PRICE_RE: only match strings that have an EXPLICIT currency marker.
# No bare decimals — "12.99" alone matches version numbers, coordinates, etc.
# Every branch here requires a currency symbol or code word.
PRICE_RE = re.compile(
    r'(?:[\$€£¥₹₩]\s*\d[\d\s,]*\.?\d{0,2})'                              # $12.99  €1 299
    r'|(?:\d[\d\s,]*(?:\.\d{1,2})?\s*(?:€|EUR|USD|GBP|DZD|MAD|TND|DA|DH|DHS|درهم|دج))',  # 12.99€  1299 DA
    re.IGNORECASE,
)

# Words that disqualify a number from being a price (found nearby in text)
_NON_PRICE_CONTEXT = re.compile(
    r'(?:item|items|qty|quantity|review|rating|star|model|ref|sku|year|'
    r'page|result|stock|count|sold|weight|kg|g\b|lb|oz|cm|mm|inch)',
    re.IGNORECASE,
)


def _is_barcode(sku: str) -> bool:
    """Return True if the SKU looks like an EAN/UPC/internal barcode.
    Barcodes are all-digit strings of 8+ chars and never appear on product pages.
    """
    return bool(sku) and re.match(r'^\d{8,}$', sku.strip()) is not None


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
    used_prices = []
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
            if (price is not None):
                used_prices.append(price)
                val = _parse_price_value(str(price))
                if (val is not None):
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


def _extract_from_visible_blocks(soup: BeautifulSoup) -> Optional[str]:
    """
    Tier 5 (broad): scan every short text node on the page.
    Accept a price only when it appears within 3 siblings of a buy/price signal word.
    This catches sites that don't use standard classes or structured data.
    """
    _BUY_RE = re.compile(
        r'(price|prix|cost|tarif|buy|achet|panier|cart|commander|add to|ajouter|'
        r'disponible|stock|checkout|total|amount|montant)',
        re.IGNORECASE,
    )

    def _short_text(node) -> str:
        t = node.get_text(" ", strip=True)
        return t if len(t) < 120 else ""

    for parent in soup.find_all(True):
        children = [c for c in parent.children if hasattr(c, "get_text")]
        for i, child in enumerate(children):
            txt = _short_text(child)
            if not txt or not PRICE_RE.search(txt):
                continue
            # Check surrounding siblings for a buy/price signal
            window = children[max(0, i-3): i+4]
            context = " ".join(_short_text(c) for c in window)
            if _BUY_RE.search(context):
                p = _extract_price_from_text(txt)
                if p:
                    val = _parse_price_value(p)
                    if val is not None:
                        return p
    return None


def _extract_from_css_classes(soup: BeautifulSoup) -> Optional[str]:
    #Try common price CSS class patterns, with validation.
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


def _name_matches(page_name: str, expected_title: str, threshold: float = 0.20) -> bool:
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

    # Adaptive threshold: short titles need a higher fraction of tokens to match.
    # "PARFUM EDT" (2 tokens) at 50% would accept any page mentioning just "parfum"
    # or just "edt" — far too permissive.
    n = len(expected_tokens)
    if n <= 2:
        threshold = max(threshold, 1.0)   # all tokens must match
    elif n == 3:
        threshold = max(threshold, 0.67)  # 2 of 3 tokens

    page_tokens = tokenise(page_name)
    overlap = expected_tokens & page_tokens
    score = len(overlap) / len(expected_tokens)
    return score >= threshold


def _scrape_page_for_price(url: str) -> dict:
    """Fetch a URL and extract price, availability and product name."""
    print(f"  {_t('→ fetching','cyan')} {_t(url,'grey')}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=14)
        if resp.status_code >= 400:
            print(f"  {_t('✘','red')} HTTP {resp.status_code} — {url}")
            return {"found_price": None, "found_availability": "Error", "page_name": ""}

        soup = BeautifulSoup(resp.text, "lxml")

        page_name = _extract_page_product_name(soup)

        # Tier 1: JSON-LD structured data (most reliable)
        found_price = _extract_from_jsonld(soup)
        tier_label = "json-ld" if found_price else None

        # Tier 2: Open Graph / meta price tags
        if not found_price:
            found_price = _extract_from_meta(soup)
            if found_price: tier_label = "meta"

        # Tier 3: itemprop="price"
        if not found_price:
            found_price = _extract_from_schema_itemprop(soup)
            if found_price: tier_label = "itemprop"

        # Tier 4: CSS class heuristics
        if not found_price:
            found_price = _extract_from_css_classes(soup)
            if found_price: tier_label = "css-class"

        # Visible text — used for SKU check and the last-resort regex tiers
        page_text = soup.get_text(" ", strip=True)

        # Tier 5: Visible block context scan (near buy/price keywords)
        if not found_price:
            found_price = _extract_from_visible_blocks(soup)
            if found_price: tier_label = "visible-block"

        # Tier 6: PRICE_RE scan on VISIBLE TEXT ONLY (not raw HTML).
        # Scanning raw HTML hits CSS values, JS strings, version numbers, etc.
        if not found_price:
            m = PRICE_RE.search(page_text)
            if m:
                val = _parse_price_value(m.group())
                if val is not None and 0.5 <= val <= 999_999:
                    found_price = m.group().strip()
                    tier_label = "text-regex"

        if not tier_label:
            tier_label = "none"

        # Availability detection (on raw HTML — these are usually in meta/JS tags)
        pg = resp.text.lower()
        found_availability = "Unknown"
        if any(x in pg for x in ["instock", "in stock", "en stock", "disponible",
                                  "in-stock", "available", "add to cart", "buy now",
                                  "ajouter au panier", "commander"]):
            found_availability = "In Stock"
        elif any(x in pg for x in ["outofstock", "out of stock", "out-of-stock",
                                    "rupture", "épuisé", "unavailable", "sold out",
                                    "indisponible"]):
            found_availability = "Out of Stock"
        elif any(x in pg for x in ["limited stock", "low stock", "hurry", "only"]):
            found_availability = "Low Stock"

        # ── Neural net scan on candidate leaf nodes ────────────────────────
        # Collect short text snippets from leaf nodes and classify them.
        # This runs alongside the rule-based tiers so you can see in the
        # terminal which text the neural net identifies for each field.
        # Neural net scan — fire-and-forget, doesn't affect return value
        try:
            from neural_net import get_net
            candidates = [
                {"text": node.get_text(" ", strip=True), "selector": node.name}
                for node in soup.find_all(True)
                if len(node.find_all()) == 0 and 1 < len(node.get_text(strip=True)) < 200
            ]
            if candidates:
                get_net().predict_page_fields(candidates[:60], verbose=True)
        except Exception:
            pass

        result = {
            "found_price":        found_price,
            "found_availability": found_availability,
            "page_name":          page_name,
            "page_text":          page_text,
            "tier":               tier_label,
        }
        _log_scrape("page", url, result)
        return result
    except Exception as exc:
        print(f"  {_t('✘','red')} exception fetching {url}: {exc}")
        return {"found_price": None, "found_availability": "Error",
                "page_name": "", "page_text": ""}


def _normalize_domain(url: str) -> str:
    """Extract bare domain from a URL string (strips www.)."""
    try:
        parsed = urlparse(url if "://" in url else "https://" + url)
        return parsed.netloc.replace("www.", "").lower()
    except Exception:
        return ""


async def _search_site_for_product(query: str, domain: str, engine: str,
                                   max_results: int = 20) -> list:
    """Search within a specific domain using site: operator, DDG + Bing in parallel."""
    site_query = f'site:{domain} {query}'
    print(f"  {_t('site-search','grey')} domain={domain} query={_t(repr(query),'yellow')}")

    ddg_task  = asyncio.to_thread(_ddg_search,  site_query, max_results)
    bing_task = asyncio.to_thread(_bing_search, site_query, max_results)
    all_batches = await asyncio.gather(ddg_task, bing_task, return_exceptions=True)

    seen: set = set()
    results: list = []
    for batch in all_batches:
        if isinstance(batch, Exception) or not batch:
            continue
        for r in batch:
            u = r.get("url", "")
            if u and domain in u and u not in seen:
                seen.add(u)
                results.append(r)

    print(f"  {_t('→','grey')} {len(results)} site-restricted results")
    return results


def _ddg_search(query: str, max_results: int = 20) -> list:
    """Blocking DuckDuckGo search — run inside asyncio.to_thread."""
    try:
        from ddgs import DDGS
        out = []
        for r in DDGS().text(query, max_results=max_results):
            href = r.get("href", "")
            if href.startswith("http"):
                out.append({"url": href,
                            "snippet": r.get("body", "") + " " + r.get("title", ""),
                            "title": r.get("title", "")})
        return out
    except Exception:
        return []


def _bing_search(query: str, max_results: int = 20) -> list:
    """Blocking Bing search — run inside asyncio.to_thread."""
    try:
        resp = requests.get(
            "https://www.bing.com/search",
            params={"q": query, "count": max_results},
            headers=HEADERS, timeout=20,
        )
        soup = BeautifulSoup(resp.text, "lxml")
        out = []
        for li in soup.select("li.b_algo")[:max_results]:
            a   = li.select_one("h2 a")
            cap = li.select_one(".b_caption p")
            if a and a.get("href", "").startswith("http"):
                out.append({
                    "url":     a["href"],
                    "snippet": cap.get_text() if cap else "",
                    "title":   a.get_text(),
                })
        return out
    except Exception:
        return []


async def _search_urls(query: str, engine: str, max_results: int = 20) -> list:
    """
    Run DDG and Bing IN PARALLEL, merge, deduplicate.
    Both always run — whichever returns first is not awaited by the other.
    """
    print(f"  {_t('search','grey')} query={_t(repr(query),'yellow')}")

    tasks = []
    if engine in ("duckduckgo", "all", "bing"):
        tasks.append(asyncio.to_thread(_ddg_search, query, max_results))
        tasks.append(asyncio.to_thread(_bing_search, query, max_results))
    else:
        tasks.append(asyncio.to_thread(_ddg_search, query, max_results))
        tasks.append(asyncio.to_thread(_bing_search, query, max_results))

    all_results = await asyncio.gather(*tasks, return_exceptions=True)

    seen_urls: set = set()
    merged: list = []
    for batch in all_results:
        if isinstance(batch, Exception) or not batch:
            continue
        for r in batch:
            u = r.get("url", "")
            if u and u not in seen_urls:
                seen_urls.add(u)
                merged.append(r)

    print(f"  {_t('→','grey')} {len(merged)} unique URLs from search")
    return merged


async def _scrape_and_verify(url: str, domain_lock: str = "",
                             expected_title: str = "", expected_sku: str = "") -> dict:
    """
    Fetch url and verify it is the right product before returning.

    Verification rules (only applied when the value is non-empty):
    - expected_title → page name must fuzzy-match it (35% token overlap)
    - expected_sku   → SKU must appear literally in the visible page text

    Both checks must pass when both values are provided.
    Returns the scrape dict on success, empty dict on any failure.
    """
    if not url.startswith("http"):
        return {}
    if domain_lock and domain_lock not in _normalize_domain(url):
        return {}

    data = await asyncio.to_thread(_scrape_page_for_price, url)

    # Name check — only when the product has a title
    if expected_title and not _name_matches(data.get("page_name", ""), expected_title):
        return {}

    # SKU check — only when the product has a SKU
    # The SKU must appear somewhere in the visible page text (case-insensitive)
    if expected_sku:
        page_text = data.get("page_text", "")
        if expected_sku.lower() not in page_text.lower():
            return {}

    return data


async def _find_price_for_product(product: Product, engine: str,
                                   target_url: Optional[str] = None,
                                   used_urls: Optional[set] = None) -> dict:
    """
    Search and verify strategy:

    Has SKU only   → search by SKU.  No extra verification needed (SKU is exact).
    Has name only  → search by name. Verify page name matches.
    Has both       → Phase 1: search by SKU, verify name matches.
                     Phase 2 (fallback): search by name, verify BOTH name and SKU match.

    If target_url is set, every query is restricted to site:<domain> and
    only URLs on that domain are ever scraped.
    """
    has_sku   = bool(product.sku and product.sku.strip())
    has_name  = bool(product.title and product.title.strip())
    domain    = _normalize_domain(target_url) if target_url else ""
    used_urls = used_urls if used_urls is not None else set()  # URLs already claimed by earlier products

    print(
        f"\n{_t('━'*60,'grey')}\n"
        f"{_t('PRODUCT','bold')} {_t(product.title or '(no title)','cyan')}"
        + (f"  SKU={_t(product.sku,'yellow')}" if has_sku else "")
        + (f"  domain={domain}" if domain else "")
        + f"\n{_t('engine','grey')}={engine}"
    )

    NOT_FOUND = {"found_price": None, "found_availability": "Not found",
                 "found_url": "", "found_source": "", "page_name": ""}

    # _SKIP: domains that never have product pages with scrapeable prices
    _SKIP_DOMAINS = {"google.", "bing.", "yahoo.", "youtube.", "facebook.",
                     "instagram.", "twitter.", "tiktok.", "wikipedia.",
                     "reddit.", "linkedin.", "pinterest."}

    # Buy-signal words — at least one must appear on the page for us to trust the price
    _BUY_SIGNALS = re.compile(
        r'add.?to.?cart|buy.?now|ajouter.?au.?panier|commander|checkout|'
        r'add.?to.?bag|acheter|in.?stock|en.?stock|disponible|livraison',
        re.IGNORECASE,
    )

    async def _try(results: list, verify_title: str = "", verify_sku: str = "",
                   limit: int = 12) -> Optional[dict]:
        """
        Visit up to `limit` URLs from search results.
        Rules:
        - Page must have at least one buy/cart signal (proves it's a product page)
        - SKU check: SKU must appear in visible text (case-insensitive)
        - Name check: 20% token overlap with expected title (very lenient)
        - domain is used to filter URLs when a target site is set (was 'domain_lock')
        """
        for r in results[:limit]:
            url = r.get("url", "")
            if not url.startswith("http"):
                continue
            if any(skip in url for skip in _SKIP_DOMAINS):
                continue
            # Filter to target domain when one is set
            if domain and domain not in _normalize_domain(url):
                continue
            # Skip URLs already claimed by a previous product in this job
            if url in used_urls:
                print(f"  {_t('skip','grey')} URL already used for another product")
                continue

            data = await asyncio.to_thread(_scrape_page_for_price, url)
            if not data.get("found_price"):
                continue

            page_text = data.get("page_text", "")
            page_name = data.get("page_name", "")

            # Gate 1: buy-signal check — soft warning only.
            # JS-heavy sites (Shopify, WooCommerce) render cart buttons dynamically;
            # visible text often won't contain them even on a real product page.
            check_text = page_text[:15_000]
            if not _BUY_SIGNALS.search(check_text):
                print(f"  {_t('warn','yellow')} no buy signals — proceeding anyway")
                # do NOT skip — name/SKU gates below are the real verification

            # Gate 2: SKU must appear in visible page text.
            # Normalise both sides (strip non-alphanumeric) so "REF-123" matches
            # "REF 123" or "REF123" as displayed on the page.
            # Skip this gate for barcodes (EAN/UPC) — they're internal codes that
            # never appear on product pages.
            if verify_sku and not _is_barcode(verify_sku):
                sku_norm  = re.sub(r'[^a-z0-9]', '', verify_sku.lower())
                page_norm = re.sub(r'[^a-z0-9]', '', page_text.lower())
                if sku_norm not in page_norm:
                    print(f"  {_t('skip','grey')} SKU «{verify_sku}» not on page")
                    continue

            # Gate 3: name check (only when no SKU to confirm identity).
            # 50% token overlap required — prevents same-brand pages from
            # matching different products (e.g. "PARFUM EDT 65ml" vs "65ml EDT").
            if verify_title and not verify_sku:
                if not _name_matches(page_name, verify_title, threshold=0.50):
                    print(f"  {_t('skip','grey')} name mismatch: page={_t(repr(page_name[:60]),'grey')}")
                    continue

            hit = {
                "found_price":        data["found_price"],
                "found_availability": data["found_availability"],
                "found_url":          url,
                "found_source":       r.get("title", ""),
                "page_name":          page_name,
            }
            used_urls.add(url)  # mark so no other product reuses this page
            print(f"  {_t('★ FOUND','green')} {_t(hit['found_price'],'bold')} "
                  f"tier={data.get('tier','?')}  {_t(url,'grey')}")
            return hit
        return None

    async def _search(query: str, n: int = 20) -> list:
        if domain:
            return await _search_site_for_product(query, domain, engine, max_results=n)
        return await _search_urls(query, engine, max_results=n)

    # ── Phase 1: SKU search ───────────────────────────────────────────────────
    # Skip entirely for barcode SKUs — EAN/UPC codes are internal and never
    # appear on retailer product pages, so searching by them returns irrelevant
    # results that pollute Phase 2.
    if has_sku and not _is_barcode(product.sku):
        print(f"  {_t('[Phase 1]','blue')} SKU search: {_t(product.sku,'yellow')}")

        # Try quoted SKU first, then unquoted if no results
        results = await _search(f'"{product.sku}"')
        if not results:
            results = await _search(product.sku)

        if results:
            # Primary: require the SKU to appear on the page
            hit = await _try(results, verify_sku=product.sku)
            if not hit:
                # Reference code not on page — try name-only check on same URLs
                hit = await _try(results, verify_title=product.title)
            if hit:
                return hit

        # target_url supplied but no search results → scrape it directly
        if domain and target_url:
            data = await asyncio.to_thread(_scrape_page_for_price, target_url)
            if data.get("found_price"):
                return {**data, "found_url": target_url, "found_source": domain}
    elif has_sku:
        print(f"  {_t('[Phase 1]','blue')} barcode SKU {_t(product.sku,'yellow')} — skipping to Phase 2")

    # ── Phase 2: name + SKU combined search ──────────────────────────────────
    if has_name:
        base   = product.title.strip()
        vendor = (product.vendor or "").strip()
        print(f"  {_t('[Phase 2]','blue')} name search: {_t(base,'cyan')}"
              + (f" + SKU {_t(product.sku,'yellow')}" if has_sku else ""))

        # Build queries most-specific first.
        # For products with generic titles (e.g. "PARFUM EDT"), include SKU
        # and product type so each product gets a unique search string.
        queries: list[str] = []
        type_hint = (product.product_type or "").strip()

        # SKU-inclusive queries first — only for real reference codes, not barcodes
        if has_sku and not _is_barcode(product.sku):
            sku = product.sku.strip()
            if vendor:
                queries.append(f'{base} {sku} {vendor}')
            queries.append(f'{base} {sku}')

        # Vendor + type enrichment
        if vendor and vendor.lower() not in base.lower():
            if type_hint and type_hint.lower() not in base.lower():
                queries.append(f'"{base}" {type_hint} {vendor}')
            queries.append(f'"{base}" {vendor}')
        elif type_hint and type_hint.lower() not in base.lower():
            queries.append(f'"{base}" {type_hint}')

        queries.append(f'"{base}"')
        short = " ".join(base.split()[:6])
        if short != base:
            queries.append(short)

        if not domain:
            queries = [q + " buy price" for q in queries]

        results: list = []
        for q in queries:
            r = await _search(q)
            results.extend(r)
            if len(results) >= 20:
                break

        # Deduplicate while preserving order
        seen: set = set()
        results = [r for r in results if not (r["url"] in seen or seen.add(r["url"]))]

        # Single pass with name verification — the adaptive threshold in
        # _name_matches handles short titles (≤2 tokens → 100% match required).
        # The removed lenient pass was causing wrong prices for generic names:
        # accepting any priced page without verification is too risky.
        hit = await _try(results, verify_title=product.title)
        if hit:
            return hit

    print(f"  {_t('✘ NOT FOUND','red')} — no price located for this product")
    return NOT_FOUND


# ── Job runner ────────────────────────────────────────────────────────────────

async def _run_scrape_job(job: ScrapeJob):
    job.status = "running"
    products = job.config.products
    engine = job.config.engine
    target_url = job.config.target_url or None
    target_currency = job.config.target_currency or "MAD"
    used_urls: set = set()  # URLs already claimed; shared across all products in this job

    site_label = f" on {target_url}" if target_url else ""
    print(
        f"\n{_t('═'*60,'cyan')}\n"
        f"{_t('SCRAPE JOB START','bold')} — {len(products)} product(s){site_label}\n"
        f"engine={engine}  currency={target_currency}\n"
        f"{_t('═'*60,'cyan')}"
    )
    job.push({"type": "status",
              "msg": f"Starting price search for {len(products)} products{site_label}…"})

    for i, product in enumerate(products):
        job.push({
            "type": "progress",
            "current": i,
            "total": len(products),
            "msg": f"[{i+1}/{len(products)}] Searching: {product.title[:60]}…"
        })

        try:
            data = await _find_price_for_product(product, engine, target_url, used_urls)
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


@router.post("/map-columns")
async def map_excel_columns_endpoint(body: dict):
    """
    Call this when a spreadsheet is uploaded on the frontend.
    Body: {"columns": {"ColName": ["row1","row2",…], …}}
    The neural net analyses column names + sample values and prints
    the full mapping to the terminal. Returns the mapping as JSON too.
    """
    import asyncio
    from neural_net import get_net
    import pandas as pd

    cols_data = body.get("columns", {})
    if not cols_data:
        return {"error": "No columns provided"}

    print(
        f"\n{_t('═'*60,'cyan')}\n"
        f"{_t('EXCEL UPLOAD — Neural Net Column Mapper','bold')}\n"
        f"{_t('═'*60,'cyan')}"
    )

    df = pd.DataFrame({k: v for k, v in cols_data.items()})
    net = await asyncio.to_thread(get_net)
    mapping = net.map_excel_columns(df, verbose=True)
    return {"mapping": mapping}
