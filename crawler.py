"""
crawler.py — Full-site product crawler.
Given a starting URL it crawls every page on that domain, detects product pages,
and extracts all available product data from each one.
"""
import asyncio
import json
import re
import uuid
from collections import deque
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter(prefix="/api/crawler")
crawl_jobs: dict = {}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8,ar;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# URL patterns that strongly suggest a product detail page
_PRODUCT_URL_RE = re.compile(
    r'/(product|produit|item|article|pd|detail|fiche|p)s?/'   # /product/ /produit/ etc.
    r'|[?&](product_id|item_id|pid|prod_id)='                 # query param IDs
    r'|/[a-z0-9%_-]+-\d{5,}[./-]'                            # slug ending in long numeric ID
    r'|/[A-Z0-9]{6,20}$',                                     # bare SKU-style path
    re.IGNORECASE,
)

# URL patterns to skip entirely (assets, utility pages, external)
_SKIP_URL_RE = re.compile(
    r'\.(jpg|jpeg|png|gif|svg|webp|pdf|zip|css|js|ico|woff2?|ttf|eot)(\?.*)?$'
    r'|/(cart|panier|checkout|login|register|compte|account|wishlist|wish-list'
    r'|search|recherche|tag|compare|print|rss|feed|sitemap|cdn-)s?[/?#]?$',
    re.IGNORECASE,
)

_BUY_SIGNALS = [
    "add to cart", "ajouter au panier", "buy now", "acheter", "commander",
    "in stock", "en stock", "disponible", "add to bag", "add to basket",
    "checkout", "quantity", "quantité", "qty", "ajouter",
]


# ── Models ────────────────────────────────────────────────────────────────────

class CrawlConfig(BaseModel):
    url: str
    max_pages: int = 300
    max_products: int = 1000
    concurrency: int = 6


class CrawlJob:
    def __init__(self, job_id: str, config: CrawlConfig):
        self.job_id = job_id
        self.config = config
        self.events: list = []
        self.products: list = []
        self.done = False
        self.status = "pending"
        self.start_time = datetime.now()
        self.pages_scanned = 0
        self.pages_queued = 0

    def push(self, event: dict):
        self.events.append(event)

    def elapsed(self):
        return round((datetime.now() - self.start_time).total_seconds(), 1)


# ── URL helpers ───────────────────────────────────────────────────────────────

def _clean_url(url: str) -> str:
    url, _ = urldefrag(url)   # strip #fragment
    return url.rstrip("/")


def _same_domain(url: str, domain: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
        return host == domain or host.endswith("." + domain)
    except Exception:
        return False


# ── Page fetching ─────────────────────────────────────────────────────────────

def _fetch(url: str) -> Optional[tuple[BeautifulSoup, str]]:
    """Fetch url, return (soup, final_url) or None on failure."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        if resp.status_code >= 400:
            return None
        ct = resp.headers.get("Content-Type", "")
        if "text/html" not in ct:
            return None
        return BeautifulSoup(resp.text, "lxml"), resp.url
    except Exception:
        return None


# ── Link extraction ───────────────────────────────────────────────────────────

def _extract_links(soup: BeautifulSoup, base_url: str, domain: str) -> list[str]:
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("javascript:", "mailto:", "tel:")):
            continue
        abs_url = _clean_url(urljoin(base_url, href))
        if not abs_url.startswith("http"):
            continue
        if not _same_domain(abs_url, domain):
            continue
        if _SKIP_URL_RE.search(abs_url):
            continue
        links.append(abs_url)
    return list(dict.fromkeys(links))   # deduplicate, preserve order


# ── Product page detection ────────────────────────────────────────────────────

def _is_product_page(soup: BeautifulSoup, url: str) -> bool:
    """
    Score the page on multiple signals. Product pages score 6+.
    A JSON-LD Product type is treated as definitive.
    """
    # Definitive: JSON-LD with @type = Product
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string or "")
            types = [ld] if isinstance(ld, dict) else ld if isinstance(ld, list) else []
            for item in types:
                t = item.get("@type", "")
                if "Product" in (t if isinstance(t, list) else [t]):
                    return True
        except Exception:
            pass

    score = 0

    # itemprop="price" — very strong signal
    if soup.find(attrs={"itemprop": "price"}):
        score += 4

    # Product URL pattern
    if _PRODUCT_URL_RE.search(url):
        score += 2

    # Buy signals in visible text
    text_lower = soup.get_text(" ", strip=True).lower()
    buy_hits = sum(1 for s in _BUY_SIGNALS if s in text_lower)
    score += min(buy_hits * 2, 6)

    # Price CSS class or ID
    if soup.select('[class*="price"],[id*="price"],[class*="prix"],[id*="prix"]'):
        score += 2

    # Exactly one H1 (most product pages have a single product title)
    if len(soup.find_all("h1")) == 1:
        score += 1

    # "Add to cart" button specifically
    page_html = str(soup).lower()
    if "add to cart" in page_html or "ajouter au panier" in page_html:
        score += 3

    return score >= 6


# ── Data extraction ───────────────────────────────────────────────────────────

def _extract_product(soup: BeautifulSoup, url: str) -> dict:
    """
    Extract every available piece of product data from a page.
    Uses a priority chain: JSON-LD → itemprop microdata → CSS heuristics → regex.
    """
    product = {
        "url": url,
        "name": None,
        "price": None,
        "currency": None,
        "sku": None,
        "brand": None,
        "availability": None,
        "description": None,
        "rating": None,
        "review_count": None,
        "category": None,
        "images": [],
        "attributes": {},
    }

    # ── Tier 1: JSON-LD ──────────────────────────────────────────────────────
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string or "")
            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                types = item.get("@type", "")
                if "Product" not in (types if isinstance(types, list) else [types]):
                    continue

                product["name"] = item.get("name")
                product["description"] = (item.get("description") or "")[:400]
                product["sku"] = item.get("sku") or item.get("mpn") or item.get("gtin13") or item.get("gtin")

                brand = item.get("brand") or {}
                product["brand"] = brand.get("name") if isinstance(brand, dict) else str(brand)

                offers = item.get("offers") or {}
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                if offers:
                    product["price"] = str(offers.get("price", "")).strip()
                    product["currency"] = offers.get("priceCurrency", "")
                    avail = offers.get("availability", "")
                    if "InStock" in avail:
                        product["availability"] = "In Stock"
                    elif avail:
                        product["availability"] = "Out of Stock"

                agg = item.get("aggregateRating") or {}
                product["rating"] = agg.get("ratingValue")
                product["review_count"] = agg.get("reviewCount")

                imgs = item.get("image", [])
                if isinstance(imgs, str):
                    imgs = [imgs]
                product["images"] = [i if i.startswith("http") else urljoin(url, i) for i in imgs[:6]]

                break   # first Product block is enough
        except Exception:
            pass

    # ── Tier 2: itemprop microdata ────────────────────────────────────────────
    def _itemprop(name: str) -> Optional[str]:
        el = soup.find(attrs={"itemprop": name})
        if not el:
            return None
        return (el.get("content") or el.get_text(strip=True)) or None

    if not product["name"]:
        product["name"] = _itemprop("name")
    if not product["price"]:
        product["price"] = _itemprop("price")
    if not product["currency"]:
        product["currency"] = _itemprop("priceCurrency")
    if not product["sku"]:
        product["sku"] = _itemprop("sku") or _itemprop("mpn")
    if not product["brand"]:
        product["brand"] = _itemprop("brand")
    if not product["description"]:
        product["description"] = (_itemprop("description") or "")[:400] or None
    if not product["rating"]:
        product["rating"] = _itemprop("ratingValue")
    if not product["review_count"]:
        product["review_count"] = _itemprop("reviewCount")

    if not product["availability"]:
        el = soup.find(attrs={"itemprop": "availability"})
        if el:
            v = el.get("content", "")
            product["availability"] = "In Stock" if "InStock" in v else "Out of Stock"

    # ── Tier 3: H1 / CSS class heuristics ────────────────────────────────────
    if not product["name"]:
        h1 = soup.find("h1")
        product["name"] = h1.get_text(strip=True) if h1 else None

    if not product["price"]:
        for sel in [
            '[class*="sale-price"]', '[class*="sale_price"]',
            '[class*="current-price"]', '[class*="current_price"]',
            '[class*="product-price"]', '[class*="product_price"]',
            '[class*="prix-solde"]', '[class*="prix"]',
            '[class*="price__amount"]', '[class*="price-item"]',
            '[class*="price"]', '[id*="price"]',
        ]:
            el = soup.select_one(sel)
            if el:
                txt = el.get_text(" ", strip=True)
                if re.search(r"\d", txt) and len(txt) < 40:
                    product["price"] = txt
                    break

    # ── Tier 4: Regex on visible text ─────────────────────────────────────────
    if not product["sku"]:
        text = soup.get_text(" ")
        m = re.search(
            r"(?:SKU|Ref(?:érence)?|Référence|MPN|Modèle|Model|Item\s*#|Article|Code)\s*[:\-#]?\s*([A-Z0-9][A-Z0-9\-_.]{3,25})",
            text, re.IGNORECASE,
        )
        product["sku"] = m.group(1).strip() if m else None

    # ── Images fallback ───────────────────────────────────────────────────────
    if not product["images"]:
        for img in soup.find_all("img")[:20]:
            src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
            if not src:
                continue
            src = urljoin(url, src)
            lower = src.lower()
            if any(x in lower for x in ("product", "item", "large", "zoom", "full", "main")):
                product["images"].append(src)
        product["images"] = list(dict.fromkeys(product["images"]))[:6]

    # ── Category from breadcrumb ──────────────────────────────────────────────
    if not product["category"]:
        crumbs = soup.select(
            '[class*="breadcrumb"] a, [aria-label="breadcrumb"] a, '
            'nav[aria-label*="bread"] a, ol.breadcrumb a, .breadcrumbs a'
        )
        if crumbs:
            cats = [a.get_text(strip=True) for a in crumbs if a.get_text(strip=True)]
            # Skip first (Home) and last (product itself), keep middle = category
            if len(cats) >= 3:
                product["category"] = " > ".join(cats[1:-1])
            elif len(cats) == 2:
                product["category"] = cats[0]

    # ── Attributes from spec tables ────────────────────────────────────────────
    for table in soup.select(
        'table.specifications, table.product-specs, table.tech-specs, '
        '[class*="spec"] table, [class*="caracteristique"] table, '
        '[class*="attribute"] table, table[class*="detail"]'
    )[:3]:
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) == 2:
                key = cells[0].get_text(strip=True)
                val = cells[1].get_text(strip=True)
                if key and val and len(key) < 60 and key not in product["attributes"]:
                    product["attributes"][key] = val

    # dl / dt-dd attribute lists (common in Magento / WooCommerce)
    for dl in soup.select("dl.product-attributes, [class*='attributes'] dl, [class*='specs'] dl")[:3]:
        terms = dl.find_all("dt")
        defs = dl.find_all("dd")
        for dt, dd in zip(terms, defs):
            key = dt.get_text(strip=True)
            val = dd.get_text(strip=True)
            if key and val and len(key) < 60:
                product["attributes"][key] = val

    return product


# ── Crawl job ─────────────────────────────────────────────────────────────────

async def _run_crawl(job: CrawlJob):
    job.status = "running"
    config = job.config

    try:
        parsed = urlparse(config.url.strip())
        if not parsed.scheme:
            config.url = "https://" + config.url
            parsed = urlparse(config.url)
        domain = parsed.netloc.lower()
        start_url = _clean_url(config.url)

        job.push({"type": "status", "msg": f"Starting crawl of {domain}…"})

        visited: set[str] = set()
        queue: deque[str] = deque([start_url])
        sem = asyncio.Semaphore(config.concurrency)

        async def handle(url: str):
            async with sem:
                if url in visited:
                    return
                if len(visited) >= config.max_pages:
                    return
                if len(job.products) >= config.max_products:
                    return

                visited.add(url)
                job.pages_scanned = len(visited)

                result = await asyncio.to_thread(_fetch, url)
                if result is None:
                    return

                soup, final_url = result

                # Discover all internal links on this page and add unseen ones to the queue
                links = await asyncio.to_thread(_extract_links, soup, final_url, domain)
                new_links = [l for l in links if l not in visited]
                for link in new_links:
                    queue.append(link)
                job.pages_queued = len(visited) + len(queue)

                # Detect and extract product pages
                is_product = await asyncio.to_thread(_is_product_page, soup, final_url)

                if is_product:
                    product = await asyncio.to_thread(_extract_product, soup, final_url)
                    job.products.append(product)
                    job.push({
                        "type": "product",
                        "data": product,
                        "stats": {
                            "scanned": job.pages_scanned,
                            "found": len(job.products),
                            "queued": job.pages_queued,
                        },
                    })
                else:
                    job.push({
                        "type": "progress",
                        "url": url,
                        "stats": {
                            "scanned": job.pages_scanned,
                            "found": len(job.products),
                            "queued": job.pages_queued,
                        },
                    })

        # BFS — process queue in batches of concurrency size
        while queue and len(visited) < config.max_pages and len(job.products) < config.max_products:
            batch = []
            while queue and len(batch) < config.concurrency * 2:
                url = queue.popleft()
                if url not in visited:
                    batch.append(url)
            if batch:
                await asyncio.gather(*[handle(u) for u in batch])

        job.status = "done"
        job.push({
            "type": "done",
            "stats": {"scanned": job.pages_scanned, "found": len(job.products)},
            "elapsed": job.elapsed(),
        })

    except Exception as e:
        job.status = "error"
        job.push({"type": "error", "msg": str(e) or type(e).__name__})
    finally:
        job.done = True


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/jobs")
async def create_crawl_job(config: CrawlConfig):
    job_id = str(uuid.uuid4())
    job = CrawlJob(job_id, config)
    crawl_jobs[job_id] = job
    asyncio.create_task(_run_crawl(job))
    return {"job_id": job_id}


@router.get("/jobs/{job_id}/stream")
async def stream_crawl(job_id: str):
    if job_id not in crawl_jobs:
        raise HTTPException(404, "Job not found")
    job = crawl_jobs[job_id]

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
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/jobs/{job_id}/results")
async def get_crawl_results(job_id: str, fmt: str = "json"):
    if job_id not in crawl_jobs:
        raise HTTPException(404, "Job not found")
    job = crawl_jobs[job_id]

    if fmt == "csv":
        import csv, io
        if not job.products:
            from fastapi.responses import Response
            return Response(content="No products found", media_type="text/csv")

        # Collect all attribute keys across all products
        attr_keys = []
        seen_attr = set()
        for p in job.products:
            for k in p.get("attributes", {}):
                if k not in seen_attr:
                    attr_keys.append(k)
                    seen_attr.add(k)

        base_fields = ["name", "price", "currency", "sku", "brand", "availability",
                       "description", "rating", "review_count", "category", "url", "images"]
        all_fields = base_fields + attr_keys

        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=all_fields, extrasaction="ignore")
        w.writeheader()
        for p in job.products:
            row = {k: p.get(k, "") for k in base_fields}
            row["images"] = " | ".join(p.get("images") or [])
            row.update(p.get("attributes", {}))
            w.writerow(row)

        return StreamingResponse(
            io.BytesIO(buf.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=crawled_products.csv"},
        )

    return {"products": job.products, "status": job.status, "elapsed": job.elapsed()}
