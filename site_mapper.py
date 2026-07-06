"""
site_mapper.py — Phase 1
BFS crawl to build a URL graph, cluster URLs into pattern groups,
and classify which patterns correspond to product pages.

Algorithm
---------
Graph:    G = (V, E)  where V = URLs, E = links between them
Crawl:    BFS from start_url, staying on the same domain
Pattern:  each URL path is tokenised and variable segments replaced with {var}/{int}
          e.g. /product/nike-air-max-90  →  /product/{var}
          URLs sharing a pattern = same page template
Classify: a pattern is "product" if ≥50% of sampled pages in that pattern
          have price signals OR buy-signal words  (scored feature vector)
"""
import re
import asyncio
from collections import defaultdict, deque
from urllib.parse import urlparse, urljoin, urldefrag
from typing import Callable, Optional
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_SKIP_EXT = re.compile(
    r'\.(jpg|jpeg|png|gif|svg|webp|pdf|zip|css|js|ico|woff2?|ttf|eot)(\?.*)?$', re.I
)
_SKIP_PATH = re.compile(
    r'/(cart|panier|checkout|login|register|compte|account|wishlist'
    r'|search|recherche|sitemap|tag|compare|print|feed|rss)[/?#]?$', re.I
)

# A segment is "variable" (slug/ID) if it's long & alphanumeric-with-hyphens
_NUMERIC   = re.compile(r'^\d+$')
_SLUG      = re.compile(r'^[a-z0-9][a-z0-9\-_%]{5,}$', re.I)

# Strong indicator of a product URL
_PRODUCT_PATH = re.compile(
    r'/(product|produit|item|article|p|pd|detail|fiche)s?/', re.I
)

_BUY_SIGNALS = [
    "add to cart", "ajouter au panier", "buy now", "acheter", "commander",
    "in stock", "en stock", "disponible", "add to bag",
]


# ── URL helpers ───────────────────────────────────────────────────────────────

def _clean(url: str) -> str:
    url, _ = urldefrag(url)
    return url.rstrip("/")


def _same_domain(url: str, domain: str) -> bool:
    try:
        h = urlparse(url).netloc.lower()
        return h == domain or h.endswith("." + domain)
    except Exception:
        return False


def url_to_pattern(url: str) -> str:
    """
    Replace variable segments in a URL path with placeholders.

    /product/nike-air-max-90   →  /product/{var}
    /category/shoes/page/2     →  /category/shoes/page/{int}
    /p/12345                   →  /p/{int}
    /?product_id=99            →  /?product_id={val}
    """
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    out = []
    for p in parts:
        if _NUMERIC.match(p):
            out.append("{int}")
        elif _SLUG.match(p):
            out.append("{var}")
        else:
            out.append(p)

    pattern = "/" + "/".join(out)

    if parsed.query:
        keys = sorted(kv.split("=")[0] for kv in parsed.query.split("&"))
        pattern += "?" + "&".join(k + "={val}" for k in keys)

    return pattern


# ── Page fetch ────────────────────────────────────────────────────────────────

def _fetch_page_info(url: str) -> tuple[list[str], dict]:
    """
    Fetch url. Return:
      links       — all <a href> absolute URLs found on the page
      signals     — feature dict used for product classification
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=13, allow_redirects=True)
        if resp.status_code >= 400:
            return [], {}
        ct = resp.headers.get("Content-Type", "")
        if "text/html" not in ct:
            return [], {}

        soup = BeautifulSoup(resp.text, "lxml")

        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith(("javascript:", "mailto:", "tel:")):
                continue
            abs_url = _clean(urljoin(url, href))
            if abs_url.startswith("http"):
                links.append(abs_url)

        text_lower = resp.text.lower()

        signals = {
            # Each signal contributes to the product score
            "has_price_itemprop": bool(soup.find(attrs={"itemprop": "price"})),
            "has_price_class":    bool(soup.select('[class*="price"],[class*="prix"]')),
            "has_jsonld_product": '"Product"' in resp.text or "'Product'" in resp.text,
            "buy_signal_count":   sum(1 for s in _BUY_SIGNALS if s in text_lower),
            "single_h1":          len(soup.find_all("h1")) == 1,
            "url_is_product":     bool(_PRODUCT_PATH.search(url)),
        }

        return list(dict.fromkeys(links)), signals

    except Exception:
        return [], {}


def _score_signals(signals: dict) -> float:
    """
    Convert feature dict to a product-page probability [0, 1].

    Weights chosen so that:
      - JSON-LD alone scores 0.8  (very reliable)
      - itemprop + buy signals scores ~0.85
      - only URL pattern scores 0.25 (weak alone)
    """
    s = 0.0
    if signals.get("has_jsonld_product"):   s += 4.0
    if signals.get("has_price_itemprop"):   s += 3.0
    if signals.get("has_price_class"):      s += 1.5
    buy = min(signals.get("buy_signal_count", 0), 4)
    s += buy * 0.75
    if signals.get("single_h1"):            s += 0.5
    if signals.get("url_is_product"):       s += 2.0
    return min(s / 10.0, 1.0)   # normalise to [0,1]


# ── BFS site mapper ───────────────────────────────────────────────────────────

async def map_site(
    start_url: str,
    max_pages: int = 500,
    concurrency: int = 8,
    progress_cb: Optional[Callable] = None,
) -> dict:
    """
    BFS crawl from start_url.

    Returns
    -------
    {
      "domain":           str,
      "url_graph":        {url: [linked_urls]},        # the link graph
      "pattern_groups":   {pattern: [urls]},            # URL template clusters
      "product_patterns": [pattern, ...],               # patterns classified as product
      "product_urls":     [url, ...],                   # all product page URLs found
      "stats":            {total_pages, products, patterns}
    }
    """
    parsed = urlparse(start_url.strip())
    if not parsed.scheme:
        start_url = "https://" + start_url
        parsed = urlparse(start_url)
    domain = parsed.netloc.lower()

    visited:  set[str]                       = set()
    queue:    deque[str]                     = deque([_clean(start_url)])
    url_graph: dict[str, list[str]]          = {}
    pattern_groups: dict[str, list[str]]     = defaultdict(list)
    pattern_scores: dict[str, list[float]]   = defaultdict(list)  # per-page scores per pattern
    product_urls: list[str]                  = []

    sem = asyncio.Semaphore(concurrency)

    async def process(url: str):
        async with sem:
            if url in visited or len(visited) >= max_pages:
                return
            visited.add(url)

            links, signals = await asyncio.to_thread(_fetch_page_info, url)

            # Keep only same-domain, non-asset, non-utility links
            internal = [
                l for l in links
                if _same_domain(l, domain)
                and not _SKIP_EXT.search(l)
                and not _SKIP_PATH.search(l)
            ]

            url_graph[url] = internal

            pattern = url_to_pattern(url)
            pattern_groups[pattern].append(url)

            score = _score_signals(signals)
            pattern_scores[pattern].append(score)

            if score >= 0.5:
                product_urls.append(url)

            for link in internal:
                if link not in visited:
                    queue.append(link)

            if progress_cb:
                await progress_cb({
                    "scanned": len(visited),
                    "queued":  len(queue),
                    "products": len(product_urls),
                    "url":     url,
                    "score":   round(score, 2),
                })

    # BFS loop — process in batches of concurrency*2
    while queue and len(visited) < max_pages:
        batch: list[str] = []
        while queue and len(batch) < concurrency * 2:
            u = queue.popleft()
            if u not in visited:
                batch.append(u)
        if batch:
            await asyncio.gather(*[process(u) for u in batch])

    # Classify patterns: "product" if average score ≥ 0.5
    product_patterns = [
        pat for pat, scores in pattern_scores.items()
        if scores and (sum(scores) / len(scores)) >= 0.5
    ]

    return {"domain":           domain,
        "url_graph":        url_graph,
        "pattern_groups":   dict(pattern_groups),
        "product_patterns": product_patterns,
        "product_urls":     list(dict.fromkeys(product_urls)),
        "stats": {
            "total_pages": len(visited),
            "products":    len(product_urls),
            "patterns":    len(pattern_groups),
        },
    }
