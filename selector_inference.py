"""
selector_inference.py — Phase 2
Fetches N product pages from the same site and compares their DOM trees
to find CSS selectors that reliably identify each product field.

Algorithm
---------
For each page:
  Walk every DOM node whose text passes a field's goal_test.
  Generate candidate CSS selectors for that node (itemprop → id → classes → parent>child).

Across all N pages:
  selector_counts[field][selector] = how many pages contain that selector with valid text

Best selector = highest count, tie-broken by specificity.
Confidence    = count / N pages.

A selector is accepted when confidence ≥ min_confidence (default 0.6).
"""
import re
import asyncio
from collections import defaultdict
from typing import Optional
import requests
from bs4 import BeautifulSoup, Tag

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
}

# ── Goal tests — one per field ────────────────────────────────────────────────
# Each function answers: "does this text look like a valid value for this field?"

PRICE_RE = re.compile(
    r'(?:[\$€£¥₹₩]\s*\d[\d\s,]*\.?\d{0,2})'
    r'|(?:\d[\d\s,]*\.?\d{0,2}\s*(?:€|EUR|USD|GBP|DZD|MAD|TND|DA|DH))',
)
SKU_RE    = re.compile(r'^[A-Z0-9][A-Z0-9\-_.]{3,25}$', re.I)
RATING_RE = re.compile(r'^[\d.]+ *(/ *[\d.]+| *out of *[\d.]+)?$')
_AVAIL_WORDS = {"stock", "disponible", "rupture", "available", "unavailable",
                "sold out", "in stock", "out of stock", "livraison", "epuise"}

# Class names that are generated/dynamic and should not appear in selectors
_GENERATED = re.compile(r'^(css-|sc-|[a-z]+-\d{3,}|[0-9a-f]{6,})', re.I)


def goal_test(field: str, text: str) -> bool:
    """Return True if text is a plausible value for field."""
    if not text or len(text) > 200:
        return False
    t = text.strip()
    if field == "price":
        return bool(PRICE_RE.search(t)) and len(t) < 40
    if field == "sku":
        return bool(SKU_RE.match(t))
    if field == "name":
        return 4 < len(t) < 150 and not t.replace(" ", "").isnumeric()
    if field == "availability":
        return any(w in t.lower() for w in _AVAIL_WORDS) and len(t) < 80
    if field == "brand":
        words = t.split()
        return 1 <= len(words) <= 4 and all(len(w) < 30 for w in words)
    if field == "rating":
        return bool(RATING_RE.match(t)) and len(t) < 15
    if field == "description":
        return len(t) > 30 and len(t.split()) > 5
    return False


# ── CSS selector generation ───────────────────────────────────────────────────

def _stable_classes(node: Tag) -> list[str]:
    """Return class names that are unlikely to be auto-generated."""
    return [c for c in node.get("class", [])
            if not _GENERATED.match(c) and len(c) <= 40]


def node_to_selectors(node: Tag) -> list[str]:
    """
    Generate candidate CSS selectors for node, from most to least specific.
    All returned selectors must be valid for soup.select_one().
    """
    sels: list[str] = []

    # 1. itemprop — most semantic, very stable
    ip = node.get("itemprop")
    if ip:
        sels.append(f'[itemprop="{ip}"]')
        sels.append(f'{node.name}[itemprop="{ip}"]')

    # 2. data attributes that imply semantic meaning
    for attr, val in node.attrs.items():
        if isinstance(val, str) and attr.startswith("data-") and any(
            k in attr for k in ("price", "sku", "ref", "id", "stock", "rating")
        ):
            sels.append(f'[{attr}]')

    # 3. id  (skip numeric/generated ids)
    node_id = node.get("id", "")
    if node_id and not re.match(r'^\d+$', node_id) and not _GENERATED.match(node_id):
        sels.append(f"#{node_id}")
        return sels  # id is unique — no need to add weaker selectors

    # 4. tag + up to 3 stable classes
    classes = _stable_classes(node)
    if classes:
        sels.append(node.name + "." + ".".join(classes[:3]))
        if len(classes) > 1:
            sels.append(node.name + "." + classes[0])   # single-class fallback

    # 5. parent > child  (adds context without being too brittle)
    parent = node.parent
    if parent and hasattr(parent, "name") and parent.name and parent.name != "[document]":
        p_classes = _stable_classes(parent)
        if p_classes and classes:
            sels.append(f"{parent.name}.{p_classes[0]} > {node.name}.{classes[0]}")
        elif p_classes:
            sels.append(f"{parent.name}.{p_classes[0]} > {node.name}")

    return sels


# ── Page analysis ─────────────────────────────────────────────────────────────

FIELDS = ["price", "name", "sku", "availability", "brand", "rating", "description"]


def _candidates_from_page(soup: BeautifulSoup) -> dict[str, list[str]]:
    """
    Walk every DOM node, find ones whose text passes each field's goal_test,
    and return their CSS selectors.

    Returns {field: [selector, ...]} (selectors may repeat across nodes).
    """
    candidates: dict[str, list[str]] = {f: [] for f in FIELDS}

    for node in soup.find_all(True):
        # Skip container nodes — we want the leaf element that holds the value
        if len(node.find_all()) > 5:
            continue
        if not hasattr(node, "get_text"):
            continue

        text = node.get_text(" ", strip=True)
        if not text:
            continue

        for field in FIELDS:
            if goal_test(field, text):
                for sel in node_to_selectors(node):
                    if sel:
                        candidates[field].append(sel)

    return candidates


def _fetch_page(url: str) -> Optional[BeautifulSoup]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=14, allow_redirects=True)
        if resp.status_code >= 400 or "text/html" not in resp.headers.get("Content-Type", ""):
            return None
        return BeautifulSoup(resp.text, "lxml")
    except Exception:
        return None


# ── Main inference function ───────────────────────────────────────────────────

async def infer_selectors(
    product_urls: list[str],
    sample_size: int = 10,
    min_confidence: float = 0.6,
) -> dict:
    """
    Fetch up to sample_size product pages concurrently.
    For each field, find the CSS selector that appears most consistently.

    Returns
    -------
    {
      "price":        {"selector": "span.price-amount",  "confidence": 0.9},
      "name":         {"selector": "h1.product-title",   "confidence": 1.0},
      "sku":          {"selector": '[itemprop="sku"]',   "confidence": 0.8},
      "availability": None,   ← not found at min_confidence
      ...
    }
    """
    sample = list(dict.fromkeys(product_urls))[:sample_size]

    soups = await asyncio.gather(
        *[asyncio.to_thread(_fetch_page, u) for u in sample]
    )
    soups = [s for s in soups if s is not None]
    n = len(soups)

    if n == 0:
        return {}

    # Count how many pages each selector works for, per field
    # selector_counts[field][selector] = page count
    selector_counts: dict[str, dict[str, int]] = {f: defaultdict(int) for f in FIELDS}

    for soup in soups:
        page_candidates = _candidates_from_page(soup)
        for field, sels in page_candidates.items():
            # Count each unique selector once per page (no double-counting)
            for sel in set(sels):
                selector_counts[field][sel] += 1

    result: dict = {}

    for field in FIELDS:
        counts = selector_counts[field]
        if not counts:
            result[field] = None
            continue

        # Best = highest page count; ties broken by length (longer = more specific)
        best = max(counts, key=lambda s: (counts[s], len(s)))
        confidence = counts[best] / n

        if confidence >= min_confidence:
            result[field] = {
                "selector":   best,
                "confidence": round(confidence, 2),
                "source":     "inference",
            }
        else:
            result[field] = None

    # h1 is a reliable name fallback even at low confidence
    if not result.get("name"):
        result["name"] = {"selector": "h1", "confidence": 0.5, "source": "fallback"}

    return result
