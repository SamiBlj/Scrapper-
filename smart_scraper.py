"""
smart_scraper.py — Pipeline orchestrator + BFS DOM extractor
Phases:
  1  site_mapper        → URL graph + product URL list + pattern map
  2  selector_inference → CSS selector per field (from DOM comparison)
  3  ai_extractor       → LLM fills any fields still missing  (optional)
  4  BFS DOM extractor  → extract values from every product page

BFS extractor logic
-------------------
For each field:
  Fast path  → try the CSS selector from the site map (O(1))
  BFS path   → traverse DOM tree breadth-first, return the SHALLOWEST
               node whose text passes the field's goal_test.

BFS is used (not DFS) because:
  - Product pages display the main price/name near the top of the DOM tree.
  - DFS would go deep into recommendation widgets or footers first.
  - Shallowest match = the element the site explicitly places for that field.
"""
import asyncio
import json
import re
import uuid
from collections import deque
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from site_mapper import map_site
from selector_inference import infer_selectors, goal_test, _fetch_page, FIELDS
from ai_extractor import extract_selectors_with_llm

router = APIRouter(prefix="/api/smart")
smart_jobs: dict = {}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
}


# ── Models ────────────────────────────────────────────────────────────────────

class SmartScrapeConfig(BaseModel):
    url:          str
    max_pages:    int  = 200   # Phase 1: how many pages to map
    sample_size:  int  = 10    # Phase 2: pages used for selector inference
    max_products: int  = 500   # Phase 4: max products to extract
    use_llm:      bool = True  # Phase 3: call ollama for missing fields
    concurrency:  int  = 6


class SmartJob:
    def __init__(self, job_id: str, config: SmartScrapeConfig):
        self.job_id       = job_id
        self.config       = config
        self.events:  list = []
        self.products:list = []
        self.selector_map: dict = {}
        self.site_map:     dict = {}
        self.done         = False
        self.status       = "pending"
        self.phase        = "idle"
        self.start_time   = datetime.now()

    def push(self, event: dict):
        self.events.append(event)

    def elapsed(self):
        return round((datetime.now() - self.start_time).total_seconds(), 1)


# ── BFS DOM extractor ─────────────────────────────────────────────────────────

def bfs_extract(soup: BeautifulSoup, field: str,
                selector: Optional[str] = None) -> Optional[str]:
    """
    Extract the value for one field from a parsed page.

    Fast path: if a CSS selector is known, try it first.
      soup.select_one(selector) → get_text → goal_test → return

    BFS fallback: traverse every node in the DOM breadth-first.
      The SHALLOWEST node whose text passes goal_test is returned.
      This finds the most prominent match, not the first-in-source-order.

    Returns the extracted string, or None if nothing found.
    """
    # ── Fast path ─────────────────────────────────────────────────────────────
    if selector:
        try:
            el = soup.select_one(selector)
            if el:
                text = el.get("content") or el.get_text(" ", strip=True)
                if text and goal_test(field, text.strip()):
                    return text.strip()
        except Exception:
            pass

    # ── BFS path ──────────────────────────────────────────────────────────────
    root = soup.body or soup
    queue: deque = deque([root])
    seen:  set   = set()

    while queue:
        node = queue.popleft()
        nid  = id(node)
        if nid in seen:
            continue
        seen.add(nid)

        if hasattr(node, "get_text"):
            # Leaf-ish: don't inspect huge containers
            if len(getattr(node, "contents", [])) <= 5:
                text = node.get_text(" ", strip=True)
                if text and goal_test(field, text):
                    return text.strip()

        for child in getattr(node, "children", []):
            if hasattr(child, "name") and child.name:
                queue.append(child)

    return None


def extract_product(soup: BeautifulSoup, url: str, selector_map: dict) -> dict:
    """
    Run bfs_extract for every field, using the selector map as the fast path.
    Also extracts images, description, and category (which don't use goal_test).
    """
    product: dict = {"url": url}

    for field in FIELDS:
        entry    = selector_map.get(field)
        selector = entry["selector"] if isinstance(entry, dict) else None
        product[field] = bfs_extract(soup, field, selector)

    # Images — goal_test doesn't apply here; match by src keyword
    images: list[str] = []
    for img in soup.find_all("img")[:25]:
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
        if not src:
            continue
        src = urljoin(url, src)
        if any(k in src.lower() for k in ("product", "item", "large", "main", "zoom", "full")):
            images.append(src)
    product["images"] = list(dict.fromkeys(images))[:6]

    # Category from breadcrumb
    crumbs = soup.select(
        '[class*="breadcrumb"] a, nav[aria-label*="bread"] a, '
        'ol.breadcrumb a, .breadcrumbs a'
    )
    if crumbs:
        cats = [a.get_text(strip=True) for a in crumbs if a.get_text(strip=True)]
        product["category"] = " > ".join(cats[1:-1]) if len(cats) > 2 else (cats[0] if cats else None)
    else:
        product["category"] = None

    return product


# ── Pipeline ──────────────────────────────────────────────────────────────────

async def _run(job: SmartJob):
    job.status = "running"
    cfg = job.config

    try:
        # ── Phase 1: Site mapping ─────────────────────────────────────────────
        print("Initiating Phase 1: Site mapping")
        job.phase = "mapping"
        job.push({"type": "phase", "phase": "mapping",
                  "msg": f"Phase 1 — Mapping {cfg.url}…"})

        async def on_map_progress(stats: dict):
            job.push({"type": "mapping_progress", **stats})

        site_map = await map_site(
            cfg.url,
            max_pages=cfg.max_pages,
            concurrency=cfg.concurrency,
            progress_cb=on_map_progress,
        )
        print(site_map)
        job.site_map = site_map

        product_urls = site_map["product_urls"]
        job.push({
            "type":             "mapping_done",
            "stats":            site_map["stats"],
            "product_urls":     len(product_urls),
            "patterns":         list(site_map["pattern_groups"].keys()),
            "product_patterns": site_map["product_patterns"],
        })

        if not product_urls:
            print("no product url")
            job.push({"type": "error", "msg": "No product pages found during mapping."})
            return

        # ── Phase 2: Selector inference ───────────────────────────────────────
        print("Initiating Phase 2: Selector inference")
        job.phase = "inferring"
        n_sample = min(cfg.sample_size, len(product_urls))
        job.push({"type": "phase", "phase": "inferring",
                  "msg": f"Phase 2 — Inferring selectors from {n_sample} product pages…"})

        selector_map = await infer_selectors(product_urls, sample_size=n_sample)
        job.selector_map = selector_map

        found   = [f for f, v in selector_map.items() if v]
        missing = [f for f in FIELDS if not selector_map.get(f)]

        job.push({
            "type":         "selectors_found",
            "selector_map": selector_map,
            "found":        found,
            "missing":      missing,
        })

        # ── Phase 3: LLM fills missing selectors ─────────────────────────────
        print("initiating Phase 3: LLM fill missing selectors")
        if missing and cfg.use_llm:
            job.phase = "llm"
            job.push({"type": "phase", "phase": "llm",
                      "msg": f"Phase 3 — LLM finding selectors for: {', '.join(missing)}…"})

            try:
                sample_html = await asyncio.to_thread(
                    lambda: requests.get(
                        product_urls[0], headers=HEADERS, timeout=15
                    ).text
                )
                llm_result = await asyncio.to_thread(
                    extract_selectors_with_llm, sample_html, missing
                )
                for field, sel in llm_result.items():
                    if sel:
                        selector_map[field] = {
                            "selector":   sel,
                            "confidence": 0.6,
                            "source":     "llm",
                        }
                job.selector_map = selector_map
                job.push({"type": "llm_done", "added": llm_result})
            except Exception as e:
                job.push({"type": "llm_error", "msg": str(e)})

        # ── Phase 4: Extract all products ─────────────────────────────────────
        print("Initiating Phase 4: Extract all products")
        job.phase  = "extracting"
        total      = min(len(product_urls), cfg.max_products)
        sem        = asyncio.Semaphore(cfg.concurrency)

        job.push({"type": "phase", "phase": "extracting",
                  "msg": f"Phase 4 — Extracting {total} products…"})

        async def extract_one(i: int, url: str):
            async with sem:
                if len(job.products) >= cfg.max_products:
                    return
                soup = await asyncio.to_thread(_fetch_page, url)
                if soup is None:
                    return
                product = extract_product(soup, url, selector_map)
                job.products.append(product)
                job.push({
                    "type":    "product",
                    "data":    product,
                    "index":   i,
                    "total":   total,
                })

        await asyncio.gather(
            *[extract_one(i, u) for i, u in enumerate(product_urls[:total])]
        )

        job.status = "done"
        job.push({
            "type":            "done",
            "total_products":  len(job.products),
            "elapsed":         job.elapsed(),
            "selector_map":    job.selector_map,
        })

    except Exception as e:
        job.status = "error"
        job.push({"type": "error", "msg": str(e) or type(e).__name__})
    finally:
        job.done = True


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/jobs")
async def create_smart_job(config: SmartScrapeConfig):
    job_id = str(uuid.uuid4())
    job = SmartJob(job_id, config)
    smart_jobs[job_id] = job
    asyncio.create_task(_run(job))
    return {"job_id": job_id}


@router.get("/jobs/{job_id}/stream")
async def stream_smart(job_id: str):
    if job_id not in smart_jobs:
        raise HTTPException(404, "Job not found")
    job = smart_jobs[job_id]

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

    return StreamingResponse(
        gen(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/jobs/{job_id}/results")
async def get_smart_results(job_id: str, fmt: str = "json"):
    if job_id not in smart_jobs:
        raise HTTPException(404, "Job not found")
    job = smart_jobs[job_id]

    if fmt == "csv":
        import csv, io
        if not job.products:
            from fastapi.responses import Response
            return Response(content="No products", media_type="text/csv")
        base_fields = ["name", "price", "sku", "brand", "availability",
                       "rating", "description", "category", "url", "images"]
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=base_fields, extrasaction="ignore")
        w.writeheader()
        for p in job.products:
            row = dict(p)
            row["images"] = " | ".join(row.get("images") or [])
            w.writerow(row)
        return StreamingResponse(
            io.BytesIO(buf.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=smart_products.csv"},
        )

    return {
        "products":     job.products,
        "selector_map": job.selector_map,
        "status":       job.status,
        "elapsed":      job.elapsed(),
    }
