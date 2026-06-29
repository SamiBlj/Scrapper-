"""
prospects.py — Business prospect finder.
Searches for businesses in a niche + location, extracts contact info,
and optionally finds their LinkedIn page.

Count as 1 IFF LinkedIn is included.

Google Maps Places API is used as primary source when a key is provided —
gives structured address, lat/lng, phone, and website straight from the API.
Falls back to DuckDuckGo / Bing when no key is supplied.

GET THE BUSINESSES FROM LINKED IN +GOOGLE MAPS + SCRAPPING --> cross checking all of them.

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

router = APIRouter(prefix="/api/prospects")
prospect_jobs: dict = {}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

PHONE_RE = re.compile(r'(?:\+?\d[\d\s\-().]{7,15}\d)')
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

PLACES_TEXT_SEARCH = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS     = "https://maps.googleapis.com/maps/api/place/details/json"


# ── Models ────────────────────────────────────────────────────────────────────

class ProspectConfig(BaseModel):
    niche: str
    location: str = ""
    engine: str = "duckduckgo"
    max_results: int = 20
    find_linkedin: bool = True
    find_email: bool = True
    serpapi_key: Optional[str] = None
    gmaps_key: Optional[str] = None        # Google Maps / Places API key


class ProspectJob:
    def __init__(self, job_id: str, config: ProspectConfig):
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


# ── Contact extraction helpers ────────────────────────────────────────────────

def _extract_phone(text: str) -> Optional[str]:
    m = PHONE_RE.search(text)
    if m:
        if len(re.sub(r'[\s\-().]', '', m.group())) >= 7:
            return m.group().strip()
    return None


def _extract_email(text: str) -> Optional[str]:
    m = EMAIL_RE.search(text)
    if m:
        e = m.group()
        if not any(e.endswith(x) for x in ['.png', '.jpg', '.gif', '.css', '.js']):
            return e
    return None


def _scrape_business_page(url: str) -> dict:
    """Visit a business website and extract phone, email, address."""
    result = {"phone": None, "email": None, "address": None}
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code >= 400:
            return result
        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(" ", strip=True)[:15000]
        result["phone"] = _extract_phone(text)
        result["email"] = _extract_email(text)
        for el in soup.find_all(attrs={"itemprop": "address"}):
            addr = el.get_text(", ", strip=True)
            if len(addr) > 5:
                result["address"] = addr[:120]
                break
    except Exception:
        pass
    return result


def _extract_domain(url: str) -> str:
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return url


# ── Google Places API ─────────────────────────────────────────────────────────

def _places_text_search(query: str, key: str, page_token: Optional[str] = None) -> dict:
    params = {"key": key}
    if page_token:
        params["pagetoken"] = page_token
    else:
        params["query"] = query
    resp = requests.get(PLACES_TEXT_SEARCH, params=params, timeout=12)
    return resp.json()


def _places_details(place_id: str, key: str) -> dict:
    """Fetch phone + website for a place (one API call per place)."""
    params = {
        "place_id": place_id,
        "fields": "formatted_phone_number,website,url",
        "key": key,
    }
    try:
        resp = requests.get(PLACES_DETAILS, params=params, timeout=10)
        return resp.json().get("result", {})
    except Exception:
        return {}


async def _search_google_places(niche: str, location: str, max_results: int, key: str) -> list:
    """
    Use Places Text Search to get businesses, then fetch Details for each
    to get phone + website. Returns list of candidate dicts ready for enrichment.
    """
    query = f"{niche} {location}".strip()
    candidates = []
    page_token = None

    while len(candidates) < max_results:
        data = await asyncio.to_thread(_places_text_search, query, key, page_token)

        if data.get("status") not in ("OK", "ZERO_RESULTS"):
            raise RuntimeError(f"Places API error: {data.get('status')} — {data.get('error_message', '')}")

        for place in data.get("results", []):
            loc = place.get("geometry", {}).get("location", {})
            place_id = place.get("place_id", "")

            # Fetch phone + website via Details
            details = await asyncio.to_thread(_places_details, place_id, key)

            website = details.get("website", "")
            phone   = details.get("formatted_phone_number", "")
            maps_url = details.get("url", f"https://www.google.com/maps/place/?q=place_id:{place_id}")

            candidates.append({
                "title":   place.get("name", ""),
                "url":     website or maps_url,
                "website": website,
                "phone":   phone,
                "address": place.get("formatted_address", ""),
                "snippet": place.get("name", "") + " " + place.get("formatted_address", ""),
                "lat":     loc.get("lat"),
                "lng":     loc.get("lng"),
                "rating":  place.get("rating"),
                "place_id": place_id,
            })

            if len(candidates) >= max_results:
                break

        page_token = data.get("next_page_token")
        if not page_token:
            break
        await asyncio.sleep(2)   # Places API requires a short delay before using next_page_token

    return candidates


# ── Fallback: DDG / Bing search ───────────────────────────────────────────────

def _ddg_html_search(query: str, max_results: int) -> list[dict]:
    """
    Scrape DDG's plain-HTML endpoint — avoids the ddgs library's
    unfixable 'Separator is not found' chunked-transfer parser bug.
    """
    import urllib.parse, time
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
                        "url": href, "website": href,
                        "title": a.get_text(strip=True),
                        "snippet": snip.get_text(strip=True) if snip else "",
                        "phone": None, "address": None,
                        "lat": None, "lng": None, "rating": None,
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


async def _search_businesses_web(query: str, engine: str, max_results: int) -> list:
    results = []

    if engine in ("duckduckgo", "all"):
        try:
            results = await asyncio.to_thread(_ddg_html_search, query, max_results)
        except Exception:
            pass

    if not results or engine == "bing":
        try:
            resp = await asyncio.to_thread(
                lambda: requests.get(
                    "https://www.bing.com/search",
                    params={"q": query, "count": max_results},
                    headers=HEADERS, timeout=12,
                )
            )
            soup = BeautifulSoup(resp.text, "lxml")
            for li in soup.select("li.b_algo")[:max_results]:
                a = li.select_one("h2 a")
                cap = li.select_one(".b_caption p")
                if a:
                    url = a.get("href", "")
                    results.append({
                        "url": url, "website": url,
                        "title": a.get_text(strip=True),
                        "snippet": cap.get_text(strip=True) if cap else "",
                        "phone": None, "address": None,
                        "lat": None, "lng": None, "rating": None,
                    })
        except Exception:
            pass

    return results


def _is_noise(url: str) -> bool:
    noise = ["linkedin.com", "facebook.com", "twitter.com", "instagram.com",
             "yelp.com", "tripadvisor.com", "yellowpages", "whitepages",
             "wikipedia.org", "youtube.com", "reddit.com", "google.com",
             "amazon.com", "bing.com", "duckduckgo.com"]
    return any(n in url.lower() for n in noise)


# ── LinkedIn finder ───────────────────────────────────────────────────────────

async def _find_linkedin(business_name: str, location: str, engine: str) -> Optional[str]:
    query = f'site:linkedin.com/company "{business_name}" {location}'
    try:
        if engine in ("duckduckgo", "all"):
            raw = await asyncio.to_thread(_ddg_html_search, query, 3)
            for r in raw:
                if "linkedin.com/company" in r["url"]:
                    return r["url"]
        resp = await asyncio.to_thread(
            lambda: requests.get(
                "https://www.bing.com/search",
                params={"q": query, "count": 3},
                headers=HEADERS, timeout=8,
            )
        )
        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.select("li.b_algo h2 a"):
            href = a.get("href", "")
            if "linkedin.com/company" in href:
                return href
    except Exception:
        pass
    return None


# ── Job runner ────────────────────────────────────────────────────────────────

async def _run_prospect_job(job: ProspectJob):
    job.status = "running"
    cfg = job.config

    # ── 1. Gather candidates ──────────────────────────────────────────────────
    if cfg.gmaps_key:
        job.push({"type": "status", "msg": f'Searching Google Maps for "{cfg.niche} {cfg.location}"…'})
        try:
            candidates = await _search_google_places(cfg.niche, cfg.location, cfg.max_results, cfg.gmaps_key)
        except RuntimeError as e:
            job.push({"type": "error", "msg": str(e)})
            job.status = "error"
            job.done = True
            return
    else:
        query = f"{cfg.niche} {cfg.location}".strip()
        job.push({"type": "status", "msg": f'Searching for "{query}"…'})
        raw = await _search_businesses_web(query, cfg.engine, cfg.max_results * 2)
        candidates = [r for r in raw if r["url"] and not _is_noise(r["url"])]
        candidates = list({r["url"]: r for r in candidates}.values())
        candidates = candidates[:cfg.max_results]

    if not candidates:
        job.push({"type": "error", "msg": "No results found. Try a different niche or location."})
        job.status = "error"
        job.done = True
        return

    job.push({"type": "status", "msg": f"Found {len(candidates)} candidates — enriching…"})

    # ── 2. Enrich each candidate ──────────────────────────────────────────────
    qualified = 0   # count = businesses WITH linkedin

    for i, c in enumerate(candidates):
        job.push({
            "type": "progress",
            "current": i,
            "total": len(candidates),
            "msg": f"[{i+1}/{len(candidates)}] {c['title'][:60]}…",
        })

        phone   = c.get("phone") or ""
        email   = ""
        address = c.get("address") or ""
        website = c.get("website") or c.get("url") or ""

        # Visit the website for extra contact info (skip if we already have phone from Places)
        if cfg.find_email or not phone:
            if website and website.startswith("http") and not _is_noise(website):
                page = await asyncio.to_thread(_scrape_business_page, website)
                phone = phone or page.get("phone") or ""
                email = page.get("email") or ""
                address = address or page.get("address") or ""

        # Snippet fallback for email
        if cfg.find_email and not email:
            email = _extract_email(c.get("snippet", "")) or ""

        # LinkedIn
        linkedin_url = ""
        if cfg.find_linkedin:
            linkedin_url = await _find_linkedin(c["title"], cfg.location, cfg.engine) or ""

        if linkedin_url:
            qualified += 1

        result = {
            "name":     c["title"],
            "domain":   _extract_domain(website) if website else "",
            "website":  website,
            "phone":    phone,
            "email":    email,
            "address":  address,
            "linkedin": linkedin_url,
            "lat":      c.get("lat"),
            "lng":      c.get("lng"),
            "rating":   c.get("rating"),
            "qualified": bool(linkedin_url),   # counts as 1 iff linkedin found
        }
        job.results.append(result)
        job.push({"type": "result", "data": result, "index": i, "qualified": qualified})

        await asyncio.sleep(0.3)

    job.status = "done"
    job.push({
        "type": "done",
        "total": len(job.results),
        "qualified": qualified,
        "elapsed": job.elapsed(),
    })
    job.done = True


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/jobs")
async def create_prospect_job(config: ProspectConfig):
    job_id = str(uuid.uuid4())
    job = ProspectJob(job_id, config)
    prospect_jobs[job_id] = job
    asyncio.create_task(_run_prospect_job(job))
    return {"job_id": job_id}


@router.get("/jobs/{job_id}/stream")
async def stream_prospect_job(job_id: str):
    from fastapi import HTTPException
    if job_id not in prospect_jobs:
        raise HTTPException(404, "Job not found")
    job = prospect_jobs[job_id]

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
async def get_prospect_results(job_id: str, fmt: str = "json"):
    from fastapi import HTTPException
    if job_id not in prospect_jobs:
        raise HTTPException(404, "Job not found")
    job = prospect_jobs[job_id]

    if fmt == "csv":
        import csv, io
        out = io.StringIO()
        if not job.results:
            from fastapi.responses import Response
            return Response(content="No results", media_type="text/csv")
        fields = [k for k in job.results[0].keys() if k not in ("lat", "lng", "qualified")]
        w = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(job.results)
        return StreamingResponse(
            io.BytesIO(out.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=prospects.csv"},
        )

    return {"results": job.results, "status": job.status, "elapsed": job.elapsed()}
