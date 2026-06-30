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


def _is_noise(url: str) -> bool:
    noise = ["linkedin.com", "facebook.com", "twitter.com", "instagram.com",
             "yelp.com", "tripadvisor.com", "yellowpages", "whitepages",
             "wikipedia.org", "youtube.com", "reddit.com", "google.com",
             "amazon.com", "bing.com", "duckduckgo.com"]
    return any(n in url.lower() for n in noise)


def _li_clean_url(url: str) -> str:
    return url.split("?")[0].rstrip("/")


def _li_name_from_url(url: str) -> str:
    try:
        slug = url.rstrip("/").split("/company/")[-1].split("/")[0]
        return slug.replace("-", " ").title()
    except Exception:
        return url


# ── STEP 1: LinkedIn company search ──────────────────────────────────────────

async def _search_linkedin_companies(niche: str, location: str, engine: str,
                                     max_results: int) -> list:
    """Search for LinkedIn company pages matching niche + location."""
    # Quoted niche = exact match (fewer results). Unquoted = broader index hits.
    # We run both and merge.
    query_broad  = f'site:linkedin.com/company {niche} {location}'
    query_exact  = f'site:linkedin.com/company "{niche}" {location}'
    query = query_broad  # used by _ddg_collect / _bing_collect below

    def _ddg_collect():
        out = []
        try:
            from ddgs import DDGS
            for r in DDGS().text(query, max_results=max_results * 50):
                href = r.get("href", "")
                if href.startswith("http") and "linkedin.com/company" in href:
                    clean = _li_clean_url(href)
                    title = (r.get("title", "")
                               .replace(" | LinkedIn", "")
                               .replace(" - LinkedIn", "")
                               .strip()) or _li_name_from_url(clean)
                    out.append({"linkedin_url": clean, "title": title,
                                "snippet": r.get("body", "")})
        except Exception:
            pass
        return out

    def _bing_collect():
        # Bing HTML only returns 10 results per request regardless of count=.
        # We paginate with first= to collect up to max_results pages.
        out = []
        pages_needed = max(1, -(-max_results // 10))  # ceil division
        for page in range(pages_needed):
            try:
                resp = requests.get(
                    "https://www.bing.com/search",
                    params={"q": query, "count": 10, "first": page * 10 + 1},
                    headers=HEADERS, timeout=15,
                )
                soup = BeautifulSoup(resp.text, "lxml")
                found_on_page = 0
                for li in soup.select("li.b_algo"):
                    a = li.select_one("h2 a")
                    cap = li.select_one(".b_caption p")
                    if not a:
                        continue
                    href = a.get("href", "")
                    if "linkedin.com/company" not in href:
                        continue
                    clean = _li_clean_url(href)
                    title = (a.get_text(strip=True)
                               .replace(" | LinkedIn", "")
                               .replace(" - LinkedIn", "")
                               .strip()) or _li_name_from_url(clean)
                    out.append({"linkedin_url": clean, "title": title,
                                "snippet": cap.get_text(strip=True) if cap else ""})
                    found_on_page += 1
                # If Bing returned no linkedin results on this page, stop paginating
                if found_on_page == 0:
                    break
                import time
                time.sleep(0.8)
            except Exception:
                break
        return out

    results: list = []
    if engine in ("duckduckgo", "all"):
        results = await asyncio.to_thread(_ddg_collect)
        # Second DDG pass with exact-match query to catch additional hits
        if len(results) < max_results:
            query = query_exact
            extra = await asyncio.to_thread(_ddg_collect)
            seen = {r["linkedin_url"] for r in results}
            results += [r for r in extra if r["linkedin_url"] not in seen]
            query = query_broad  # restore for Bing

    # Bing always runs — it paginates and is the most reliable source of volume
    bing = await asyncio.to_thread(_bing_collect)
    seen = {r["linkedin_url"] for r in results}
    results += [r for r in bing if r["linkedin_url"] not in seen]

    # Exact Bing pass if still short
    if len(results) < max_results:
        query = query_exact
        bing_exact = await asyncio.to_thread(_bing_collect)
        seen = {r["linkedin_url"] for r in results}
        results += [r for r in bing_exact if r["linkedin_url"] not in seen]

    seen_urls: set = set()
    deduped = []
    for r in results:
        if r["linkedin_url"] not in seen_urls:
            seen_urls.add(r["linkedin_url"])
            deduped.append(r)

    return deduped[:max_results]


# ── STEP 2 helper: find website when no Maps key ──────────────────────────────

async def _find_website_web(company_name: str, location: str, engine: str) -> dict:
    """Web-search a company to find its official website (no Maps API key path)."""
    query = f'"{company_name}" {location} official website'
    base: dict = {"website": "", "url": "", "address": "", "phone": "",
                  "lat": None, "lng": None, "rating": None}

    def _ddg_collect():
        try:
            from ddgs import DDGS
            for r in DDGS().text(query, max_results=5):
                href = r.get("href", "")
                if href.startswith("http") and not _is_noise(href):
                    return href
        except Exception:
            pass
        return ""

    def _bing_collect():
        try:
            resp = requests.get(
                "https://www.bing.com/search",
                params={"q": query, "count": 5},
                headers=HEADERS, timeout=50,
            )
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.select("li.b_algo h2 a"):
                href = a.get("href", "")
                if href.startswith("http") and not _is_noise(href):
                    return href
        except Exception:
            pass
        return ""

    website = ""
    if engine in ("duckduckgo", "all"):
        website = await asyncio.to_thread(_ddg_collect)
    if not website:
        website = await asyncio.to_thread(_bing_collect)
    if website:
        base["website"] = website
        base["url"] = website
    return base


# ── Job runner ────────────────────────────────────────────────────────────────

async def _run_prospect_job(job: ProspectJob):
    """
    GET THE BUSINESSES FROM LINKED IN +GOOGLE MAPS + SCRAPPING --> cross checking all of them.

    Pipeline:
      1. Search LinkedIn for companies matching niche + location.
      2. Cross-check each on Google Maps (Places API or web) for address/lat/lng/phone/website.
      3. Scrape each business website for full contact details (email, phone, address).
    """
    job.status = "running"
    cfg = job.config

    # ── STEP 1: LinkedIn ──────────────────────────────────────────────────────
    job.push({"type": "status",
              "msg": f'Step 1/3 — Searching LinkedIn for "{cfg.niche} {cfg.location}"…'})

    linkedin_hits = await _search_linkedin_companies(
        cfg.niche, cfg.location, cfg.engine, cfg.max_results
    )

    if not linkedin_hits:
        job.push({"type": "error",
                  "msg": "No LinkedIn results found. Try a different niche or location."})
        job.status = "error"
        job.done = True
        return

    job.push({"type": "status",
              "msg": f"Found {len(linkedin_hits)} on LinkedIn — Step 2/3: Google Maps…"})

    # ── STEP 2: Google Maps enrichment ────────────────────────────────────────
    candidates: list = []
    for li in linkedin_hits:
        base: dict = {
            "title":       li["title"],
            "linkedin_url": li["linkedin_url"],
            "snippet":     li["snippet"],
            "website": "", "url": "", "phone": "",
            "address": "", "lat": None, "lng": None, "rating": None,
        }

        if cfg.gmaps_key:
            try:
                places = await _search_google_places(
                    f"{li['title']} {cfg.location}", cfg.location, 2, cfg.gmaps_key
                )
                if places:
                    p = places[0]
                    base.update({
                        "website": p.get("website", ""),
                        "url":     p.get("url", p.get("website", "")),
                        "phone":   p.get("phone", ""),
                        "address": p.get("address", ""),
                        "lat":     p.get("lat"),
                        "lng":     p.get("lng"),
                        "rating":  p.get("rating"),
                    })
            except Exception:
                pass
        else:
            web = await _find_website_web(li["title"], cfg.location, cfg.engine)
            base.update(web)

        candidates.append(base)
        await asyncio.sleep(0.2)

    job.push({"type": "status",
              "msg": f"Maps done — Step 3/3: Scraping {len(candidates)} websites…"})

    # ── STEP 3: Website scraping ──────────────────────────────────────────────
    qualified = 0  # all results sourced from LinkedIn → all qualify

    for i, c in enumerate(candidates):
        job.push({
            "type": "progress",
            "current": i, "total": len(candidates),
            "msg": f"[{i+1}/{len(candidates)}] {c['title'][:60]}…",
        })

        website = c.get("website") or c.get("url") or ""
        phone   = c.get("phone") or ""
        email   = ""
        address = c.get("address") or ""

        if website and website.startswith("http") and not _is_noise(website):
            page = await asyncio.to_thread(_scrape_business_page, website)
            phone   = phone or page.get("phone") or ""
            email   = page.get("email") or ""
            address = address or page.get("address") or ""

        if cfg.find_email and not email:
            email = _extract_email(c.get("snippet", "")) or ""

        qualified += 1

        result = {
            "name":      c["title"],
            "domain":    _extract_domain(website) if website else "",
            "website":   website,
            "phone":     phone,
            "email":     email,
            "address":   address,
            "linkedin":  c["linkedin_url"],
            "lat":       c.get("lat"),
            "lng":       c.get("lng"),
            "rating":    c.get("rating"),
            "qualified": True,
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
