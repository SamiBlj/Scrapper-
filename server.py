import asyncio
import json
import os
import sys
import uuid
from typing import Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Add project to path
sys.path.insert(0, os.path.dirname(__file__))

app = FastAPI(title="Product Spider API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job store
jobs: dict = {}


class SpiderConfig(BaseModel):
    query: str
    pages: int = 3
    threads: int = 10
    buy_signals: list[str] = ["add to cart", "buy now", "in stock", "checkout"]
    skip_domains: list[str] = ["wikipedia", "reddit", "youtube"]
    extract_fields: list[str] = ["price", "rating", "reviews", "brand", "availability"]
    serpapi_key: Optional[str] = None
    search_engine: str = "duckduckgo"  # "google" | "duckduckgo" | "bing"


class Job:
    def __init__(self, job_id: str, config: SpiderConfig):
        self.job_id = job_id
        self.config = config
        self.status = "pending"  # pending | running | done | error
        self.events: list[dict] = []
        self.results: list[dict] = []
        self.metrics = {"scanned": 0, "found": 0, "skipped": 0}
        self.start_time = datetime.now()
        self.queue: asyncio.Queue = asyncio.Queue()
        self.done = False

    def push(self, event: dict):
        self.events.append(event)
        self.queue.put_nowait(event)

    def elapsed(self):
        return round((datetime.now() - self.start_time).total_seconds(), 1)


async def _search_google(config: SpiderConfig) -> list[str]:
    """Google via SerpAPI (API key required)."""
    if not config.serpapi_key:
        raise RuntimeError(
            "Google search requires a SerpAPI key. Add one in the sidebar, "
            "or switch to DuckDuckGo / Bing."
        )
    try:
        from serpapi import GoogleSearch
    except ImportError:
        raise RuntimeError("google-search-results is not installed. Run: pip install google-search-results")
    urls = []
    for page in range(config.pages):
        params = {
            "q": config.query,
            "api_key": config.serpapi_key,
            "start": page * 10,
            "num": 10,
        }
        search = GoogleSearch(params)
        results = search.get_dict()
        for r in results.get("organic_results", []):
            url = r.get("link")
            if url and not any(s in url for s in config.skip_domains):
                urls.append(url)
    return urls


async def _search_duckduckgo(config: SpiderConfig) -> list[str]:
    """DuckDuckGo via ddgs library (no API key needed)."""
    try:
        from ddgs import DDGS
    except ImportError:
        raise RuntimeError("ddgs is not installed. Run: pip install ddgs")
    max_results = config.pages * 10
    results = await asyncio.to_thread(
        lambda: list(DDGS().text(config.query, max_results=max_results))
    )
    urls = []
    for r in results:
        url = r.get("href", "")
        if url and not any(s in url for s in config.skip_domains):
            urls.append(url)
    return list(dict.fromkeys(urls))


async def _search_bing(config: SpiderConfig) -> list[str]:
    """Bing via HTML scraping (no API key needed)."""
    import requests
    from bs4 import BeautifulSoup
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }
    urls = []
    for page in range(config.pages):
        try:
            resp = await asyncio.to_thread(
                lambda p=page: requests.get(
                    "https://www.bing.com/search",
                    params={"q": config.query, "first": p * 10 + 1, "count": 10},
                    headers=headers,
                    timeout=15,
                )
            )
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.select("li.b_algo h2 a, li.b_algo .b_title a"):
                url = a.get("href", "")
                if url.startswith("http") and not any(s in url for s in config.skip_domains):
                    urls.append(url)
            await asyncio.sleep(1)
        except Exception:
            continue
    return list(dict.fromkeys(urls))


async def fetch_urls(config: SpiderConfig) -> list[str]:
    """Dispatch to the selected search engine."""
    engine = (config.search_engine or "duckduckgo").lower()
    if engine == "google":
        return await _search_google(config)
    elif engine == "bing":
        return await _search_bing(config)
    else:
        return await _search_duckduckgo(config)


async def run_spider_job(job: Job):
    """Run the full spider pipeline for a job."""
    job.status = "running"

    try:
        # Step 1: fetch URLs
        engine_label = (job.config.search_engine or "duckduckgo").capitalize()
        job.push({"type": "status", "msg": f'Searching {engine_label} for "{job.config.query}"…'})
        urls = await fetch_urls(job.config)

        if not urls:
            job.push({"type": "error", "msg": "No URLs found. Try a different query, or add a SerpAPI key for more reliable results."})
            job.status = "error"
            job.done = True
            return

        job.push({"type": "status", "msg": f"Found {len(urls)} candidate URLs. Starting crawl…"})

        # Step 2: run Scrapy in a subprocess to avoid event loop conflicts
        import tempfile

        config_payload = {
            "start_urls": urls,
            "buy_signals": job.config.buy_signals,
            "skip_domains": job.config.skip_domains,
            "extract_fields": job.config.extract_fields,
            "concurrent": job.config.threads,
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_payload, f)
            config_file = f.name

        output_file = config_file.replace(".json", "_out.jsonl")
        runner_script = os.path.join(os.path.dirname(__file__), "run_scrapy.py")

        proc = await asyncio.create_subprocess_exec(
            sys.executable, runner_script, config_file, output_file,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=os.path.dirname(__file__),
        )

        # Stream stdout lines as events
        async for line in proc.stdout:
            line = line.decode().strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue

            if item.get("_type") == "log":
                status = item.get("_status", "ok")
                job.metrics["scanned"] += 1
                if status == "ok":
                    job.metrics["found"] += 1
                else:
                    job.metrics["skipped"] += 1
                job.push({
                    "type": "log",
                    "status": status,
                    "msg": item.get("_msg", ""),
                    "url": item.get("_url", ""),
                    "metrics": dict(job.metrics),
                    "elapsed": job.elapsed(),
                })
            elif item.get("_type") == "product":
                clean = {k: v for k, v in item.items() if not k.startswith("_")}
                job.results.append(clean)
                job.push({"type": "product", "data": clean})

        await proc.wait()
        os.unlink(config_file)
        try:
            os.unlink(output_file)
        except Exception:
            pass

        job.status = "done"
        job.push({
            "type": "done",
            "metrics": dict(job.metrics),
            "elapsed": job.elapsed(),
            "results": job.results,
        })

    except Exception as e:
        job.status = "error"
        job.push({"type": "error", "msg": str(e)})
    finally:
        job.done = True


@app.post("/api/jobs")
async def create_job(config: SpiderConfig):
    job_id = str(uuid.uuid4())
    job = Job(job_id, config)
    jobs[job_id] = job
    asyncio.create_task(run_spider_job(job))
    return {"job_id": job_id}


@app.get("/api/jobs/{job_id}/stream")
async def stream_job(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]

    async def event_generator():
        sent = 0
        while True:
            while sent < len(job.events):
                event = job.events[sent]
                yield f"data: {json.dumps(event)}\n\n"
                sent += 1
            if job.done:
                break
            await asyncio.sleep(0.1)
        yield "data: {\"type\": \"stream_end\"}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/api/jobs/{job_id}/results")
async def get_results(job_id: str, fmt: str = "json"):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]

    if fmt == "csv":
        import csv, io
        output = io.StringIO()
        if not job.results:
            return StreamingResponse(io.BytesIO(b"No results"), media_type="text/csv")
        fields = list(job.results[0].keys())
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(job.results)
        content = output.getvalue().encode()
        return StreamingResponse(
            io.BytesIO(content),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=products.csv"},
        )

    return {"results": job.results, "metrics": job.metrics, "status": job.status}


@app.get("/api/jobs/{job_id}/status")
async def get_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    job = jobs[job_id]
    return {
        "status": job.status,
        "metrics": job.metrics,
        "elapsed": job.elapsed(),
        "result_count": len(job.results),
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
async def root():
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(html_path, encoding="utf-8") as f:
        return f.read()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)
