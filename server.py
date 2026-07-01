import asyncio
import json
import os
import queue as _queue
import subprocess
import sys
import threading
import uuid
from typing import Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse
from pydantic import BaseModel

sys.path.insert(0, os.path.dirname(__file__))

app = FastAPI(title="Product Spider API")

# Mount scraper router
from scraper import router as scraper_router
app.include_router(scraper_router)

# Mount prospects router
from prospects import router as prospects_router
app.include_router(prospects_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

jobs: dict = {}


class SpiderConfig(BaseModel):
    query: str
    max_results: int = 30          # total URLs to gather (no upper limit)
    threads: int = 10
    buy_signals: list[str] = ["add to cart", "buy now", "in stock", "checkout"]
    skip_domains: list[str] = ["wikipedia", "reddit", "youtube"]
    extract_fields: list[str] = ["price", "rating", "reviews", "brand", "availability"]
    serpapi_key: Optional[str] = None
    search_engine: str = "bing"   # "google" | "duckduckgo" | "bing" | "all"
    verbose: bool = False


class Job:
    def __init__(self, job_id: str, config: SpiderConfig):
        self.job_id = job_id
        self.config = config
        self.status = "pending"
        self.events: list[dict] = []
        self.results: list[dict] = []
        self.metrics = {"scanned": 0, "found": 0, "skipped": 0}
        self.start_time = datetime.now()
        self.done = False

    def push(self, event: dict):
        self.events.append(event)

    def elapsed(self):
        return round((datetime.now() - self.start_time).total_seconds(), 1)


# ── Search backends ──────────────────────────────────────────────────────────

async def _search_google(query: str, max_results: int, serpapi_key: str,
                         skip_domains: list[str]) -> list[str]:
    if not serpapi_key:
        raise RuntimeError(
            "Google requires a SerpAPI key — add it in the sidebar, or switch engine."
        )
    try:
        from serpapi import GoogleSearch
    except ImportError:
        raise RuntimeError("google-search-results not installed: pip install google-search-results")

    urls = []
    pages = (max_results + 9) // 10
    for page in range(pages):
        params = {
            "q": query,
            "api_key": serpapi_key,
            "start": page * 10,
            "num": 10,
        }
        data = await asyncio.to_thread(lambda p=params: GoogleSearch(p).get_dict())
        for r in data.get("organic_results", []):
            url = r.get("link", "")
            if url and not any(s in url for s in skip_domains):
                urls.append(url)
        if len(urls) >= max_results:
            break
    return list(dict.fromkeys(urls))[:max_results]


async def _search_duckduckgo(query: str, max_results: int,
                              skip_domains: list[str]) -> list[str]:
    from ddgs import DDGS

    def _collect() -> list[str]:
        # Iterate one-by-one so a mid-stream separator error keeps partial results
        urls: list[str] = []
        try:
            for r in DDGS().text(query, max_results=6):
                url = r.get("href", "")
                if url and url.startswith("http") and not any(s in url for s in skip_domains):
                    urls.append(url)
        except Exception:
            pass
        return list(dict.fromkeys(urls))

    urls = await asyncio.to_thread(_collect)
    if urls:
        return urls[:max_results]
    # Fallback to Bing if DDG returned nothing
    return await _search_bing(query, max_results, skip_domains)


async def _search_bing(query: str, max_results: int,
                        skip_domains: list[str]) -> list[str]:
    import requests
    from bs4 import BeautifulSoup

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }
    urls = []
    pages = (max_results + 9) // 10
    for page in range(pages):
        try:
            resp = await asyncio.to_thread(
                lambda p=page: requests.get(
                    "https://www.bing.com/search",
                    params={"q": query, "first": p * 10 + 1, "count": 10},
                    headers=headers,
                    timeout=15,
                )
            )
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.select("li.b_algo h2 a"):
                url = a.get("href", "")
                if url.startswith("http") and not any(s in url for s in skip_domains):
                    urls.append(url)
            await asyncio.sleep(1)
        except Exception:
            continue
        if len(urls) >= max_results:
            break
    return list(dict.fromkeys(urls))[:max_results]


async def fetch_urls(config: SpiderConfig, job: Job) -> list[str]:
    engine = (config.search_engine or "duckduckgo").lower()

    if engine == "all":
        # Run all three concurrently, deduplicate
        tasks = [
            _search_duckduckgo(config.query, config.max_results, config.skip_domains),
            _search_bing(config.query, config.max_results, config.skip_domains),
        ]
        if config.serpapi_key:
            tasks.append(
                _search_google(config.query, config.max_results,
                               config.serpapi_key, config.skip_domains)
            )
        else:
            job.push({"type": "status",
                      "msg": "All-engines mode: skipping Google (no SerpAPI key)"})

        results = await asyncio.gather(*tasks, return_exceptions=True)
        urls = []
        labels = ["DuckDuckGo", "Bing", "Google"] if config.serpapi_key else ["DuckDuckGo", "Bing"]
        for label, res in zip(labels, results):
            if isinstance(res, Exception):
                job.push({"type": "status", "msg": f"{label} error: {res}"})
            else:
                job.push({"type": "status", "msg": f"{label}: {len(res)} URLs"})
                urls.extend(res)
        return list(dict.fromkeys(urls))[:config.max_results]

    elif engine == "google":
        job.push({"type": "status", "msg":"Initializing Google Search"})
        return await _search_google(config.query, config.max_results,
                                    config.serpapi_key, config.skip_domains)
    elif engine == "bing":
        job.push({"type": "status", "msg":"Initializing Bing Search"})

        return await _search_bing(config.query, config.max_results, config.skip_domains)
    else:
        job.push({"type":"status", "msg":"Initializing DuckDuckGo Search"})
        return await _search_duckduckgo(config.query, config.max_results, config.skip_domains)


# ── Spider job ───────────────────────────────────────────────────────────────

async def run_spider_job(job: Job):
    job.status = "running"
    try:
        engine_label = job.config.search_engine.capitalize()
        job.push({"type": "status",
                  "msg": f'Searching {engine_label} for "{job.config.query}"…'})

        urls = await fetch_urls(job.config, job)

        if not urls:
            job.push({"type": "error",
                      "msg": "No URLs found. Try a different query or engine."})
            job.status = "error"
            job.done = True
            return

        job.push({"type": "status",
                  "msg": f"Found {len(urls)} URLs — starting crawl…"})

        import tempfile
        config_payload = {
            "start_urls": urls,
            "buy_signals": job.config.buy_signals,
            "skip_domains": job.config.skip_domains,
            "extract_fields": job.config.extract_fields,
            "concurrent": job.config.threads,
            "verbose": job.config.verbose,
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_payload, f)
            config_file = f.name

        output_file = config_file.replace(".json", "_out.jsonl")
        runner_script = os.path.join(os.path.dirname(__file__), "run_scrapy.py")

        # Use subprocess.Popen in a thread — avoids asyncio event loop
        # subprocess limitations on Windows (SelectorEventLoop raises NotImplementedError).
        line_q: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        done_event = asyncio.Event()

        def _run_proc():
            proc = subprocess.Popen(
                [sys.executable, runner_script, config_file, output_file],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=os.path.dirname(__file__),
                encoding="utf-8",
                errors="replace",
            )

            def _drain_stderr():
                for raw in proc.stderr:
                    line_q.put(("err", raw.rstrip()))

            threading.Thread(target=_drain_stderr, daemon=True).start()

            for raw in proc.stdout:
                line_q.put(("out", raw.rstrip()))

            proc.wait()
            line_q.put(("done", None))
            loop.call_soon_threadsafe(done_event.set)

        threading.Thread(target=_run_proc, daemon=True).start()

        while not done_event.is_set() or not line_q.empty():
            try:
                kind, data = line_q.get_nowait()
            except _queue.Empty:
                await asyncio.sleep(0.05)
                continue

            if kind == "done":
                break

            if kind == "err":
                if data and ("ERROR" in data or "CRITICAL" in data or job.config.verbose):
                    job.push({"type": "log", "status": "verbose",
                              "msg": f"[scrapy] {data}", "url": "",
                              "metrics": dict(job.metrics),
                              "elapsed": job.elapsed()})
                continue

            # kind == "out"
            line = data.strip()
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

        await done_event.wait()
        try:
            os.unlink(config_file)
        except Exception:
            pass
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
        msg = str(e) or f"{type(e).__name__} (no details — check server console)"
        job.push({"type": "error", "msg": msg})
    finally:
        job.done = True


# ── Routes ───────────────────────────────────────────────────────────────────

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
                yield f"data: {json.dumps(job.events[sent])}\n\n"
                sent += 1
            if job.done:
                break
            await asyncio.sleep(0.1)
        yield 'data: {"type": "stream_end"}\n\n'

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                 "Connection": "keep-alive"},
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
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
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

#user_input = input('reset the server by pressing "r" ')

if __name__ == "__main__" : #or user_input == 'r'
    print("_"*20)
    print("initialising Server, YIPPEEEEE")
    print("_"*20)
    import uvicorn
    print("INITIALIZING UVICORN ON LOCALHOST PORT 8000 WITH RELOAD")
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)