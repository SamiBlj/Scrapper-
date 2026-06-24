"""
Subprocess runner: reads config JSON, runs Scrapy, prints JSONL to stdout.
"""
import sys
import json
import os

# Windows: Scrapy needs SelectorEventLoop, not the default ProactorEventLoop
if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_dir)


# Must be at module level so Scrapy can find it via "__main__.JsonlPipeline"
class JsonlPipeline:
    def process_item(self, item, spider):
        print(json.dumps(dict(item)), flush=True)
        return item


def main():
    if len(sys.argv) < 3:
        print(json.dumps({"_type": "log", "_status": "err", "_msg": "Missing args"}), flush=True)
        sys.exit(1)

    config_file = sys.argv[1]
    output_file = sys.argv[2]

    with open(config_file, encoding="utf-8") as f:
        config = json.load(f)

    from scrapy.crawler import CrawlerProcess
    from scrapy.settings import Settings
    from product_spider import ProductSpider

    settings = Settings()
    settings.set("BOT_NAME", "product_spider")
    settings.set("ROBOTSTXT_OBEY", False)
    settings.set("CONCURRENT_REQUESTS", config.get("concurrent", 10))
    settings.set("CONCURRENT_REQUESTS_PER_DOMAIN", 2)
    settings.set("DOWNLOAD_DELAY", 0.5)
    settings.set("RANDOMIZE_DOWNLOAD_DELAY", True)
    settings.set("DOWNLOAD_TIMEOUT", 20)       # ← drop any site that hangs >20s
    settings.set("RETRY_TIMES", 1)
    settings.set("RETRY_HTTP_CODES", [500, 502, 503, 504, 429])
    settings.set("LOG_LEVEL", "ERROR")
    settings.set("TELNETCONSOLE_ENABLED", False)
    settings.set("REQUEST_FINGERPRINTER_IMPLEMENTATION", "2.7")
    settings.set("FEED_EXPORT_ENCODING", "utf-8")
    settings.set("DEFAULT_REQUEST_HEADERS", {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    settings.set("ITEM_PIPELINES", {"__main__.JsonlPipeline": 100})

    process = CrawlerProcess(settings)
    process.crawl(
        ProductSpider,
        start_urls=config["start_urls"],
        buy_signals=config.get("buy_signals"),
        skip_domains=config.get("skip_domains"),
        extract_fields=config.get("extract_fields"),
        verbose=config.get("verbose", False),
    )
    process.start()


if __name__ == "__main__":
    main()
