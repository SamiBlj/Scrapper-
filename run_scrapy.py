"""
Subprocess runner: reads config JSON, runs Scrapy, prints JSONL items to stdout.
Called by server.py as a subprocess to avoid event loop conflicts.
"""
import sys
import json
import os

sys.path.insert(0, os.path.dirname(__file__))

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def main():
    if len(sys.argv) < 3:
        print(json.dumps({"_type": "log", "_status": "err", "_msg": "Missing args"}))
        sys.exit(1)

    config_file = sys.argv[1]
    output_file = sys.argv[2]

    with open(config_file) as f:
        config = json.load(f)

    os.environ["SCRAPY_SETTINGS_MODULE"] = "settings"

    settings = get_project_settings()
    settings.set("CONCURRENT_REQUESTS", config.get("concurrent", 10))
    settings.set("LOG_LEVEL", "ERROR")
    settings.set("FEEDS", {})  # disable default feed

    collected = []

    class JsonlPipeline:
        def process_item(self, item, spider):
            data = dict(item)
            print(json.dumps(data), flush=True)
            collected.append(data)
            return item

    settings.set("ITEM_PIPELINES", {"__main__.JsonlPipeline": 100})

    process = CrawlerProcess(settings)
    process.crawl(
        "products",
        start_urls=config["start_urls"],
        buy_signals=config.get("buy_signals"),
        skip_domains=config.get("skip_domains"),
        extract_fields=config.get("extract_fields"),
    )
    process.start()


if __name__ == "__main__":
    main()
