BOT_NAME = "product_spider"
SPIDER_MODULES = ["product_spider"]
NEWSPIDER_MODULE = "product_spider"

ROBOTSTXT_OBEY = True
DOWNLOAD_DELAY = 1.5
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 2

RETRY_TIMES = 2
RETRY_HTTP_CODES = [500, 502, 503, 504, 429]

DEFAULT_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

LOG_LEVEL = "ERROR"
TELNETCONSOLE_ENABLED = False
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
