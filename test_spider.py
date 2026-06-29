"""Run: python test_spider.py"""
import json, subprocess, sys, tempfile, os

config_payload = {
    "start_urls": [
        "https://www.newegg.com/p/pl?d=wireless+headphones",
        "https://www.bhphotovideo.com/c/search?Ntt=wireless+headphones",
        "https://www.adorama.com/l/?searchinfo=wireless+headphones",
    ],
    "buy_signals": ["add to cart", "buy now", "price", "$", "buy", "in stock"],
    "skip_domains": ["wikipedia", "reddit"],
    "extract_fields": ["price", "rating", "brand"],
    "concurrent": 3,
    "verbose": True,
}

with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
    json.dump(config_payload, f)
    config_file = f.name
output_file = config_file.replace(".json", "_out.jsonl")

print("Running Scrapy subprocess (timeout: 60s)...")
result = subprocess.run(
    [sys.executable, "run_scrapy.py", config_file, output_file],
    capture_output=True, text=True, timeout=60,
    cwd=os.path.dirname(os.path.abspath(__file__))
)
print("=== STDOUT ===")
print(result.stdout[:3000] or "(empty)")
print("=== STDERR (last 2000 chars) ===")
print(result.stderr[-2000:] or "(empty)")
print("=== Return code:", result.returncode)