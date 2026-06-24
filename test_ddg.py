"""Run this to debug: python test_ddg.py"""
try:
    from duckduckgo_search import DDGS
    print("duckduckgo-search is installed OK")
except ImportError:
    print("ERROR: duckduckgo-search is NOT installed. Run: pip install duckduckgo-search")
    exit(1)

print("Searching for 'buy wireless headphones'...")
try:
    with DDGS() as ddgs:
        results = list(ddgs.text("buy wireless headphones", max_results=10))
    print(f"Got {len(results)} results")
    for r in results[:5]:
        print(" -", r.get("href"))
except Exception as e:
    print(f"ERROR: {e}")
