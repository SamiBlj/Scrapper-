import scrapy
import re
import html


class ProductSpider(scrapy.Spider):
    name = "products"

    def __init__(self, start_urls=None, buy_signals=None, skip_domains=None,
                 extract_fields=None, job_id=None, verbose=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = start_urls or []
        self.verbose = str(verbose).lower() in ("true", "1", "yes")
        self.buy_signals = buy_signals or [
            "add to cart", "buy now", "in stock", "checkout",
            "add to bag", "purchase", "order now", "free shipping",
            "add to basket", "buy", "shop now", "price", "$", "€", "£",
            "quantity", "qty", "select size", "select color", "ships",
        ]
        self.skip_domains = skip_domains or [
            "wikipedia", "reddit", "youtube", "forum", "quora"
        ]
        self.extract_fields = extract_fields or [
            "price", "rating", "reviews", "brand", "availability"
        ]
        self.job_id = job_id

    def parse(self, response):
        page_text = response.text.lower()


        # Check skip domains
        domain = response.url.split("/")[2] if "//" in response.url else response.url
        if any(skip in domain for skip in self.skip_domains):
            yield {
                "_type": "log",
                "_status": "skip",
                "_msg": f"[{response.status}] {domain} →  Skipped (domain filter)",
                "_url": response.url,
            }
            return

        # Count buy signals
        signals_found = [s for s in self.buy_signals if s in page_text]

        if len(signals_found) < 1:
            if self.verbose:
                yield {
                    "_type": "log",
                    "_status": "skip",
                    "_msg": f"[{response.status}] {domain} → There were No buy signals found",
                    "_url": response.url,
                }
            return

        yield {
            "_type": "log",
            "_status": "ok",
            "_msg": f"[{response.status}] {domain} → ✅ Product page — signals: {', '.join(signals_found[:5])}",
            "_url": response.url,
        }

        item = {
            "_type": "product",
            "source_url": response.url,
            "site": domain,
            "name": self._extract_name(response),
        }

        number_contexts = self._extract_number_contexts(response)
        if number_contexts:
            item["number_contexts"] = number_contexts
            item["html_data"] = self._build_html_data(number_contexts)

        if "price" in self.extract_fields:
            item["price"] = self._extract_price(response)
        if "rating" in self.extract_fields:
            item["rating"] = self._extract_rating(response)
        if "reviews" in self.extract_fields:
            item["reviews"] = self._extract_reviews(response)
        if "brand" in self.extract_fields:
            item["brand"] = self._extract_brand(response)
        if "availability" in self.extract_fields:
            item["availability"] = self._extract_availability(response)
        if "description" in self.extract_fields:
            item["description"] = self._extract_description(response)

        yield item

    def _extract_name(self, response):
        for sel in [
            "h1::text",
            '[class*="product-title"]::text',
            '[class*="product_title"]::text',
            '[class*="productName"]::text',
            '[itemprop="name"]::text',
        ]:
            val = response.css(sel).get()
            if val and val.strip():
                return val.strip()
        return response.xpath("//h1/text()").get("N/A").strip()

    def _extract_price(self, response):
        # Schema.org
        val = response.css('[itemprop="price"]::attr(content)').get()
        if val:
            return val.strip()
        # Common class names
        for sel in ['[class*="price"]::text', '[class*="Price"]::text', '[id*="price"]::text']:
            val = response.css(sel).get()
            if val and re.search(r'[\d]', val):
                return val.strip()
        # Regex fallback
        matches = re.findall(r'[\$\€\£\¥]\s?\d+[\.,]\d{2}', response.text)
        return matches[0] if matches else "N/A"

    def _extract_rating(self, response):
        for sel in [
            '[itemprop="ratingValue"]::attr(content)',
            '[class*="rating"]::attr(content)',
            '[class*="stars"]::attr(aria-label)',
            '[class*="rating"]::text',
        ]:
            val = response.css(sel).get()
            if val and val.strip():
                return val.strip()
        return "N/A"

    def _extract_reviews(self, response):
        for sel in [
            '[itemprop="reviewCount"]::text',
            '[class*="review-count"]::text',
            '[class*="reviewCount"]::text',
            '[class*="ratings-count"]::text',
        ]:
            val = response.css(sel).get()
            if val and val.strip():
                return val.strip()
        return "N/A"

    def _extract_brand(self, response):
        for sel in [
            '[itemprop="brand"] [itemprop="name"]::attr(content)',
            '[itemprop="brand"]::text',
            '[class*="brand"]::text',
        ]:
            val = response.css(sel).get()
            if val and val.strip():
                return val.strip()
        return "N/A"


    def _extract_availability(self, response):
        val = response.css('[itemprop="availability"]::attr(content)').get()
        if val:
            return "In stock" if "InStock" in val else "Out of stock"
        for sel in ['[class*="availability"]::text', '[class*="stock"]::text']:
            val = response.css(sel).get()
            if val and val.strip():
                return val.strip()
        return "N/A"


    def _extract_description(self, response):
        for sel in [
            '[itemprop="description"]::text',
            '[class*="product-description"]::text',
            '[class*="productDescription"]::text',
        ]:
            val = response.css(sel).get()
            if val and val.strip():
                return val.strip()[:300]
        return "N/A"


    def _extract_number_contexts(self, response, radius=60):
        html_text = response.text
        contexts = []
        for match in re.finditer(r'[\d][\d\.,]*', html_text):
            start = max(0, match.start() - radius)
            end = min(len(html_text), match.end() + radius)
            raw_context = html_text[start:end].replace("\n", " ")
            text_context = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", raw_context)).strip()
            contexts.append({
                "number": match.group(),
                "html_context": raw_context.strip(),
                "text_context": text_context,
            })
        return contexts


    def _build_html_data(self, contexts):
        rows = []
        for ctx in contexts:
            escaped_html = html.escape(ctx["html_context"])
            rows.append(
                '<div class="number-context">'
                f'<span class="number">{ctx["number"]}</span>'
                f'<span class="context">{escaped_html}</span>'
                '</div>'
            )
        return '<section>' + ''.join(rows) + '</section>'
