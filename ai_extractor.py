"""
ai_extractor.py — Phase 3 (LLM fallback)
When selector_inference can't find a field with enough confidence,
this sends a compressed HTML skeleton to a local LLM (ollama) to identify
the CSS selector.

Why a skeleton and not raw HTML?
  Raw HTML is 200k+ chars. The LLM doesn't need the content — it needs
  the structure. The skeleton keeps only tag names, class names, ids,
  semantic attributes (itemprop, data-*), and a 60-char text preview.
  This reduces the input to ~4-6k chars, which fits in any context window
  and costs nothing on a local model.

Plugging in your own model
  Set OLLAMA_MODEL env var to any model you have pulled:
    ollama pull llama3.2:3b
    OLLAMA_MODEL=llama3.2:3b python server.py

  To swap in a different backend (OpenAI-compatible API, your fine-tuned model, etc.):
  Replace _call_model() below — the rest of the file stays the same.
"""
import json
import os
import re
from typing import Optional
import requests



OLLAMA_URL   = os.getenv("OLLAMA_URL",   "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")


# ── HTML skeleton builder ─────────────────────────────────────────────────────

def build_skeleton(html: str, max_chars: int = 6000) -> str:
    """
    Strip all text content. Keep only:
      - tag name
      - class, id, itemprop attributes
      - data-* attributes that look semantic (price, sku, ref, stock, name)
      - first 60 chars of text content as a preview

    The skeleton shows structure without noise so the LLM can identify
    which element holds each product field.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "meta", "link", "noscript", "svg"]):
        tag.decompose()

    lines: list[str] = []
    for node in soup.find_all(True):
        # Only include nodes that contain text directly (leaf-ish)
        if len(node.find_all()) > 2:
            continue

        attrs: dict = {}
        classes = node.get("class", [])
        if classes:
            attrs["class"] = " ".join(classes[:3])
        if node.get("id"):
            attrs["id"] = node["id"]
        if node.get("itemprop"):
            attrs["itemprop"] = node["itemprop"]
        for attr, val in node.attrs.items():
            if attr.startswith("data-") and any(
                k in attr for k in ("price", "sku", "ref", "stock", "rating", "name", "id")
            ):
                attrs[attr] = val if isinstance(val, str) else ""

        preview = node.get_text(" ", strip=True)[:60]
        attr_str = " ".join(f'{k}="{v}"' for k, v in attrs.items())
        lines.append(f"<{node.name} {attr_str}>{preview}</{node.name}>")

    return "\n".join(lines)[:max_chars]


# ── Prompt builder ────────────────────────────────────────────────────────────

def build_prompt(skeleton: str, missing_fields: list[str]) -> str:
    fields_str = ", ".join(missing_fields)
    return (
        f"You are an HTML analysis assistant. "
        f"Below is the skeleton of a product page — only tags, classes, and short text previews.\n"
        f"Find the best CSS selector for each of these product fields: {fields_str}.\n\n"
        f"HTML skeleton:\n{skeleton}\n\n"
        f"Return ONLY a JSON object. No explanation. No markdown. Example:\n"
        f'{{"price": "span.price-amount", "sku": "[itemprop=\\"sku\\"]", "name": "h1.product-title"}}\n'
        f"Set a field to null if you cannot find it.\n"
        f"Only include fields from this list: {fields_str}"
    )


# ── Model call (swap this function for any other backend) ─────────────────────

def _call_model(prompt: str) -> Optional[str]:
    """
    Call the local ollama instance.
    Returns the raw response string, or None on failure.

    To use a different model backend, replace this function.
    The rest of the module doesn't care how the response is generated —
    it just needs a string back that (hopefully) contains a JSON object.
    """
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=45,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")
    except Exception:
        return None


# ── Response parser ───────────────────────────────────────────────────────────

def _parse_response(text: str) -> dict:
    """
    Extract a JSON object from the LLM's response.
    LLMs often add extra text or markdown around the JSON — this handles that.
    """
    if not text:
        return {}
    # Find the first {...} block in the response
    m = re.search(r'\{[^{}]+\}', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except Exception:
            pass
    return {}


# ── Public API ────────────────────────────────────────────────────────────────

def extract_selectors_with_llm(html: str, missing_fields: list[str]) -> dict:
    """
    Build skeleton → prompt → call model → parse JSON → return {field: selector}.

    Returns {} if the model is unavailable or returns nothing useful.
    Fields with null value from the model are excluded from the result.
    """
    if not missing_fields:
        return {}

    skeleton = build_skeleton(html)
    prompt   = build_prompt(skeleton, missing_fields)
    raw      = _call_model(prompt)
    parsed   = _parse_response(raw or "")

    # Keep only non-null string values for requested fields
    return {
        field: sel
        for field, sel in parsed.items()
        if field in missing_fields and sel and isinstance(sel, str)
    }
