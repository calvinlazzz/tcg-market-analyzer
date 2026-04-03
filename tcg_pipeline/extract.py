"""
TCG Market Analyzer — Extract layer.

Two data sources:
    1.  PokémonTCG API  →  card metadata + TCGplayer market prices
    2.  eBay "Sold" listings  →  real-world transaction prices (scraped)
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from tcg_pipeline.config import (
    EBAY_DEFAULT_SEARCH_TERM,
    EBAY_REQUEST_DELAY,
    EBAY_SEARCH_URL,
    HTTP_USER_AGENT,
    POKEMONTCG_API_BASE,
    POKEMONTCG_API_KEY,
    POKEMONTCG_DEFAULT_QUERY,
    POKEMONTCG_PAGE_SIZE,
    PROJECT_ROOT,
)

logger = logging.getLogger(__name__)

# ── Shared HTTP session ─────────────────────────────────────────────────────

_session = requests.Session()
_session.headers.update({"User-Agent": HTTP_USER_AGENT})


# ═══════════════════════════════════════════════════════════════════════════
# 1.  PokémonTCG API
# ═══════════════════════════════════════════════════════════════════════════


def fetch_pokemontcg_cards(
    query: str | None = None,
    page_size: int | None = None,
) -> list[dict[str, Any]]:
    """Return a list of flat dicts with card info + TCGplayer market prices.

    Each dict has the keys:
        card_id, card_name, set_name, condition, price_usd,
        date_recorded, data_source
    """
    query = query or POKEMONTCG_DEFAULT_QUERY
    page_size = page_size or POKEMONTCG_PAGE_SIZE

    headers: dict[str, str] = {}
    if POKEMONTCG_API_KEY:
        headers["X-Api-Key"] = POKEMONTCG_API_KEY

    params: dict[str, Any] = {"q": query, "pageSize": page_size, "page": 1}
    all_rows: list[dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    while True:
        logger.info(
            "PokémonTCG API — fetching page %d  (query=%r)", params["page"], query
        )
        resp = _session.get(
            f"{POKEMONTCG_API_BASE}/cards",
            params=params,
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()

        cards: list[dict] = payload.get("data", [])
        if not cards:
            break

        for card in cards:
            prices = (
                card.get("tcgplayer", {}).get("prices", {})
            )
            # TCGplayer nests prices under sub-types like "holofoil", "normal", etc.
            if not prices:
                # No pricing data — still record the card with NULL price.
                all_rows.append(
                    {
                        "card_id": card.get("id", ""),
                        "card_name": card.get("name", ""),
                        "set_name": card.get("set", {}).get("name", ""),
                        "condition": None,
                        "price_usd": None,
                        "date_recorded": now_utc,
                        "data_source": "tcgplayer",
                    }
                )
                continue

            for sub_type, price_obj in prices.items():
                market_price = price_obj.get("market") or price_obj.get("mid")
                all_rows.append(
                    {
                        "card_id": card.get("id", ""),
                        "card_name": card.get("name", ""),
                        "set_name": card.get("set", {}).get("name", ""),
                        "condition": sub_type,  # e.g. "holofoil", "reverseHolofoil"
                        "price_usd": market_price,
                        "date_recorded": now_utc,
                        "data_source": "tcgplayer",
                    }
                )

        # Pagination — stop when we have all pages.
        total_count = payload.get("totalCount", 0)
        fetched_so_far = params["page"] * page_size
        if fetched_so_far >= total_count:
            break
        params["page"] += 1
        time.sleep(0.5)  # courtesy delay

    logger.info("PokémonTCG API — extracted %d price rows", len(all_rows))
    return all_rows


# ═══════════════════════════════════════════════════════════════════════════
# 2.  eBay Sold Listings Scraper
# ═══════════════════════════════════════════════════════════════════════════

# Regex to pull a dollar amount out of an eBay price string.
_PRICE_RE = re.compile(r"\$[\d,]+\.?\d*")


def _parse_price(text: str) -> float | None:
    """Extract the first dollar amount from *text* and return as a float."""
    match = _PRICE_RE.search(text)
    if not match:
        return None
    try:
        return float(match.group().replace("$", "").replace(",", ""))
    except ValueError:
        return None


def scrape_ebay_sold(
    search_term: str | None = None,
    max_pages: int = 2,
) -> list[dict[str, Any]]:
    """Scrape eBay "Sold" listings for *search_term* and return flat dicts.

    Returns dicts with the same schema as the TCG API extractor so they can
    be merged seamlessly in the Transform step.

    Parameters
    ----------
    search_term:
        Free-text eBay search query.  Defaults to ``config.EBAY_DEFAULT_SEARCH_TERM``.
    max_pages:
        Number of eBay result pages to scrape (each ≈ 60 results).
    """
    search_term = search_term or EBAY_DEFAULT_SEARCH_TERM
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    all_rows: list[dict[str, Any]] = []

    for page_num in range(1, max_pages + 1):
        params = {
            "_nkw": search_term,
            "LH_Complete": "1",   # completed listings
            "LH_Sold": "1",      # sold only
            "_pgn": page_num,
        }

        logger.info(
            "eBay scraper — fetching page %d for %r", page_num, search_term
        )

        try:
            resp = _session.get(
                EBAY_SEARCH_URL, params=params, timeout=30
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("eBay request failed (page %d): %s", page_num, exc)
            break

        soup = BeautifulSoup(resp.text, "lxml")

        # eBay wraps each result item in <li> tags with class "s-item".
        items = soup.select("li.s-item")
        if not items:
            logger.info("eBay scraper — no items found on page %d, stopping.", page_num)
            break

        for item in items:
            title_tag = item.select_one(".s-item__title")
            price_tag = item.select_one(".s-item__price")

            if not title_tag or not price_tag:
                continue

            title = title_tag.get_text(strip=True)
            # eBay injects a "Shop on eBay" or "New Listing" sentinel —
            # skip those placeholder items.
            if title.lower().startswith("shop on ebay"):
                continue

            price = _parse_price(price_tag.get_text(strip=True))

            # Try to grab the sold-date element.
            date_tag = item.select_one(".s-item__title--tagblock .POSITIVE")
            sold_date = (
                date_tag.get_text(strip=True).replace("Sold", "").strip()
                if date_tag
                else None
            )

            all_rows.append(
                {
                    "card_id": None,  # eBay doesn't map to pokemontcg IDs
                    "card_name": title,
                    "set_name": None,  # not structured on eBay
                    "condition": None,
                    "price_usd": price,
                    "date_recorded": sold_date or now_utc,
                    "data_source": "ebay",
                }
            )

        time.sleep(EBAY_REQUEST_DELAY)

    logger.info("eBay scraper — extracted %d sold-listing rows", len(all_rows))
    return all_rows


# ═══════════════════════════════════════════════════════════════════════════
# 3.  Sample / fixture data  (for offline testing)
# ═══════════════════════════════════════════════════════════════════════════

_FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"


def load_sample_data() -> list[dict[str, Any]]:
    """Load sample TCG + eBay data from local JSON fixtures.

    This lets you run the full Transform → Load pipeline without making
    any network requests — useful for local development, CI, and testing
    while the pokemontcg.io API transitions to the paid Scrydex service.
    """
    all_rows: list[dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # ── TCG fixture ───────────────────────────────────────────────────
    tcg_file = _FIXTURES_DIR / "sample_tcg_response.json"
    if tcg_file.exists():
        payload = json.loads(tcg_file.read_text(encoding="utf-8"))
        for card in payload.get("data", []):
            prices = card.get("tcgplayer", {}).get("prices", {})
            if not prices:
                all_rows.append(
                    {
                        "card_id": card.get("id", ""),
                        "card_name": card.get("name", ""),
                        "set_name": card.get("set", {}).get("name", ""),
                        "condition": None,
                        "price_usd": None,
                        "date_recorded": now_utc,
                        "data_source": "tcgplayer",
                    }
                )
                continue
            for sub_type, price_obj in prices.items():
                market_price = price_obj.get("market") or price_obj.get("mid")
                all_rows.append(
                    {
                        "card_id": card.get("id", ""),
                        "card_name": card.get("name", ""),
                        "set_name": card.get("set", {}).get("name", ""),
                        "condition": sub_type,
                        "price_usd": market_price,
                        "date_recorded": now_utc,
                        "data_source": "tcgplayer",
                    }
                )
        logger.info("Sample data — loaded %d TCG rows from fixture", len(all_rows))
    else:
        logger.warning("TCG fixture not found at %s", tcg_file)

    # ── eBay fixture ──────────────────────────────────────────────────
    ebay_file = _FIXTURES_DIR / "sample_ebay_results.json"
    if ebay_file.exists():
        ebay_rows = json.loads(ebay_file.read_text(encoding="utf-8"))
        all_rows.extend(ebay_rows)
        logger.info("Sample data — loaded %d eBay rows from fixture", len(ebay_rows))
    else:
        logger.warning("eBay fixture not found at %s", ebay_file)

    logger.info("Sample data — %d total rows ready for transform", len(all_rows))
    return all_rows
