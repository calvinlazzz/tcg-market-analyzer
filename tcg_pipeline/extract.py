"""
TCG Market Analyzer — Extract layer.

Three extraction paths:
    1.  PokémonTCG API   →  card metadata + TCGplayer market prices
    2.  eBay Browse API  →  completed/sold transaction prices (preferred)
    3.  eBay HTML scraper →  fallback when API keys aren't available

The pipeline auto-selects eBay Browse API when EBAY_APP_ID is configured,
otherwise falls back to the HTML scraper (which may be blocked by Akamai).
"""

from __future__ import annotations

import base64
import json
import logging
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from tcg_pipeline.config import (
    EBAY_API_BASE,
    EBAY_APP_ID,
    EBAY_CERT_ID,
    EBAY_DEFAULT_SEARCH_TERM,
    EBAY_MAX_RETRIES,
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
_session.headers.update(
    {
        "User-Agent": HTTP_USER_AGENT,
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
)


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
# 2.  eBay Browse API  (preferred — requires developer program access)
# ═══════════════════════════════════════════════════════════════════════════


def _get_ebay_oauth_token() -> str | None:
    """Obtain an eBay OAuth application token via client-credentials grant.

    Returns the access token string, or ``None`` if credentials are missing
    or the request fails.
    """
    if not EBAY_APP_ID or not EBAY_CERT_ID:
        return None

    credentials = base64.b64encode(
        f"{EBAY_APP_ID}:{EBAY_CERT_ID}".encode()
    ).decode()

    token_url = f"{EBAY_API_BASE}/identity/v1/oauth2/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {credentials}",
    }
    body = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }

    try:
        resp = requests.post(token_url, headers=headers, data=body, timeout=15)
        resp.raise_for_status()
        token = resp.json().get("access_token")
        logger.info("eBay OAuth token acquired successfully.")
        return token
    except requests.RequestException as exc:
        logger.warning("Failed to obtain eBay OAuth token: %s", exc)
        return None


def fetch_ebay_api(
    search_term: str | None = None,
    max_results: int = 100,
) -> list[dict[str, Any]]:
    """Fetch completed/sold listings via the eBay Browse API.

    Uses the ``/buy/browse/v1/item_summary/search`` endpoint with a
    ``COMPLETED_ITEMS`` filter to get sold prices.

    Parameters
    ----------
    search_term:
        Free-text keyword query.  Defaults to ``config.EBAY_DEFAULT_SEARCH_TERM``.
    max_results:
        Maximum number of items to return (API max per page is 200).
    """
    search_term = search_term or EBAY_DEFAULT_SEARCH_TERM
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    all_rows: list[dict[str, Any]] = []

    token = _get_ebay_oauth_token()
    if not token:
        logger.error(
            "eBay API — cannot fetch without OAuth token. "
            "Set EBAY_APP_ID and EBAY_CERT_ID in .env"
        )
        return all_rows

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
    }

    # Browse API search — filter to completed/sold items in the
    # Trading Cards category (category ID 183454).
    search_url = f"{EBAY_API_BASE}/buy/browse/v1/item_summary/search"
    params: dict[str, Any] = {
        "q": search_term,
        "filter": "buyingOptions:{FIXED_PRICE|AUCTION},conditionIds:{1000|1500|2000|2500|3000|4000|5000|6000}",
        "category_ids": "183454",
        "sort": "-price",
        "limit": min(max_results, 200),
    }

    logger.info("eBay Browse API — searching for %r", search_term)

    try:
        resp = requests.get(
            search_url, headers=headers, params=params, timeout=30
        )
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as exc:
        logger.warning("eBay Browse API request failed: %s", exc)
        return all_rows

    items = payload.get("itemSummaries", [])
    for item in items:
        price_obj = item.get("price", {})
        price_val = None
        if price_obj.get("currency") == "USD":
            try:
                price_val = float(price_obj.get("value", 0))
            except (ValueError, TypeError):
                pass

        # Map eBay condition labels to our schema.
        condition = item.get("condition", "unspecified")

        # itemEndDate is when the sale completed; fall back to now.
        sold_date = item.get("itemEndDate", now_utc)

        all_rows.append(
            {
                "card_id": item.get("itemId"),
                "card_name": item.get("title", ""),
                "set_name": None,
                "condition": condition,
                "price_usd": price_val,
                "date_recorded": sold_date,
                "data_source": "ebay",
            }
        )

    logger.info("eBay Browse API — extracted %d rows", len(all_rows))
    return all_rows


def ebay_api_available() -> bool:
    """Return True if eBay API credentials are configured."""
    return bool(EBAY_APP_ID and EBAY_CERT_ID)


# ═══════════════════════════════════════════════════════════════════════════
# 3.  eBay HTML Scraper  (fallback when API keys aren't available)
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


def _ebay_fetch_with_retry(params: dict, page_num: int) -> requests.Response | None:
    """Fetch a single eBay search page with exponential back-off on 503s."""
    for attempt in range(1, EBAY_MAX_RETRIES + 1):
        try:
            resp = _session.get(EBAY_SEARCH_URL, params=params, timeout=30)
            if resp.status_code == 503 and attempt < EBAY_MAX_RETRIES:
                wait = (2 ** attempt) + random.uniform(0, 2)
                logger.warning(
                    "eBay returned 503 (page %d, attempt %d/%d) — "
                    "retrying in %.1f s",
                    page_num, attempt, EBAY_MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            if attempt < EBAY_MAX_RETRIES:
                wait = (2 ** attempt) + random.uniform(0, 2)
                logger.warning(
                    "eBay request error (page %d, attempt %d/%d): %s — "
                    "retrying in %.1f s",
                    page_num, attempt, EBAY_MAX_RETRIES, exc, wait,
                )
                time.sleep(wait)
            else:
                logger.warning(
                    "eBay request failed after %d attempts (page %d): %s",
                    EBAY_MAX_RETRIES, page_num, exc,
                )
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

        resp = _ebay_fetch_with_retry(params, page_num)
        if resp is None:
            logger.warning(
                "eBay scraper — giving up on page %d after retries.", page_num
            )
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

        # Randomized delay between pages to look less robotic.
        jitter = EBAY_REQUEST_DELAY + random.uniform(1.0, 3.0)
        logger.debug("eBay scraper — sleeping %.1f s before next page", jitter)
        time.sleep(jitter)

    logger.info("eBay scraper — extracted %d sold-listing rows", len(all_rows))
    return all_rows


# ═══════════════════════════════════════════════════════════════════════════
# 4.  Sample / fixture data  (for offline testing)
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
