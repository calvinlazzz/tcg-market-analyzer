"""
TCG Market Analyzer — configuration and shared constants.

All tunables live here so the rest of the pipeline stays free of magic strings.
Environment variables (loaded from .env) take precedence when present.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
load_dotenv()  # reads .env in project root if it exists

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "pokemon_market.db"

# ---------------------------------------------------------------------------
# PokémonTCG API  (https://docs.pokemontcg.io)
# ---------------------------------------------------------------------------
POKEMONTCG_API_BASE = "https://api.pokemontcg.io/v2"
POKEMONTCG_API_KEY: str | None = os.getenv("POKEMONTCG_API_KEY")  # optional, raises rate-limit

# Default search query — fetches Base Set cards.  Override via CLI or .env.
POKEMONTCG_DEFAULT_QUERY = os.getenv(
    "POKEMONTCG_DEFAULT_QUERY",
    'set.id:"base1"',
)

# Maximum number of cards to pull per API page (API max is 250).
POKEMONTCG_PAGE_SIZE = int(os.getenv("POKEMONTCG_PAGE_SIZE", "250"))

# ---------------------------------------------------------------------------
# eBay scraping
# ---------------------------------------------------------------------------
EBAY_SEARCH_URL = "https://www.ebay.com/sch/i.html"

# Default card to scrape.  Can be overridden at runtime.
EBAY_DEFAULT_SEARCH_TERM = os.getenv(
    "EBAY_DEFAULT_SEARCH_TERM",
    "Charizard Base Set 4/102",
)

# Polite delay between HTTP requests (seconds) to avoid throttling.
EBAY_REQUEST_DELAY = float(os.getenv("EBAY_REQUEST_DELAY", "2.0"))

# Realistic browser UA so eBay doesn't block the scraper outright.
HTTP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Schema — canonical column order for the card_prices table
# ---------------------------------------------------------------------------
CANONICAL_COLUMNS = [
    "card_id",
    "card_name",
    "set_name",
    "condition",
    "price_usd",
    "date_recorded",
    "data_source",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = "%(asctime)s | %(name)-22s | %(levelname)-8s | %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def configure_logging() -> None:
    """Set up root logger with console + rotating-file handlers."""
    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)

    # Avoid duplicate handlers when called more than once (e.g. tests).
    if root.handlers:
        return

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FMT)

    # Console
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    # File (append mode — one file per day keeps things manageable)
    from datetime import date

    log_file = LOG_DIR / f"pipeline_{date.today().isoformat()}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    root.addHandler(fh)
