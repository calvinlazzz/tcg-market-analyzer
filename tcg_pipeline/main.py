#!/usr/bin/env python3
"""
TCG Market Analyzer — ETL orchestrator.

Usage
-----
    # Run with local sample data (no network — great for testing):
    python -m tcg_pipeline.main --sample-data

    # Run the full pipeline (TCGplayer API + eBay scrape):
    python -m tcg_pipeline.main

    # TCGplayer only:
    python -m tcg_pipeline.main --source tcgplayer

    # eBay only (custom search term):
    python -m tcg_pipeline.main --source ebay --ebay-term "Pikachu Illustrator"

    # Custom pokemontcg.io query:
    python -m tcg_pipeline.main --source tcgplayer --tcg-query 'name:"Charizard"'
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from tcg_pipeline.config import (
    DB_PATH,
    EBAY_DEFAULT_SEARCH_TERM,
    POKEMONTCG_DEFAULT_QUERY,
    configure_logging,
)
from tcg_pipeline.extract import (
    ebay_api_available,
    fetch_ebay_api,
    fetch_pokemontcg_cards,
    load_sample_data,
    scrape_ebay_sold,
)
from tcg_pipeline.load import init_db, load_dataframe, row_count
from tcg_pipeline.transform import build_dataframe

logger = logging.getLogger(__name__)


# ───────────────────────────────────────────────────────────────────────────
# CLI
# ───────────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pokémon TCG Market Analyzer — ETL pipeline",
    )
    parser.add_argument(
        "--source",
        choices=["all", "tcgplayer", "ebay"],
        default="all",
        help="Which data source(s) to extract from (default: all).",
    )
    parser.add_argument(
        "--tcg-query",
        default=POKEMONTCG_DEFAULT_QUERY,
        help="pokemontcg.io search query (default: Base Set).",
    )
    parser.add_argument(
        "--ebay-term",
        default=EBAY_DEFAULT_SEARCH_TERM,
        help="eBay search term (default: Charizard Base Set 4/102).",
    )
    parser.add_argument(
        "--ebay-pages",
        type=int,
        default=2,
        help="Number of eBay result pages to scrape (default: 2).",
    )
    parser.add_argument(
        "--sample-data",
        action="store_true",
        default=False,
        help="Use local fixture files instead of live APIs (offline testing).",
    )
    return parser.parse_args(argv)


# ───────────────────────────────────────────────────────────────────────────
# Pipeline
# ───────────────────────────────────────────────────────────────────────────


def run_pipeline(args: argparse.Namespace) -> None:
    """Execute Extract → Transform → Load."""
    start = time.monotonic()

    # ── INIT ──────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("TCG Market Analyzer — pipeline start")
    logger.info("=" * 60)
    init_db()

    raw_rows: list[dict] = []

    # ── EXTRACT ───────────────────────────────────────────────────────────
    if args.sample_data:
        logger.info("Using LOCAL SAMPLE DATA — no network requests.")
        raw_rows = load_sample_data()
    else:
        if args.source in ("all", "tcgplayer"):
            try:
                tcg_rows = fetch_pokemontcg_cards(query=args.tcg_query)
                raw_rows.extend(tcg_rows)
            except Exception:
                logger.exception("TCGplayer extraction failed")

        if args.source in ("all", "ebay"):
            try:
                if ebay_api_available():
                    logger.info("eBay API credentials detected — using Browse API.")
                    ebay_rows = fetch_ebay_api(search_term=args.ebay_term)
                else:
                    logger.info(
                        "No eBay API credentials — falling back to HTML scraper. "
                        "Set EBAY_APP_ID + EBAY_CERT_ID in .env to use the API."
                    )
                    ebay_rows = scrape_ebay_sold(
                        search_term=args.ebay_term,
                        max_pages=args.ebay_pages,
                    )
                raw_rows.extend(ebay_rows)
            except Exception:
                logger.exception("eBay extraction failed")

    if not raw_rows:
        logger.warning("No data extracted from any source — exiting early.")
        return

    # ── TRANSFORM ─────────────────────────────────────────────────────────
    df = build_dataframe(raw_rows)
    if df.empty:
        logger.warning("DataFrame is empty after transform — nothing to load.")
        return

    # ── LOAD ──────────────────────────────────────────────────────────────
    new_rows = load_dataframe(df)

    # ── SUMMARY ───────────────────────────────────────────────────────────
    elapsed = time.monotonic() - start
    total = row_count()
    logger.info("-" * 60)
    logger.info("Pipeline finished in %.1f s", elapsed)
    logger.info("  New rows inserted : %d", new_rows)
    logger.info("  Total rows in DB  : %d", total)
    logger.info("  Database file     : %s", DB_PATH)
    logger.info("-" * 60)


# ───────────────────────────────────────────────────────────────────────────
# Entrypoint
# ───────────────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    args = _parse_args(argv)
    try:
        run_pipeline(args)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user.")
        sys.exit(130)
    except Exception:
        logger.exception("Pipeline failed with an unhandled exception")
        sys.exit(1)


if __name__ == "__main__":
    main()
