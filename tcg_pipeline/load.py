"""
TCG Market Analyzer — Load layer.

Handles all SQLite interactions:
    • Create the ``card_prices`` table (idempotent).
    • Append cleaned DataFrames while preventing exact-duplicate rows
      (same card_id + date_recorded + data_source + condition + price_usd).
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

from tcg_pipeline.config import CANONICAL_COLUMNS, DB_PATH

logger = logging.getLogger(__name__)

# ───────────────────────────────────────────────────────────────────────────
# DDL
# ───────────────────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS card_prices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id         TEXT    NOT NULL,
    card_name       TEXT    NOT NULL,
    set_name        TEXT,
    condition       TEXT,
    price_usd       REAL,
    date_recorded   TEXT    NOT NULL,
    data_source     TEXT    NOT NULL,

    -- Composite unique constraint prevents loading the exact same row twice.
    UNIQUE (card_id, card_name, condition, price_usd, date_recorded, data_source)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_card_prices_card_id
    ON card_prices (card_id);
CREATE INDEX IF NOT EXISTS idx_card_prices_date
    ON card_prices (date_recorded);
CREATE INDEX IF NOT EXISTS idx_card_prices_source
    ON card_prices (data_source);
"""


# ───────────────────────────────────────────────────────────────────────────
# Public API
# ───────────────────────────────────────────────────────────────────────────


def init_db(db_path: Path | str | None = None) -> None:
    """Create the database file and table if they don't already exist."""
    db_path = Path(db_path or DB_PATH)
    logger.info("Initialising database at %s", db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(_CREATE_TABLE_SQL + _CREATE_INDEX_SQL)
        conn.commit()
        logger.info("Database schema ready.")
    finally:
        conn.close()


def load_dataframe(
    df: pd.DataFrame,
    db_path: Path | str | None = None,
) -> int:
    """Insert *df* into ``card_prices``, skipping exact duplicates.

    Returns the number of **newly inserted** rows.
    """
    db_path = Path(db_path or DB_PATH)

    if df.empty:
        logger.warning("load_dataframe called with empty DataFrame — nothing to insert.")
        return 0

    # Ensure column order matches the table.
    df = df[CANONICAL_COLUMNS].copy()

    conn = sqlite3.connect(str(db_path))
    inserted = 0
    try:
        cursor = conn.cursor()
        for _, row in df.iterrows():
            try:
                cursor.execute(
                    """
                    INSERT INTO card_prices
                        (card_id, card_name, set_name, condition,
                         price_usd, date_recorded, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["card_id"],
                        row["card_name"],
                        row["set_name"],
                        row["condition"],
                        row["price_usd"],
                        row["date_recorded"],
                        row["data_source"],
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                # Duplicate row — skip silently (UNIQUE constraint).
                pass
        conn.commit()
    finally:
        conn.close()

    logger.info(
        "Loaded %d new rows into card_prices  (%d duplicates skipped)",
        inserted,
        len(df) - inserted,
    )
    return inserted


def row_count(db_path: Path | str | None = None) -> int:
    """Return total row count in ``card_prices``."""
    db_path = Path(db_path or DB_PATH)
    conn = sqlite3.connect(str(db_path))
    try:
        (count,) = conn.execute("SELECT COUNT(*) FROM card_prices").fetchone()
        return count
    finally:
        conn.close()
