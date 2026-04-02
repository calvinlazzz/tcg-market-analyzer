"""
TCG Market Analyzer — Transform layer.

Responsibilities:
    • Build a pandas DataFrame from raw extracted dicts.
    • Enforce the canonical schema  (see config.CANONICAL_COLUMNS).
    • Clean price strings  ("$12.50" → 12.50).
    • Handle missing values with sensible defaults.
    • Drop exact-duplicate rows to avoid re-loading identical records.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

from tcg_pipeline.config import CANONICAL_COLUMNS

logger = logging.getLogger(__name__)

# Pre-compiled regex for stripping stray dollar signs / commas from prices
# that slipped through the extract layer (defensive).
_PRICE_JUNK_RE = re.compile(r"[^\d.]")


# ───────────────────────────────────────────────────────────────────────────
# Public API
# ───────────────────────────────────────────────────────────────────────────


def build_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert a list of flat dicts into a canonical DataFrame.

    Steps
    -----
    1. Create DataFrame; add any missing canonical columns.
    2. Normalise *price_usd* to ``float64``.
    3. Fill missing text fields with sensible defaults.
    4. Enforce column order and drop full-row duplicates.
    """
    if not rows:
        logger.warning("build_dataframe called with 0 rows — returning empty DataFrame")
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    df = pd.DataFrame(rows)

    # 1 — ensure every canonical column exists
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Keep only canonical columns (drop any extras the extractor sneaked in).
    df = df[CANONICAL_COLUMNS].copy()

    # 2 — price cleanup
    df["price_usd"] = df["price_usd"].apply(_clean_price)

    # 3 — fill text defaults
    df["card_id"] = df["card_id"].fillna("UNKNOWN")
    df["card_name"] = df["card_name"].fillna("UNKNOWN")
    df["set_name"] = df["set_name"].fillna("UNKNOWN")
    df["condition"] = df["condition"].fillna("unspecified")
    df["data_source"] = df["data_source"].fillna("unknown")

    # 4 — strip whitespace from string columns
    str_cols = ["card_id", "card_name", "set_name", "condition", "data_source"]
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    # 5 — drop exact duplicates
    before = len(df)
    df.drop_duplicates(inplace=True)
    after = len(df)
    if before != after:
        logger.info("Dropped %d exact-duplicate rows", before - after)

    logger.info(
        "Transform complete — %d rows, %d with non-null prices",
        len(df),
        df["price_usd"].notna().sum(),
    )
    return df.reset_index(drop=True)


# ───────────────────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────────────────


def _clean_price(value: Any) -> float | None:
    """Coerce *value* to a float, stripping ``$`` and ``,`` if present.

    Returns ``None`` when the value cannot be interpreted as a number.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    # String path — strip junk characters.
    cleaned = _PRICE_JUNK_RE.sub("", str(value)).strip()
    if not cleaned:
        return None

    try:
        return float(cleaned)
    except ValueError:
        logger.debug("Could not parse price from %r", value)
        return None
