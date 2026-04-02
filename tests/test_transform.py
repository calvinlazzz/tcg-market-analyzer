"""
Unit tests for the Transform layer.

These run without network access — they validate schema enforcement,
price cleaning, deduplication, and missing-value handling.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

from tcg_pipeline.transform import build_dataframe


# ───────────────────────────────────────────────────────────────────────────
# Fixtures
# ───────────────────────────────────────────────────────────────────────────


def _sample_rows() -> list[dict]:
    return [
        {
            "card_id": "base1-4",
            "card_name": "Charizard",
            "set_name": "Base",
            "condition": "holofoil",
            "price_usd": 320.0,
            "date_recorded": "2026-04-02T00:00:00+00:00",
            "data_source": "tcgplayer",
        },
        {
            "card_id": None,
            "card_name": "Charizard Base Set 4/102 Holo",
            "set_name": None,
            "condition": None,
            "price_usd": "$295.00",
            "date_recorded": "2026-04-01T12:00:00+00:00",
            "data_source": "ebay",
        },
        {
            "card_id": "base1-4",
            "card_name": "Charizard",
            "set_name": "Base",
            "condition": "holofoil",
            "price_usd": 320.0,
            "date_recorded": "2026-04-02T00:00:00+00:00",
            "data_source": "tcgplayer",
        },  # exact duplicate of row 0
    ]


# ───────────────────────────────────────────────────────────────────────────
# Tests
# ───────────────────────────────────────────────────────────────────────────


class TestBuildDataframe:
    def test_empty_input_returns_empty_df(self):
        df = build_dataframe([])
        assert df.empty
        assert list(df.columns) == [
            "card_id",
            "card_name",
            "set_name",
            "condition",
            "price_usd",
            "date_recorded",
            "data_source",
        ]

    def test_columns_match_canonical_order(self):
        df = build_dataframe(_sample_rows())
        assert list(df.columns) == [
            "card_id",
            "card_name",
            "set_name",
            "condition",
            "price_usd",
            "date_recorded",
            "data_source",
        ]

    def test_price_string_cleaned(self):
        df = build_dataframe(_sample_rows())
        ebay_row = df[df["data_source"] == "ebay"].iloc[0]
        assert ebay_row["price_usd"] == 295.0

    def test_missing_values_filled(self):
        df = build_dataframe(_sample_rows())
        ebay_row = df[df["data_source"] == "ebay"].iloc[0]
        assert ebay_row["card_id"] == "UNKNOWN"
        assert ebay_row["set_name"] == "UNKNOWN"
        assert ebay_row["condition"] == "unspecified"

    def test_duplicates_dropped(self):
        df = build_dataframe(_sample_rows())
        # The third row is an exact dup of the first → should be dropped.
        assert len(df) == 2

    def test_none_price_stays_none(self):
        rows = [
            {
                "card_id": "xy1-1",
                "card_name": "Venusaur",
                "set_name": "XY",
                "condition": "normal",
                "price_usd": None,
                "date_recorded": "2026-04-02T00:00:00+00:00",
                "data_source": "tcgplayer",
            }
        ]
        df = build_dataframe(rows)
        assert df["price_usd"].iloc[0] is None or (
            isinstance(df["price_usd"].iloc[0], float)
            and math.isnan(df["price_usd"].iloc[0]) is False
            and df["price_usd"].iloc[0] is None
        ) or pd.isna(df["price_usd"].iloc[0])
