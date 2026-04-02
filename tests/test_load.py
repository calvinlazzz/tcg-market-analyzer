"""
Unit tests for the Load layer.

Uses an in-memory SQLite database so no file I/O is needed.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from tcg_pipeline.config import CANONICAL_COLUMNS
from tcg_pipeline.load import init_db, load_dataframe, row_count


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    """Yield a path to a fresh temp database."""
    db = tmp_path / "test.db"
    init_db(db)
    return db


def _make_df(n: int = 3) -> pd.DataFrame:
    rows = [
        {
            "card_id": f"base1-{i}",
            "card_name": f"Card {i}",
            "set_name": "Base",
            "condition": "normal",
            "price_usd": 10.0 + i,
            "date_recorded": f"2026-04-0{i + 1}T00:00:00+00:00",
            "data_source": "tcgplayer",
        }
        for i in range(n)
    ]
    return pd.DataFrame(rows, columns=CANONICAL_COLUMNS)


class TestInitDb:
    def test_table_created(self, tmp_db: Path):
        conn = sqlite3.connect(str(tmp_db))
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='card_prices'"
        ).fetchall()
        conn.close()
        assert len(tables) == 1

    def test_idempotent(self, tmp_db: Path):
        # Calling init_db again should not raise.
        init_db(tmp_db)


class TestLoadDataframe:
    def test_inserts_rows(self, tmp_db: Path):
        df = _make_df(3)
        inserted = load_dataframe(df, tmp_db)
        assert inserted == 3
        assert row_count(tmp_db) == 3

    def test_skips_exact_duplicates(self, tmp_db: Path):
        df = _make_df(3)
        load_dataframe(df, tmp_db)
        inserted_again = load_dataframe(df, tmp_db)
        assert inserted_again == 0
        assert row_count(tmp_db) == 3

    def test_empty_df_returns_zero(self, tmp_db: Path):
        empty = pd.DataFrame(columns=CANONICAL_COLUMNS)
        assert load_dataframe(empty, tmp_db) == 0
