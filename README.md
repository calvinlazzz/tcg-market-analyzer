# 🃏 TCG Market Analyzer

An ETL (Extract → Transform → Load) pipeline that tracks **Pokémon card market prices** by pulling data from:

| Source | Method | Data |
|---|---|---|
| **pokemontcg.io API** | REST (`requests`) | Card metadata + TCGplayer market prices |
| **eBay Sold Listings** | Web scraping (`BeautifulSoup4`) | Real-world transaction prices |

Cleaned data lands in a local **SQLite** database (`pokemon_market.db`) ready for analysis or dashboarding.

---

## Project Structure

```
tcg-market-analyzer/
├── tcg_pipeline/
│   ├── __init__.py
│   ├── config.py        # All settings, paths, env-var loading
│   ├── extract.py       # PokémonTCG API + eBay scraper
│   ├── transform.py     # pandas cleaning / schema enforcement
│   ├── load.py          # SQLite table creation + insert
│   └── main.py          # CLI orchestrator (Extract → Transform → Load)
├── tests/
│   ├── __init__.py
│   ├── test_transform.py
│   └── test_load.py
├── data/                # Created at runtime — holds pokemon_market.db
├── logs/                # Created at runtime — daily log files
├── .env.example         # Copy to .env and fill in your API key
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Quick Start (macOS)

### 1. Clone & enter the project

```bash
git clone https://github.com/calvinlazzz/tcg-market-analyzer.git
cd tcg-market-analyzer
```

### 2. Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. (Optional) Add your API key

```bash
cp .env.example .env
# Edit .env and paste your free key from https://pokemontcg.io
```

A key is **not required** — the API works without one, just at a lower rate limit.

### 5. Run the pipeline

```bash
# Full pipeline — both TCGplayer API + eBay scraping
python -m tcg_pipeline.main

# TCGplayer data only
python -m tcg_pipeline.main --source tcgplayer

# eBay only, custom search
python -m tcg_pipeline.main --source ebay --ebay-term "Pikachu Illustrator"

# Custom pokemontcg.io query
python -m tcg_pipeline.main --source tcgplayer --tcg-query 'name:"Charizard"'
```

### 6. Inspect the database

```bash
sqlite3 data/pokemon_market.db "SELECT card_name, price_usd, data_source FROM card_prices LIMIT 10;"
```

### 7. Run tests

```bash
pip install pytest
pytest tests/ -v
```

---

## Database Schema

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER | Auto-increment primary key |
| `card_id` | TEXT | pokemontcg.io card ID (e.g. `base1-4`) |
| `card_name` | TEXT | Card name |
| `set_name` | TEXT | Set name (e.g. "Base") |
| `condition` | TEXT | Condition / sub-type (e.g. `holofoil`, `unspecified`) |
| `price_usd` | REAL | Market price in USD |
| `date_recorded` | TEXT | ISO-8601 timestamp of when the price was recorded |
| `data_source` | TEXT | `tcgplayer` or `ebay` |

A composite **UNIQUE** constraint on `(card_id, card_name, condition, price_usd, date_recorded, data_source)` prevents inserting exact-duplicate rows when the pipeline is re-run.

---

## Configuration

All settings can be tuned via environment variables (or a `.env` file). See `.env.example` for the full list.

| Variable | Default | Description |
|---|---|---|
| `POKEMONTCG_API_KEY` | *(none)* | Free API key — raises rate limit |
| `POKEMONTCG_DEFAULT_QUERY` | `set.id:"base1"` | Default card search query |
| `EBAY_DEFAULT_SEARCH_TERM` | `Charizard Base Set 4/102` | eBay search term |
| `EBAY_REQUEST_DELAY` | `2.0` | Seconds between eBay requests |
| `LOG_LEVEL` | `INFO` | Python log level |

---

## Future Plans

- [ ] Dockerize for headless deployment on a Linux homelab
- [ ] Add a scheduled cron / systemd timer to run daily
- [ ] Build a Grafana or Streamlit dashboard on top of the SQLite DB
- [ ] Add more data sources (e.g. TCGplayer direct API when available)

---

## License

MIT
