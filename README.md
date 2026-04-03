# 🃏 TCG Market Analyzer

An ETL (Extract → Transform → Load) pipeline that tracks **Pokémon card market prices** by pulling data from:

| Source | Method | Data |
|---|---|---|
| **pokemontcg.io API** | REST (`requests`) | Card metadata + TCGplayer market prices |
| **eBay Browse API** | REST + OAuth (`requests`) | Sold transaction prices — preferred |
| **eBay Sold Listings** | Web scraping (`BeautifulSoup4`) | Fallback when API keys aren't set |

Cleaned data lands in a local **SQLite** database (`pokemon_market.db`) ready for analysis or dashboarding.

> **⚠️ API Note (Apr 2026):** pokemontcg.io has been absorbed into [Scrydex](https://scrydex.com), a paid API service (starts at $29/mo). The legacy `api.pokemontcg.io/v2` endpoint still responds for now, but may be retired. Use `--sample-data` to develop and test the full pipeline offline without any API key or network access.

> **🛠️ eBay Integration:** The pipeline supports two eBay modes. When `EBAY_APP_ID` + `EBAY_CERT_ID` are set in `.env`, it uses the official [eBay Browse API](https://developer.ebay.com/api-docs/buy/browse/overview.html) (structured JSON, reliable). Without those keys, it falls back to an HTML scraper which eBay's Akamai CDN may block. Apply for eBay developer access at [developer.ebay.com](https://developer.ebay.com/develop/apis).

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
│   ├── test_load.py
│   └── fixtures/
│       ├── sample_tcg_response.json
│       └── sample_ebay_results.json
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

### 4. (Optional) Add your API keys

```bash
cp .env.example .env
# Edit .env and fill in your keys:
#   - POKEMONTCG_API_KEY  — free from https://dev.pokemontcg.io (or Scrydex)
#   - EBAY_APP_ID         — from https://developer.ebay.com (once accepted)
#   - EBAY_CERT_ID        — from https://developer.ebay.com (once accepted)
```

No keys are required to get started — use `--sample-data` for fully offline testing.

### 5. Run the pipeline

```bash
# ✅ RECOMMENDED FIRST RUN — uses local fixture data, no API/network needed
python -m tcg_pipeline.main --sample-data

# Full pipeline — both TCGplayer API + eBay scraping (requires network)
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
| `POKEMONTCG_API_KEY` | *(none)* | API key (free for legacy, paid for Scrydex) |
| `POKEMONTCG_API_BASE` | `https://api.pokemontcg.io/v2` | Override to point at Scrydex when ready |
| `POKEMONTCG_DEFAULT_QUERY` | `set.id:"base1"` | Default card search query |
| `EBAY_APP_ID` | *(none)* | eBay Developer Client ID — enables Browse API |
| `EBAY_CERT_ID` | *(none)* | eBay Developer Client Secret |
| `EBAY_API_ENVIRONMENT` | `production` | `sandbox` or `production` |
| `EBAY_DEFAULT_SEARCH_TERM` | `Charizard Base Set 4/102` | eBay search term |
| `EBAY_REQUEST_DELAY` | `3.0` | Base seconds between eBay requests (+random jitter) |
| `EBAY_MAX_RETRIES` | `3` | Retry attempts with back-off on eBay 503s |
| `LOG_LEVEL` | `INFO` | Python log level |

---

## Future Plans

- [ ] Migrate from legacy pokemontcg.io API to Scrydex paid API
- [ ] Dockerize for headless deployment on a Linux homelab
- [ ] Add a scheduled cron / systemd timer to run daily
- [ ] Build a Grafana or Streamlit dashboard on top of the SQLite DB
- [ ] Add more data sources (e.g. TCGplayer direct API when available)

---

## License

MIT
