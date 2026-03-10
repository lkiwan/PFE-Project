# Casablanca Bourse Data Scrapers

This project contains Python scripts designed to scrape stock market data for the Casablanca Bourse and store it in a PostgreSQL database. It is split into two levels of data collection: End-of-Day (OHLCV) data and Order Book (Level 2) data.

## 📁 Files Overview

### 1. `scrap_level1.py` (End of Day Data)
**Purpose:** Scrapes the daily OHLCV (Open, High, Low, Close, Volume) data for target stocks at the end of the trading session.
*   **Source:** Casablanca Bourse Live Market Page (`https://www.casablanca-bourse.com/fr/live-market/marche-actions-groupement/`).
*   **Method:** Uses Selenium (Headless Chrome) to execute the page's JavaScript and load the table, then uses `pandas.read_html` to extract the raw data.
*   **Logic:**
    *   Cleans and formats numeric text strings (removing spaces, converting French commas to dots).
    *   Maps the raw instrument names from the website to internal database symbols using the `ref.instrument_aliases` table (with a built-in fallback dictionary).
    *   Saves the data to the `md.eod_bars` table.
    *   Uses an **UPSERT** (`ON CONFLICT DO UPDATE`) mechanism so that if the script runs multiple times on the same `trade_date`, it safely updates the existing row instead of throwing duplicate key errors.
*   **Execution:** Runs as a continuous service, resting during market hours and automatically triggering after the market closes (e.g., 15:40) on weekdays.

### 2. `scrap_level2.py` (Order Book Data)
**Purpose:** Scrapes intraday Level 2 Order Book data, capturing the Best Bid (Prix d'Achat) and Best Ask (Prix de Vente) along with their respective quantities.
*   **Source:** Medias24 Stock Pages (`https://medias24.com/leboursier/fiche-action?action={slug}&valeur=carnet-d-ordres`).
*   **Method:** Uses Selenium to load individual HTML stock pages, waiting specifically for the "Carnet d'ordres" table to render to bypass Cloudflare and lazy-loading protections.
*   **Logic:**
    *   Maps internal database symbols to Medias24 URL "slugs" (e.g., `IAM` -> `maroc-telecom`).
    *   Extracts the top row of the Order Book table via Pandas to get the best current Bid and Ask.
    *   Takes a timestamped snapshot and inserts the row into the `md.order_books` table.
*   **Execution:** Runs continuously on a loop **during** market opening hours (09:30 - 15:40), checking the books every 5 minutes (300 seconds).

---

## 🗄️ Database Schema Representation

The scrapers are built to interact with a strongly-typed PostgreSQL database divided into two primary schemas: `ref` (Reference Data) and `md` (Market Data).

### 1. Reference Data Schema (`ref`)

**`ref.instruments`**
The core table containing all trackable assets.
*   `instrument_id` (INTEGER, Primary Key)
*   `exchange_id` (INTEGER, Foreign Key to `ref.exchanges`)
*   `symbol` (VARCHAR) - e.g., 'IAM', 'ATW'
*   `name` (VARCHAR)
*   `instrument_type` (VARCHAR)
*   `is_active` (BOOLEAN)

**`ref.instrument_aliases`**
Used by the scrapers to translate website-specific names into the canonical `instrument_id`.
*   `alias_id` (SERIAL, Primary Key)
*   `instrument_id` (INTEGER, Foreign Key to `ref.instruments`)
*   `source_name` (VARCHAR) - e.g., 'casablanca_bourse' or 'medias24'
*   `alias_value` (VARCHAR) - The exact string found on the website (e.g., 'DOUJA PROM ADDOHA' or 'douja-prom-addoha')

### 2. Market Data Schema (`md`)

**`md.eod_bars`** (Populated by `scrap_level1.py`)
Stores daily historical price data.
*   `id` (SERIAL, Primary Key)
*   `instrument_id` (INTEGER, Foreign Key)
*   `trade_date` (DATE)
*   `price` (NUMERIC) - Closing / Last traded price
*   `open` (NUMERIC)
*   `high` (NUMERIC)
*   `low` (NUMERIC)
*   `volume` (NUMERIC)
*   `change_pct` (NUMERIC)
*   `source_name` (VARCHAR)
*   `scraped_at` (TIMESTAMP)
*   *Unique Constraint:* `(instrument_id, trade_date)`

**`md.order_books`** (Populated by `scrap_level2.py`)
Stores point-in-time snapshots of the order book.
*   `id` (SERIAL, Primary Key)
*   `instrument_id` (INTEGER, Foreign Key)
*   `snapshot_time` (TIMESTAMP)
*   `bid_price` (NUMERIC)
*   `bid_qty` (NUMERIC)
*   `ask_price` (NUMERIC)
*   `ask_qty` (NUMERIC)
*   `source_name` (VARCHAR)
*   `scraped_at` (TIMESTAMP)

---

## 🚀 How to Run

Before running, ensure you have set the `DATABASE_URL` environment variable:

**PowerShell (Windows):**
```powershell
$env:DATABASE_URL="postgresql://postgres:123456@localhost:5432/PFE"
```

**Running Level 1 (EOD data):**
```bash
python scrap_level1.py
```

**Running Level 2 (Order Book data):**
```bash
python scrap_level2.py
```
