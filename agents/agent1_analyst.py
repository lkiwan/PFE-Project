"""
AI Agent 1 — Whale Analyst
Analyzes Moroccan stocks using qwen2.5:7b via Ollama.
Pre-fetches all market data in Python, then asks the LLM to analyze and save predictions.

Usage:
    python agents/agent1_analyst.py                  # Analyze all 11 stocks
    python agents/agent1_analyst.py --symbol ATW     # Analyze single stock

Data Sources:
    ┌─────────────────────────┬────────────────────────┬──────────────────────────────┐
    │ Tool / Data             │ DB Table               │ Source (scraped by)          │
    ├─────────────────────────┼────────────────────────┼──────────────────────────────┤
    │ Stock prices (18 days)  │ md.eod_bars            │ scrap_level1.py (CSE)        │
    │ Historical trends       │ md.historical_prices   │ ingest_historical.py (CSVs)  │
    │ MASI index              │ md.market_index        │ masi_scraper.py (Investing)  │
    │ Order book (bid/ask)    │ md.order_books         │ scrap_level2.py (Medias24)   │
    │ News articles           │ md.news_articles       │ boursenews + medias24 news   │
    │ Technical indicators    │ computed from eod_bars  │ Calculated (SMA, RSI, MACD)  │
    │ Prediction output       │ md.predictions         │ Written by this agent        │
    └─────────────────────────┴────────────────────────┴──────────────────────────────┘
"""
import argparse
import json
import logging
import sys
import os
import subprocess
import requests
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Logging Setup ────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"agent1_{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("whale_analyst")

from .agent import Agent
from agno.models.openai import OpenAIChat
from sqlalchemy import create_engine, text

from agents.tools.market_data import (
    get_stock_history,
    get_historical_trend,
    get_masi_index,
    get_orderbook,
)
from agents.tools.news_data import get_stock_news
from agents.tools.technical import compute_technical
from agents.output.prediction_writer import save_prediction

DB_URL = "postgresql://postgres:123456@localhost:5432/PFE"
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)


def check_ollama_status():
    """Check if Ollama is running and accessible."""
    try:
        response = requests.get("http://localhost:11434/", timeout=2)
        if response.status_code == 200:
            logger.info("✅ Ollama is running and accessible.")
            return True
        else:
            logger.error(f"❌ Ollama returned unexpected status code: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        logger.error("❌ Could not connect to Ollama at http://localhost:11434.")
        logger.error("   Please ensure Ollama is running (run 'ollama serve' in a terminal).")
        return False
    except Exception as e:
        logger.error(f"❌ Error checking Ollama status: {e}")
        return False



def get_all_symbols():
    """Load active stock symbols from ref.instruments."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT symbol FROM ref.instruments WHERE is_active = TRUE ORDER BY symbol")
        ).fetchall()
    return [row[0] for row in rows]


def prefetch_data(symbol: str, max_date: str = None) -> str:
    """
    Pre-fetch ALL market data for a stock using Python.
    Returns a formatted string with all data ready for LLM analysis.
    
    This is the key design decision: instead of making the 7B model 
    orchestrate 6 tool calls, we fetch everything upfront and give 
    it as context. The LLM only needs to ANALYZE and call save_prediction.
    """
    logger.info(f"[{symbol}] Fetching stock history from md.eod_bars (source: scrap_level1.py / CSE)")
    stock_data = get_stock_history(symbol, max_date=max_date)
    logger.info(f"[{symbol}] Stock history: {stock_data[:200]}...")

    logger.info(f"[{symbol}] Fetching historical trends from md.historical_prices (source: Investing.com CSVs)")
    hist_data = get_historical_trend(symbol, "weekly", max_date=max_date)
    logger.info(f"[{symbol}] Historical trend: {hist_data[:200]}...")

    logger.info(f"[{symbol}] Fetching MASI index from md.market_index (source: masi_scraper.py / Investing.com)")
    masi_data = get_masi_index(max_date=max_date)
    logger.info(f"[{symbol}] MASI data: {masi_data[:200]}...")

    logger.info(f"[{symbol}] Fetching orderbook from md.order_books (source: scrap_level2.py / Medias24)")
    ob_data = get_orderbook(symbol, max_date=max_date)
    logger.info(f"[{symbol}] Orderbook: {ob_data[:200]}...")

    logger.info(f"[{symbol}] Fetching news from md.news_articles (source: boursenews + medias24 scrapers)")
    # We won't filter news by max_date for now, assuming macro-level sentiment holds
    news_data = get_stock_news(symbol)
    logger.info(f"[{symbol}] News: {news_data[:200]}...")

    logger.info(f"[{symbol}] Computing technical indicators (SMA, RSI, MACD from md.eod_bars)")
    tech_data = compute_technical(symbol, max_date=max_date)
    logger.info(f"[{symbol}] Technicals: {tech_data}")

    report = f"""
=== MARKET DATA FOR {symbol} ===

--- 1. RECENT PRICES (last 18 days from md.eod_bars, source: Casablanca Stock Exchange) ---
{stock_data}

--- 2. HISTORICAL TREND (weekly from md.historical_prices, source: Investing.com CSVs) ---
{hist_data}

--- 3. MASI INDEX (from md.market_index, source: Investing.com) ---
{masi_data}

--- 4. ORDER BOOK (from md.order_books, source: Medias24) ---
{ob_data}

--- 5. NEWS ARTICLES (from md.news_articles, source: BourseNews + Medias24) ---
{news_data}

--- 6. TECHNICAL INDICATORS (computed from md.eod_bars) ---
{tech_data}

=== END OF DATA ===
"""
    return report


def create_agent():
    """Create the Whale Analyst agent with ONLY save_prediction tool.
    The LLM's job is simple: analyze pre-fetched data → call save_prediction."""
    return Agent(
        name="Whale Analyst",
        model=OpenAIChat(
            id="qwen2.5:7b",
            base_url="http://localhost:11434/v1",
            api_key="ollama",
        ),
        tools=[save_prediction],  # Only 1 tool — much easier for 7B model
        description=(
            "You are Whale Analyst, a ruthless, highly-skilled quantitative financial analyst for Bourse de Casablanca. "
            "Your clients are high-net-worth investors who demand actionable, data-backed insights, not generic summaries. "
            "You receive pre-fetched market data and must produce a high-conviction prediction. "
            "You MUST call the save_prediction tool with your analysis."
        ),
        instructions=[
            "You will receive market data that has already been fetched for you.",
            "Analyze ALL the data: prices, historical trends, MASI index, orderbook, news, technicals.",
            "Determine: trend, predicted_action, confidence_pct.",
            "You MUST call save_prediction with your structured analysis. This is MANDATORY.",
            "CRITICAL - REASONING RULES for the 'reasoning' parameter:",
            "1. NO FILLER: Write like a senior quant. Do not pad with generic statements.",
            "2. CLEAR THESIS: State exactly why the setup is a BUY/SELL/HOLD.",
            "3. METRICS: You MUST cite the RSI, MACD, and critical price levels.",
            "4. NO NULLS: DO NOT pass 'null' for any numeric field (e.g. support_price, resistance_price). Use 0.0 if unknown.",
            "5. NO EXTRA ARGS: ONLY pass the exact arguments defined in save_prediction. DO NOT pass rsi_signal, sma_5_vs_sma_10, or any other hallucinated metric.",
            "For trend/masi_trend: use BULLISH, BEARISH, or NEUTRAL.",
            "For predicted_action: use BUY, SELL, or HOLD.",
            "For news_sentiment: use POSITIVE, NEGATIVE, or NEUTRAL.",
            "For orderbook_bias: use BUY_PRESSURE, SELL_PRESSURE, or BALANCED.",
            "For strength: use a number from 1 to 10.",
            "For confidence_pct: use a number from 0 to 100.",
        ],
        markdown=True,
    )


def analyze_stock(agent: Agent, symbol: str):
    """Run analysis on a single stock."""
    logger.info(f"{'='*60}")
    logger.info(f"STARTING ANALYSIS: {symbol}")
    logger.info(f"{'='*60}")

    # Step 1: Pre-fetch all data in Python (reliable, no LLM needed)
    logger.info(f"[{symbol}] Step 1: Pre-fetching all market data...")
    data_report = prefetch_data(symbol)
    logger.info(f"[{symbol}] Data report length: {len(data_report)} chars")

    # Step 2: Give data to LLM and ask for analysis + save_prediction call
    logger.info(f"[{symbol}] Step 2: Sending data to qwen2.5:7b for analysis...")
    prompt = (
        f"Here is all the market data for **{symbol}** on the Bourse de Casablanca:\n\n"
        f"{data_report}\n\n"
        f"Analyze this data and call save_prediction with your structured prediction for {symbol}. "
        f"You MUST call save_prediction — do not just describe the data."
    )
    response = agent.run(prompt)
    # Log the full LLM response
    if response and response.content:
        # Check for error messages returned by the agent
        if "Connection error" in response.content or "Error calling model" in response.content:
            logger.error(f"[{symbol}] ❌ LLM Analysis Failed: {response.content}")
            print(f"   ❌ LLM Analysis Failed: {response.content}")
            return
        
        logger.info(f"[{symbol}] LLM Response: {response.content[:500]}...")
    else:
        logger.warning(f"[{symbol}] ⚠️ LLM returned no content")

    # Log tool calls
    if response and response.messages:
        for msg in response.messages:
            if hasattr(msg, 'tool_calls') and msg.tool_calls:
                for tc in msg.tool_calls:
                    logger.info(f"[{symbol}] Tool call: {tc}")

    # Also print nicely to console
    if response and response.content and "Connection error" not in response.content:
        print(f"\n{response.content}")

    logger.info(f"[{symbol}] Analysis complete")


def run_scrapers():
    """Run all scrapers sequentially to get fresh data before analysis."""
    scrapers = [
        "scrapers/scrap_level1.py",
        "scrapers/scrap_level2.py", 
        "scrapers/masi_scraper.py",
        "scrapers/boursenews_scraper.py",
        "scrapers/medias24_news_scraper.py"
    ]
    
    logger.info(f"{'='*60}")
    logger.info(f"🚀 STARTING SCRAPERS ({len(scrapers)} scripts)")
    logger.info(f"{'='*60}")
    print(f"\n🚀 Running {len(scrapers)} scrapers to get fresh market data...")
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    for scraper in scrapers:
        script_path = os.path.join(project_root, scraper.replace("/", os.sep))
        logger.info(f"Running scraper: {scraper}")
        print(f"   ▶️ Running {scraper}...")
        
        try:
            # Run the scraper and capture output
            result = subprocess.run(
                [sys.executable, script_path], 
                cwd=project_root,
                capture_output=True, 
                text=True, 
                check=True
            )
            logger.info(f"Success: {scraper}\nOutput: {result.stdout[:500]}...")
        except subprocess.CalledProcessError as e:
            logger.error(f"Scraper failed: {scraper}\nError: {e.stderr}", exc_info=True)
            print(f"   ❌ Error running {scraper}. See logs for details.")
            continue
            
    logger.info(f"✅ All scrapers finished.")
    print(f"✅ Fresh data scraped successfully!\n")

def main():
    parser = argparse.ArgumentParser(description="AI Agent 1 — Whale Analyst")
    parser.add_argument("--symbol", type=str, default=None, help="Analyze a single stock (e.g. ATW)")
    parser.add_argument("--scrape", action="store_true", help="Run all scrapers to get fresh data before analyzing")
    args = parser.parse_args()

    if args.scrape:
        run_scrapers()

    # Check Ollama status before creating agent
    if not check_ollama_status():
        print("\n❌ CRITICAL ERROR: Ollama is not running or not accessible.")
        print("   Please start Ollama (run 'ollama serve') and try again.")
        sys.exit(1)

    agent = create_agent()

    if args.symbol:
        analyze_stock(agent, args.symbol.upper())
    else:
        symbols = get_all_symbols()
        logger.info(f"Whale Analyst starting — analyzing {len(symbols)} stocks")
        print(f"🐳 Whale Analyst starting — analyzing {len(symbols)} stocks")
        print(f"   Stocks: {', '.join(symbols)}")
        print(f"   Model: qwen2.5:7b (Ollama)")
        print(f"\n   Data Sources:")
        print(f"     • md.eod_bars        ← scrap_level1.py (CSE live market)")
        print(f"     • md.historical_prices← ingest_historical.py (Investing.com CSVs)")
        print(f"     • md.market_index    ← masi_scraper.py (Investing.com)")
        print(f"     • md.order_books     ← scrap_level2.py (Medias24)")
        print(f"     • md.news_articles   ← boursenews + medias24 news scrapers")
        print(f"     • Technical (computed)← SMA, RSI, MACD from eod_bars")
        print(f"     • md.predictions     ← OUTPUT written by this agent")
        print()

        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}]", end="")
            try:
                analyze_stock(agent, symbol)
            except Exception as e:
                logger.error(f"Error analyzing {symbol}: {e}", exc_info=True)
                print(f"❌ Error analyzing {symbol}: {e}")
                continue

        print(f"\n{'='*60}")
        print(f"🎉 Analysis complete! Check md.predictions table for results.")
        print(f"{'='*60}")


if __name__ == "__main__":
    main()
