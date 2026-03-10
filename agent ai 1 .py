from agno.agent import Agent
from agno.models.openai import OpenAIChat
from sqlalchemy import create_engine, text
import pandas as pd

# 1. Connexion l-DB (PFE Project)
engine = create_engine("postgresql://postgres:123456@localhost:5432/PFE")

# TOOL 1: Company Stats
def get_company_stats(symbol: str, **kwargs):
    """Fetch real-time stock data (Price, Volume, Variation) for a company."""
    print(f"\n--- 🚀 AGENT RUNNING PYTHON: get_company_stats({symbol}) ---")
    query = text("""
        SELECT valeur_name, cours_close, variation_pct, volume_mad 
        FROM md.eod_bars WHERE valeur_name ILIKE :nm ORDER BY trade_date DESC LIMIT 1
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"nm": f"%{symbol}%"})
        if df.empty: return f"Error: No data for {symbol}."
        return df.to_json(orient='records')

# TOOL 2: Market Trend
def get_market_trend(**kwargs):
    """Fetch the latest MASI index value and variation."""
    print("\n--- 🚀 AGENT RUNNING PYTHON: get_market_trend() ---")
    query = text("SELECT valeur, variation_pct FROM md.market_indices WHERE index_name = 'MASI' ORDER BY record_date DESC LIMIT 1")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        return df.to_json(orient='records')

# 3. L-Agent (The Strict Bridge)
whale_agent = Agent(
    model=OpenAIChat(
        id="qwen2.5-coder:7b",          # Use Qwen for better reasoning
        base_url="http://localhost:11434/v1", 
        api_key="ollama",              
    ),
    tools=[get_company_stats, get_market_trend],
    description="Nta houwa 'Whale AI', expert analyste f Bourse de Casablanca.",
    instructions=[
        "Rule 1: You MUST call 'get_market_trend' for EVERY request.",
        "Rule 2: You MUST call 'get_company_stats' for any mentioned stock.",
        "Rule 3: After tool execution, translate the numbers into Moroccan Darija.",
        "Rule 4: Final output MUST NOT be JSON. Only text in Darija/French Business.",
        "Rule 5: Use ONLY the numbers provided by the tools."
    ],
    markdown=True,
    # Hada houwa l-mifta7 bach Agno i-bqa i-runni l-loop
    tool_call_limit=5
)

# 4. Execution (STRICT)
print("🐳 Whale Agent (V17 - Strict) kheddam...")
whale_agent.print_response(
    "Chno hiya l-7ala dyal MASI l-youm? Ou TAQA MOROCCO wach tal3a m3ah?", 
    stream=False,
    show_tool_calls=True # Hna f print_response bach ma-t-dirch TypeError
)