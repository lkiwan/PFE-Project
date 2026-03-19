import argparse
import sys
import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import timedelta

# Ensure we can import from agents module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.agent1_analyst import create_agent, prefetch_data
from agents.output.prediction_writer import engine

def get_test_dates(symbol: str, test_days: int) -> list[str]:
    """Fetch the last N trading dates for the stock to be used as our test set."""
    query = text("""
        SELECT e.trade_date
        FROM md.eod_bars e
        JOIN ref.instruments i ON e.instrument_id = i.instrument_id
        WHERE i.symbol = :sym
        ORDER BY e.trade_date DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"sym": symbol, "limit": test_days})
    
    # We want to traverse dates from oldest to newest within our test set
    dates = df["trade_date"].astype(str).tolist()
    dates.reverse()
    return dates

def get_actual_outcome(symbol: str, target_date: str) -> float:
    """Gets the actual % change on the NEXT available trading day after target_date."""
    query = text("""
        SELECT e.change_pct
        FROM md.eod_bars e
        JOIN ref.instruments i ON e.instrument_id = i.instrument_id
        WHERE i.symbol = :sym AND e.trade_date > :t_date
        ORDER BY e.trade_date ASC
        LIMIT 1
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"sym": symbol, "t_date": target_date}).fetchone()
        if result:
            return float(result[0])
    return None

def backtest(symbol: str, test_days: int):
    print(f"\n{'='*60}")
    print(f"🚀 STARTING LLM BACKTEST FOR {symbol}")
    print(f"📅 Testing the last {test_days} trading days")
    print(f"{'='*60}\n")

    dates = get_test_dates(symbol, test_days)
    if not dates:
        print(f"❌ Error: No dates found in md.eod_bars for {symbol}")
        return

    agent = create_agent()
    results = []

    for i, t_date in enumerate(dates, 1):
        print(f"[{i}/{len(dates)}] Simulating Date: {t_date}")
        
        # 1. Fetch data hiding everything after t_date
        data_report = prefetch_data(symbol, max_date=t_date)
        
        # 2. Ask LLM for prediction
        prompt = (
            f"Here is all historical market data for **{symbol}** up to **{t_date}**:\n\n"
            f"{data_report}\n\n"
            f"Analyze this data and call save_prediction with your structured prediction for {symbol}. "
            f"You MUST call save_prediction — do not just describe the data."
        )
        
        # Run agent
        response = agent.run(prompt)
        
        # Ensure we extract exactly what the agent predicted via the tool call
        predicted_action = "HOLD"
        if response and response.messages:
            for msg in response.messages:
                if hasattr(msg, 'tool_calls') and msg.tool_calls:
                    for tc in msg.tool_calls:
                        if tc.get('name') == 'save_prediction':
                            try:
                                args = eval(tc.get('arguments', '{}'))
                                predicted_action = args.get('predicted_action', 'HOLD')
                            except Exception:
                                pass
        
        # 3. Lookup Actual Market Outcome for T + 1
        actual_change = get_actual_outcome(symbol, t_date)
        
        if actual_change is None:
            print(f"   ⚠️ No future data available to evaluate. Skipping...")
            continue
            
        actual_direction = "BUY" if actual_change > 0 else "SELL" if actual_change < 0 else "HOLD"
        
        # 4. Evaluate Win/Loss Status
        success = False
        if predicted_action == "BUY" and actual_change > 0:
            success = True
        elif predicted_action == "SELL" and actual_change < 0:
            success = True
        elif predicted_action == "HOLD":
            # Just ignore holds for accuracy math, or treat neutral as success if change is tiny
            success = abs(actual_change) < 0.5 
            
        print(f"   🤖 Agent said: {predicted_action:<4} | 📈 Actual Change: {actual_change:>+5.2f}% | {'✅ WIN' if success else '❌ LOSS'}\n")
        
        results.append({
            "date": t_date,
            "prediction": predicted_action,
            "actual_change": actual_change,
            "win": success
        })

    # Print Final Summary Report
    buys = [r for r in results if r["prediction"] == "BUY"]
    sells = [r for r in results if r["prediction"] == "SELL"]
    evaluated = buys + sells  # We ignore HOLDS in binary accuracy
    
    wins = sum(1 for r in evaluated if r["win"])
    total_trades = len(evaluated)
    
    acc = (wins / total_trades * 100) if total_trades > 0 else 0
    total_return = sum(r["actual_change"] for r in buys) - sum(r["actual_change"] for r in sells)
    
    print(f"{'='*60}")
    print(f"📊 BACKTEST RESULTS FOR {symbol}")
    print(f"{'='*60}")
    print(f"Total Days Tested     : {len(results)}")
    print(f"Actionable Trades     : {total_trades} (Buys: {len(buys)}, Sells: {len(sells)})")
    print(f"Winning Trades        : {wins}")
    print(f"Accuracy Rate         : {acc:.2f}%")
    print(f"Theoretical Return    : {total_return:+.2f}%")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LLM Agent Backtester")
    parser.add_argument("--symbol", type=str, required=True, help="Stock symbol to backtest (e.g. ATW)")
    parser.add_argument("--days", type=int, default=10, help="Number of historical days to simulate (default 10)")
    args = parser.parse_args()
    
    backtest(args.symbol, args.days)
