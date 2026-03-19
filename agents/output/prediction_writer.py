"""
Prediction writer tool for AI Agent 1.
Writes structured predictions to md.predictions table.
"""
import json
from datetime import date

from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres:123456@localhost:5432/PFE"
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)


def _sanitize_enum(value: str, valid_values: list[str], default: str) -> str:
    """Extract a valid enum value from LLM output that may contain extra text."""
    if not value:
        return default
    v = str(value).upper().strip()
    # Direct match
    if v in valid_values:
        return v
    # Check if any valid value is contained in the string
    for valid in valid_values:
        if valid in v:
            return valid
    return default


def _sanitize_float(value, default: float = 0.0) -> float:
    """Safely convert to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _sanitize_int(value, min_val: int = 1, max_val: int = 10, default: int = 5) -> int:
    """Safely convert to int within range."""
    if value is None:
        return default
    try:
        v = int(float(value))
        return max(min_val, min(max_val, v))
    except (ValueError, TypeError):
        return default


def save_prediction(
    symbol: str,
    predicted_action: str,
    confidence_pct: float,
    reasoning: str,
    trend: str = "NEUTRAL",
    strength: int = 5,
    support_price: float = 0.0,
    resistance_price: float = 0.0,
    predicted_move: float = 0.0,
    news_sentiment: str = "NEUTRAL",
    masi_trend: str = "NEUTRAL",
    orderbook_bias: str = "BALANCED",
) -> str:
    """Save the structured prediction to md.predictions table.
    You MUST call this after analyzing all data for a stock.

    Args:
        symbol: Stock symbol (e.g. 'ATW', 'AKDITAL')
        trend: 'BULLISH', 'BEARISH', or 'NEUTRAL'
        strength: 1-10 signal strength
        support_price: Nearest support level
        resistance_price: Nearest resistance level
        predicted_action: 'BUY', 'SELL', or 'HOLD'
        confidence_pct: 0-100 confidence percentage
        predicted_move: Expected % move in next session
        reasoning: Your analysis explanation
        news_sentiment: 'POSITIVE', 'NEGATIVE', or 'NEUTRAL'
        masi_trend: 'BULLISH', 'BEARISH', or 'NEUTRAL'
        orderbook_bias: 'BUY_PRESSURE', 'SELL_PRESSURE', or 'BALANCED'
    """
    # Sanitize LLM outputs to valid values
    trend = _sanitize_enum(trend, ["BULLISH", "BEARISH", "NEUTRAL"], "NEUTRAL")
    predicted_action = _sanitize_enum(predicted_action, ["BUY", "SELL", "HOLD"], "HOLD")
    news_sentiment = _sanitize_enum(news_sentiment, ["POSITIVE", "NEGATIVE", "NEUTRAL"], "NEUTRAL")
    masi_trend = _sanitize_enum(masi_trend, ["BULLISH", "BEARISH", "NEUTRAL"], "NEUTRAL")
    orderbook_bias = _sanitize_enum(orderbook_bias, ["BUY_PRESSURE", "SELL_PRESSURE", "BALANCED"], "BALANCED")
    strength = _sanitize_int(strength, 1, 10, 5)
    confidence_pct = min(100.0, max(0.0, _sanitize_float(confidence_pct, 50.0)))
    support_price = _sanitize_float(support_price)
    resistance_price = _sanitize_float(resistance_price)
    predicted_move = _sanitize_float(predicted_move)
    reasoning = str(reasoning)[:2000] if reasoning else "No reasoning provided"

    # Get instrument_id for the symbol
    with engine.begin() as conn:
        instrument_id = conn.execute(
            text("SELECT instrument_id FROM ref.instruments WHERE symbol = :sym"),
            {"sym": symbol},
        ).scalar_one_or_none()

        if instrument_id is None:
            # Try to handle suffixes if needed, e.g. "ADDOHA-P" -> "ADDOHA" or vice versa
            # For now, just raise an error or return failure
            return f"❌ Error: Symbol '{symbol}' not found in ref.instruments table."

        conn.execute(
            text("""
                INSERT INTO md.predictions (
                    instrument_id, symbol, analysis_date,
                    trend, strength, support_price, resistance_price,
                    predicted_action, confidence_pct, predicted_move, reasoning,
                    news_sentiment, masi_trend, orderbook_bias,
                    model_name
                )
                VALUES (
                    :instrument_id, :symbol, :analysis_date,
                    :trend, :strength, :support_price, :resistance_price,
                    :predicted_action, :confidence_pct, :predicted_move, :reasoning,
                    :news_sentiment, :masi_trend, :orderbook_bias,
                    'qwen2.5:7b'
                )
                ON CONFLICT (symbol, analysis_date) DO UPDATE SET
                    trend = EXCLUDED.trend,
                    strength = EXCLUDED.strength,
                    support_price = EXCLUDED.support_price,
                    resistance_price = EXCLUDED.resistance_price,
                    predicted_action = EXCLUDED.predicted_action,
                    confidence_pct = EXCLUDED.confidence_pct,
                    predicted_move = EXCLUDED.predicted_move,
                    reasoning = EXCLUDED.reasoning,
                    news_sentiment = EXCLUDED.news_sentiment,
                    masi_trend = EXCLUDED.masi_trend,
                    orderbook_bias = EXCLUDED.orderbook_bias,
                    model_name = EXCLUDED.model_name,
                    created_at = CURRENT_TIMESTAMP
            """),
            {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "analysis_date": date.today(),
                "trend": trend,
                "strength": strength,
                "support_price": support_price,
                "resistance_price": resistance_price,
                "predicted_action": predicted_action,
                "confidence_pct": confidence_pct,
                "predicted_move": predicted_move,
                "reasoning": reasoning,
                "news_sentiment": news_sentiment,
                "masi_trend": masi_trend,
                "orderbook_bias": orderbook_bias,
            },
        )

    return f"✅ Prediction saved for {symbol}: {predicted_action} (confidence: {confidence_pct}%)"
