"""Performance metrics for backtest results."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


def _clean_equity(equity: pd.Series) -> pd.Series:
    """Return a cleaned equity series with NaNs removed."""
    if not isinstance(equity, pd.Series):
        raise TypeError("equity must be a pandas Series")
    return equity.dropna().astype(float)


def _clean_returns(returns: pd.Series) -> pd.Series:
    """Return a cleaned return series with NaNs removed."""
    if not isinstance(returns, pd.Series):
        raise TypeError("returns must be a pandas Series")
    return returns.dropna().astype(float)


def total_return(equity: pd.Series) -> float:
    """Return total strategy return over the full sample."""
    clean = _clean_equity(equity)
    if len(clean) < 2:
        return 0.0
    return float(clean.iloc[-1] / clean.iloc[0] - 1.0)


def cagr(equity: pd.Series, bars_per_year: int) -> float:
    """Return compound annual growth rate using bars_per_year as annualization basis."""
    if bars_per_year <= 0:
        raise ValueError("bars_per_year must be positive")

    clean = _clean_equity(equity)
    periods = len(clean) - 1
    if periods <= 0:
        return 0.0

    years = periods / bars_per_year
    if years <= 0:
        return 0.0

    return float((clean.iloc[-1] / clean.iloc[0]) ** (1.0 / years) - 1.0)


def annualized_volatility(returns: pd.Series, bars_per_year: int) -> float:
    """Return annualized volatility from periodic strategy returns."""
    if bars_per_year <= 0:
        raise ValueError("bars_per_year must be positive")

    clean = _clean_returns(returns)
    if clean.empty:
        return 0.0

    std = clean.std(ddof=0)
    return float(std * np.sqrt(bars_per_year))


def max_drawdown(equity: pd.Series) -> float:
    """Return maximum drawdown for an equity curve."""
    clean = _clean_equity(equity)
    if clean.empty:
        return 0.0

    running_max = clean.cummax()
    drawdown = clean / running_max - 1.0
    return float(drawdown.min())


def sharpe_ratio(returns: pd.Series, bars_per_year: int) -> float:
    """Return annualized Sharpe ratio with zero risk-free rate assumption."""
    if bars_per_year <= 0:
        raise ValueError("bars_per_year must be positive")

    clean = _clean_returns(returns)
    if clean.empty:
        return 0.0

    std = clean.std(ddof=0)
    if math.isclose(float(std), 0.0):
        return 0.0

    return float((clean.mean() / std) * np.sqrt(bars_per_year))


def turnover_summary_stats(turnover: pd.Series) -> dict[str, float]:
    """Return simple turnover summary statistics from a turnover series."""
    clean = _clean_returns(turnover)
    if clean.empty:
        return {
            "avg_turnover": 0.0,
            "median_turnover": 0.0,
            "max_turnover": 0.0,
            "total_turnover": 0.0,
        }

    return {
        "avg_turnover": float(clean.mean()),
        "median_turnover": float(clean.median()),
        "max_turnover": float(clean.max()),
        "total_turnover": float(clean.sum()),
    }


def summary_metrics(
    equity: pd.Series,
    bars_per_year: int,
    returns: pd.Series | None = None,
    turnover: pd.Series | None = None,
    gross_returns: pd.Series | None = None,
    holdings_history: pd.DataFrame | None = None,
    rebalance_log: pd.DataFrame | None = None,
) -> dict[str, float]:
    """Return a dictionary of core strategy metrics from equity and optional turnover."""
    clean_equity = _clean_equity(equity)
    if len(clean_equity) < 2:
        raise ValueError("equity must contain at least two non-null observations")

    if returns is None:
        inferred_returns = clean_equity.pct_change().fillna(0.0)
    else:
        inferred_returns = _clean_returns(returns)

    metrics: dict[str, Any] = {
        "total_return": total_return(clean_equity),
        "cagr": cagr(clean_equity, bars_per_year=bars_per_year),
        "annualized_volatility": annualized_volatility(
            inferred_returns, bars_per_year=bars_per_year
        ),
        "sharpe": sharpe_ratio(inferred_returns, bars_per_year=bars_per_year),
        "max_drawdown": max_drawdown(clean_equity),
    }

    if turnover is not None:
        metrics.update(turnover_summary_stats(turnover))

    if gross_returns is not None:
        gross = _clean_returns(gross_returns)
        if len(gross) == len(clean_equity):
            gross_equity = clean_equity.iloc[0] * (1.0 + gross).cumprod()
            gross_total = total_return(gross_equity)
            metrics["gross_total_return"] = float(gross_total)
            metrics["net_total_return"] = float(metrics["total_return"])
            metrics["cost_drag_total_return"] = float(gross_total - metrics["total_return"])

    if holdings_history is not None and not holdings_history.empty:
        invested_mask = holdings_history.sum(axis=1) > 1e-12
        holdings_count = (holdings_history > 1e-12).sum(axis=1)
        metrics["pct_time_invested"] = float(invested_mask.mean())
        metrics["avg_holdings_count"] = float(holdings_count.mean())

    if rebalance_log is not None:
        metrics["rebalance_count"] = float(len(rebalance_log))

    return {k: float(v) for k, v in metrics.items()}


def summarize(result: pd.DataFrame, bars_per_year: int) -> dict[str, float]:
    """Backward-compatible summary from a backtest result DataFrame."""
    if "equity" not in result.columns:
        raise ValueError("result must include an 'equity' column")

    equity = result["equity"]
    returns = result["strategy_return"] if "strategy_return" in result.columns else None
    turnover = result["turnover"] if "turnover" in result.columns else None

    return {
        **summary_metrics(
            equity=equity,
            bars_per_year=bars_per_year,
            returns=returns,
            turnover=turnover,
        )
    }
