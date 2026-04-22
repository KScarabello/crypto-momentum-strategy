"""Backtest engine for long-only crypto momentum rotation."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from strategy.momentum import check_regime_filter, compute_momentum_score, rank_symbols_for_date

LOGGER = logging.getLogger(__name__)

REQUIRED_OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume", "symbol"]


@dataclass(frozen=True)
class BacktestResult:
    """Container for momentum rotation backtest outputs."""

    portfolio: pd.DataFrame
    rebalance_log: pd.DataFrame
    holdings_history: pd.DataFrame
    turnover: pd.Series
    gross_return: pd.Series


def _validate_ohlcv(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Validate and standardize long-format OHLCV input."""
    if ohlcv.empty:
        raise ValueError("ohlcv input is empty")

    missing = [col for col in REQUIRED_OHLCV_COLUMNS if col not in ohlcv.columns]
    if missing:
        raise ValueError(f"ohlcv is missing required columns: {missing}")

    frame = ohlcv[REQUIRED_OHLCV_COLUMNS].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp", "symbol", "close"])
    frame = frame.sort_values(["symbol", "timestamp"]).drop_duplicates(
        subset=["symbol", "timestamp"], keep="last"
    )
    # Guard: non-positive close prices cause pct_change to generate inf returns.
    bad_close = frame["close"] <= 0
    if bad_close.any():
        n = int(bad_close.sum())
        LOGGER.warning("Dropping %d rows with non-positive close prices from engine input", n)
        frame = frame[~bad_close]
    return frame.reset_index(drop=True)


def _close_matrix(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Pivot long-format OHLCV to a timestamp x symbol close matrix."""
    close = ohlcv.pivot(index="timestamp", columns="symbol", values="close").sort_index().astype(float)
    close = close.dropna(how="all")
    if close.empty:
        raise ValueError("close matrix is empty after pivot")
    return close


def _build_target_weights(symbols: pd.Index, selected: list[str]) -> pd.Series:
    """Build equal-weight long-only target weights for selected symbols."""
    weights = pd.Series(0.0, index=symbols, dtype=float)
    if selected:
        weights.loc[selected] = 1.0 / len(selected)
    return weights


def _validate_weights(weights: pd.Series, context: str) -> None:
    """Validate long-only normalized portfolio weights."""
    if weights.isna().any():
        raise ValueError(f"NaN weight detected in {context}")
    if (weights < -1e-12).any():
        raise ValueError(f"Negative weight detected in {context}")

    total_weight = float(weights.sum())
    if total_weight > 1.0 + 1e-9:
        raise ValueError(
            f"Weight sum exceeds 1.0 in {context}: {total_weight:.6f}"
        )


def _apply_turnover_cap(
    current_weights: pd.Series,
    target_weights: pd.Series,
    max_turnover_per_rebalance: float | None,
) -> pd.Series:
    """Apply an optional turnover cap by scaling the trade vector."""
    if max_turnover_per_rebalance is None:
        return target_weights

    raw_turnover = float((target_weights - current_weights).abs().sum())
    if raw_turnover <= max_turnover_per_rebalance or raw_turnover <= 0:
        return target_weights

    scale = max_turnover_per_rebalance / raw_turnover
    capped = current_weights + (target_weights - current_weights) * scale
    capped = capped.clip(lower=0.0)
    if capped.sum() > 1.0:
        capped = capped / float(capped.sum())
    return capped


def _apply_position_weight_cap(
    target_weights: pd.Series,
    max_position_weight: float | None,
) -> pd.Series:
    """Cap individual asset weights and leave any remainder in cash."""
    if max_position_weight is None:
        return target_weights
    return target_weights.clip(lower=0.0, upper=max_position_weight)


def _apply_gross_exposure_cap(
    target_weights: pd.Series,
    max_gross_exposure: float | None,
) -> pd.Series:
    """Scale the full portfolio down to a maximum gross exposure."""
    if max_gross_exposure is None:
        return target_weights

    total_weight = float(target_weights.sum())
    if total_weight <= 0 or total_weight <= max_gross_exposure:
        return target_weights

    scale = max_gross_exposure / total_weight
    return target_weights * scale


def run_momentum_rotation_backtest(
    ohlcv: pd.DataFrame,
    top_n: int,
    rebalance_every_bars: int,
    short_lookback_bars: int = 7,
    medium_lookback_bars: int = 30,
    short_weight: float = 0.5,
    medium_weight: float = 0.5,
    btc_symbol: str = "BTC/USD",
    regime_ma_lookback_bars: int = 30,
    use_regime_filter: bool = True,
    max_position_weight: float | None = None,
    max_gross_exposure: float | None = None,
    transaction_cost_bps: float = 10.0,
    slippage_bps: float = 0.0,
    initial_capital: float = 10_000.0,
    min_history_bars: int | None = None,
    min_eligible_assets: int = 1,
    min_median_volume: float | None = None,
    max_turnover_per_rebalance: float | None = None,
) -> BacktestResult:
    """Backtest long-only momentum rotation with periodic rebalancing and BTC regime filter."""
    from config import SETTINGS

    if max_position_weight is None:
        max_position_weight = SETTINGS.max_position_weight
    if max_gross_exposure is None:
        max_gross_exposure = SETTINGS.max_gross_exposure

    if top_n <= 0:
        raise ValueError("top_n must be positive")
    if rebalance_every_bars <= 0:
        raise ValueError("rebalance_every_bars must be positive")
    if initial_capital <= 0:
        raise ValueError("initial_capital must be positive")
    if transaction_cost_bps < 0 or slippage_bps < 0:
        raise ValueError("cost and slippage bps must be non-negative")
    if min_history_bars is not None and min_history_bars <= 0:
        raise ValueError("min_history_bars must be positive when provided")
    if min_eligible_assets <= 0:
        raise ValueError("min_eligible_assets must be positive")
    if min_median_volume is not None and min_median_volume < 0:
        raise ValueError("min_median_volume must be non-negative when provided")
    if max_turnover_per_rebalance is not None and max_turnover_per_rebalance <= 0:
        raise ValueError("max_turnover_per_rebalance must be positive when provided")
    if max_position_weight is not None and (max_position_weight <= 0 or max_position_weight > 1.0):
        raise ValueError("max_position_weight must be within (0, 1] when provided")
    if max_gross_exposure is not None and (max_gross_exposure <= 0 or max_gross_exposure > 1.0):
        raise ValueError("max_gross_exposure must be within (0, 1] when provided")

    if min_history_bars is None:
        min_history_bars = max(short_lookback_bars, medium_lookback_bars)

    clean = _validate_ohlcv(ohlcv)
    close = _close_matrix(clean)
    volume = clean.pivot(index="timestamp", columns="symbol", values="volume").sort_index().astype(float)
    volume = volume.reindex(close.index)
    returns = close.pct_change().fillna(0.0)

    # Safety net: non-positive close prices can still produce inf after pivot
    # when neighbouring timestamps align a zero with a real price across symbols.
    n_inf = int(np.isinf(returns.values).sum())
    if n_inf > 0:
        LOGGER.warning(
            "Replacing %d inf/-inf return values with 0.0 (check input prices)", n_inf
        )
        returns = returns.replace([np.inf, -np.inf], 0.0)

    momentum_score = compute_momentum_score(
        close=close,
        short_lookback_bars=short_lookback_bars,
        medium_lookback_bars=medium_lookback_bars,
        short_weight=short_weight,
        medium_weight=medium_weight,
    )

    start_bar = max(
        short_lookback_bars,
        medium_lookback_bars,
        regime_ma_lookback_bars if use_regime_filter else 0,
    )
    if start_bar >= len(close.index) - 1:
        raise ValueError("Not enough rows for configured lookbacks")

    index = close.index
    symbols = close.columns

    holdings_history = pd.DataFrame(0.0, index=index, columns=symbols)
    turnover = pd.Series(0.0, index=index, dtype=float)
    gross_return = pd.Series(0.0, index=index, dtype=float)
    strategy_return = pd.Series(0.0, index=index, dtype=float)
    equity = pd.Series(float("nan"), index=index, dtype=float)

    rebalance_records: list[dict[str, object]] = []

    equity.iloc[start_bar] = initial_capital
    current_weights = pd.Series(0.0, index=symbols, dtype=float)
    pending_target: pd.Series | None = None
    pending_signal_timestamp: pd.Timestamp | None = None
    pending_selected: list[str] = []
    pending_risk_on = True
    pending_eligible_count = 0

    total_bps = transaction_cost_bps + slippage_bps
    valid_price = close.notna()
    history_count = valid_price.astype(int).cumsum()
    rolling_median_volume = (
        volume.rolling(window=min_history_bars, min_periods=min_history_bars).median()
        if min_median_volume is not None
        else None
    )

    for i in range(start_bar, len(index) - 1):
        ts = index[i]
        is_rebalance = (i - start_bar) % rebalance_every_bars == 0
        cost_rate = 0.0

        # Execute prior signal with one-bar delay to avoid same-bar execution optimism.
        if pending_target is not None:
            exec_turnover = float((pending_target - current_weights).abs().sum())
            cost_rate = (exec_turnover * total_bps) / 10_000.0
            turnover.iloc[i] = exec_turnover
            current_weights = pending_target
            _validate_weights(current_weights, context=f"execution at {ts}")

            rebalance_records.append(
                {
                    "signal_timestamp": pending_signal_timestamp,
                    "execution_timestamp": ts,
                    "risk_on": pending_risk_on,
                    "selected_symbols": "|".join(pending_selected),
                    "num_selected": len(pending_selected),
                    "eligible_count": pending_eligible_count,
                    "turnover": exec_turnover,
                    "cost_rate": cost_rate,
                    "weight_sum": float(current_weights.sum()),
                }
            )

            pending_target = None
            pending_signal_timestamp = None
            pending_selected = []
            pending_risk_on = True
            pending_eligible_count = 0

        if is_rebalance:
            # The BTC filter is optional because robustness tests showed it can
            # be a useful research overlay but a weak default risk-control rule.
            risk_on = True
            if use_regime_filter:
                risk_on = check_regime_filter(
                    close=close,
                    rebalance_timestamp=ts,
                    btc_symbol=btc_symbol,
                    ma_lookback_bars=regime_ma_lookback_bars,
                )

            target_weights = pd.Series(0.0, index=symbols, dtype=float)
            selected: list[str] = []

            eligible_mask = valid_price.loc[ts] & (history_count.loc[ts] >= min_history_bars)
            if rolling_median_volume is not None:
                eligible_mask = eligible_mask & (rolling_median_volume.loc[ts] >= min_median_volume)
            eligible_symbols = symbols[eligible_mask.values]

            if risk_on and len(eligible_symbols) >= min_eligible_assets:
                ranked = rank_symbols_for_date(
                    momentum_score=momentum_score,
                    rebalance_timestamp=ts,
                    top_n=len(symbols),
                )
                selected = [sym for sym in ranked if sym in set(eligible_symbols)][:top_n]
                target_weights = _build_target_weights(symbols, selected)

                # This cap is a simple cash overlay only; it does not alter
                # ranking or selection, only the maximum exposure per asset.
                target_weights = _apply_position_weight_cap(
                    target_weights=target_weights,
                    max_position_weight=max_position_weight,
                )
                target_weights = _apply_gross_exposure_cap(
                    target_weights=target_weights,
                    max_gross_exposure=max_gross_exposure,
                )

            target_weights = _apply_turnover_cap(
                current_weights=current_weights,
                target_weights=target_weights,
                max_turnover_per_rebalance=max_turnover_per_rebalance,
            )
            _validate_weights(target_weights, context=f"target build at {ts}")

            pending_target = target_weights
            pending_signal_timestamp = ts
            pending_selected = selected
            pending_risk_on = risk_on
            pending_eligible_count = int(len(eligible_symbols))

        next_bar_return = float((current_weights * returns.iloc[i + 1]).sum())
        gross_return.iloc[i + 1] = next_bar_return
        net_return = next_bar_return - cost_rate
        strategy_return.iloc[i + 1] = net_return

        current_equity = float(equity.iloc[i])
        new_equity = current_equity * (1.0 + net_return)
        if not np.isfinite(new_equity):
            LOGGER.error(
                "Equity became non-finite at bar %d (ts=%s): "
                "net_return=%.6f, current_equity=%.4f — clamping to previous value",
                i + 1,
                index[i + 1],
                net_return,
                current_equity,
            )
            new_equity = current_equity
        equity.iloc[i + 1] = new_equity
        holdings_history.iloc[i] = current_weights

    holdings_history.iloc[-1] = current_weights

    portfolio = pd.DataFrame(
        {
            "strategy_return": strategy_return,
            "equity": equity,
        },
        index=index,
    ).dropna(subset=["equity"])

    turnover = turnover.loc[portfolio.index]
    holdings_history = holdings_history.loc[portfolio.index]
    rebalance_log = pd.DataFrame(rebalance_records)

    LOGGER.info(
        "Backtest finished: bars=%d, rebalances=%d, final_equity=%.2f",
        len(portfolio),
        len(rebalance_log),
        float(portfolio["equity"].iloc[-1]),
    )

    return BacktestResult(
        portfolio=portfolio,
        rebalance_log=rebalance_log,
        holdings_history=holdings_history,
        turnover=turnover,
        gross_return=gross_return.loc[portfolio.index],
    )


def run_momentum_rotation(
    close: pd.DataFrame,
    lookback_bars: int,
    top_n: int,
    rebalance_every_bars: int,
    fee_bps: float,
    initial_capital: float,
) -> pd.DataFrame:
    """Backward-compatible wrapper for close-matrix based momentum backtests."""
    long_ohlcv = close.stack(dropna=False).rename("close").reset_index()
    long_ohlcv.columns = ["timestamp", "symbol", "close"]
    long_ohlcv["open"] = long_ohlcv["close"]
    long_ohlcv["high"] = long_ohlcv["close"]
    long_ohlcv["low"] = long_ohlcv["close"]
    long_ohlcv["volume"] = 0.0
    long_ohlcv = long_ohlcv[["timestamp", "open", "high", "low", "close", "volume", "symbol"]]

    bt = run_momentum_rotation_backtest(
        ohlcv=long_ohlcv,
        top_n=top_n,
        rebalance_every_bars=rebalance_every_bars,
        short_lookback_bars=lookback_bars,
        medium_lookback_bars=lookback_bars,
        short_weight=0.5,
        medium_weight=0.5,
        btc_symbol=str(close.columns[0]),
        transaction_cost_bps=fee_bps,
        slippage_bps=0.0,
        initial_capital=initial_capital,
    )

    result = bt.portfolio.copy()
    result["turnover"] = bt.turnover
    return result
