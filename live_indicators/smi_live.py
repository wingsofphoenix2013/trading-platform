# smi_live.py
# Расчёт индикатора SMI по проверенной формуле (идентично историческим расчётам)

import pandas as pd
import numpy as np

# 1. Вспомогательная функция: двойное EMA
def double_ema(series, period):
    ema1 = series.ewm(span=period, adjust=False).mean()
    ema2 = ema1.ewm(span=period, adjust=False).mean()
    return ema2

# 2. Расчёт SMI (тот же, что и в indicators_main.py)
async def calculate_smi(df: pd.DataFrame, k: int, d: int, s: int) -> dict:
    try:
        df[["high", "low", "close"]] = df[["high", "low", "close"]].astype(float)

        hh = df['high'].rolling(window=k).max()
        ll = df['low'].rolling(window=k).min()
        center = (hh + ll) / 2
        range_ = hh - ll
        rel = df['close'] - center

        smi_raw = 200 * (double_ema(rel, d) / double_ema(range_, d))
        smi_signal = smi_raw.ewm(span=s, adjust=False).mean()

        return {
            "smi": round(smi_raw.iloc[-1], 2),
            "smi_signal": round(smi_signal.iloc[-1], 2)
        }

    except Exception as e:
        print(f"[ERROR] SMI расчёт не удался: {e}", flush=True)
        return {"smi": None, "smi_signal": None}
