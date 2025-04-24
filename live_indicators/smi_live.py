# smi_live.py
# Расчёт индикатора SMI по формуле TradingView (Pine Script совместимость)

import pandas as pd
import numpy as np

# 1. Расчёт SMI (TV-совместимый)
# Вход: df (DataFrame с колонками high, low, close), параметры k/d/s
# Выход: словарь со значениями smi и smi_signal
async def calculate_smi(df: pd.DataFrame, k: int, d: int, s: int) -> dict:
    try:
        df[["high", "low", "close"]] = df[["high", "low", "close"]].astype(float)

        high = df['high']
        low = df['low']
        close = df['close']

        # Формула по аналогии с Pine Script
        hl = high - low
        hl_avg = hl.rolling(window=k).mean()
        mid = (high + low).rolling(window=k).mean() / 2
        close_diff = close - mid

        # EMA-сглаживания (как в TV)
        rel_ema = close_diff.ewm(span=d, adjust=False).mean()
        range_ema = hl_avg.ewm(span=d, adjust=False).mean()

        smi = 100 * (rel_ema / (range_ema / 2))
        smi_signal = smi.ewm(span=s, adjust=False).mean()

        return {
            "smi": round(smi.iloc[-1], 2),
            "smi_signal": round(smi_signal.iloc[-1], 2)
        }

    except Exception as e:
        print(f"[ERROR] SMI расчёт не удался: {e}", flush=True)
        return {"smi": None, "smi_signal": None}
