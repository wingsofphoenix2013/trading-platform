# smi_live.py
# Расчёт индикатора SMI в live-режиме на основе DataFrame с виртуальной свечой

import pandas as pd
import numpy as np

# 1. Расчёт SMI
# Вход: df (DataFrame с колонками high, low, close), параметры k/d/s
# Выход: словарь со значениями smi и smi_signal
async def calculate_smi(df: pd.DataFrame, k: int, d: int, s: int) -> dict:
    try:
        high = df['high']
        low = df['low']
        close = df['close']

        # Расчёт midpoints
        min_low = low.rolling(k).min()
        max_high = high.rolling(k).max()
        mid = (min_low + max_high) / 2
        rel_close = close - mid
        range_ = max_high - min_low

        # EMA через сглаженное RMA (Wilder's EMA)
        def rma(series, period):
            return series.ewm(alpha=1/period, adjust=False).mean()

        rel_ema = rma(rel_close, d)
        range_ema = rma(range_, d)

        smi = 100 * (rel_ema / (range_ema / 2))
        smi_signal = rma(smi, s)

        # Последние значения
        return {
            "smi": round(smi.iloc[-1], 2),
            "smi_signal": round(smi_signal.iloc[-1], 2)
        }

    except Exception as e:
        print(f"[ERROR] SMI расчёт не удался: {e}", flush=True)
        return {"smi": None, "smi_signal": None}
