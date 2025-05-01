# strategy_1.py

import logging

# 🔹 Заглушка стратегии — загружается, но пока не выполняет логику
async def on_signal(data: dict, interface):
    logging.info(f"🔧 strategy_1.on_signal() вызван для {data['symbol']} ({data['direction']})")