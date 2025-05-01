import os
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime
import redis.asyncio as redis

# 🔸 Инициализация FastAPI приложения
app = FastAPI()

# 🔸 Настройка базового логирования
logging.basicConfig(level=logging.INFO)

# 🔸 Подключение к Redis через переменные окружения
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
    ssl=True
)

# 🔸 Приём сигналов от TradingView (формат JSON)
# Ожидается: message, symbol, time (бар), sent_at (время отправки)
@app.post("/webhook_v2")
async def webhook_v2(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # 🔹 Извлечение ключевых полей сигнала
    message = payload.get("message")
    symbol = payload.get("symbol")
    bar_time = payload.get("time")
    sent_at = payload.get("sent_at")

    if not message or not symbol:
        raise HTTPException(status_code=422, detail="Missing 'message' or 'symbol'")

    # 🔹 Текущее UTC-время приёма сигнала
    received_at = datetime.utcnow().isoformat()

    # 🔹 Логирование сигнала
    logging.info(f"Webhook V2: {message} | {symbol} | bar_time={bar_time} | sent_at={sent_at}")

    # 🔹 Отправка сигнала в Redis Stream (название: signals_stream)
    await redis_client.xadd("signals_stream", {
        "message": message,
        "symbol": symbol,
        "bar_time": bar_time or "",
        "sent_at": sent_at or "",
        "received_at": received_at
    })

    # 🔹 Ответ клиенту
    return JSONResponse({"status": "ok", "received_at": received_at})