import os
import logging
from pathlib import Path
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import redis.asyncio as redis

# 🔸 Инициализация FastAPI приложения
app = FastAPI()

# 🔸 Абсолютный путь к шаблонам (для совместимости с Render)
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

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
    
# 🔸 Главная страница
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# 🔸 Страница тикеров
@app.get("/tickers", response_class=HTMLResponse)
async def tickers(request: Request):
    return templates.TemplateResponse("tickers.html", {"request": request})

# 🔸 Страница индикаторов
@app.get("/indicators", response_class=HTMLResponse)
async def indicators(request: Request):
    return templates.TemplateResponse("indicators.html", {"request": request})

# 🔸 Страница сигналов
@app.get("/signals", response_class=HTMLResponse)
async def signals(request: Request):
    return templates.TemplateResponse("signals.html", {"request": request})

# 🔸 Страница стратегий
@app.get("/strategies", response_class=HTMLResponse)
async def strategies(request: Request):
    return templates.TemplateResponse("strategies.html", {"request": request})

# 🔸 Страница создания новой стратегии
@app.get("/strategies/new", response_class=HTMLResponse)
async def strategy_new(request: Request):
    return templates.TemplateResponse("strategies_new.html", {"request": request})        