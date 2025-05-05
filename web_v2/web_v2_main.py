import os
import logging
from pathlib import Path
from datetime import datetime
import asyncpg
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.responses import RedirectResponse
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

# 🔸 Страница создания новой стратегии (форма + список сигналов/тикеров)
@app.get("/strategies/new", response_class=HTMLResponse)
async def strategy_new(request: Request):
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
    try:
        signals = await conn.fetch("""
            SELECT id, name, signal_type, enabled
            FROM signals_v2
            WHERE signal_type = 'action'
            ORDER BY name
        """)

        tickers = await conn.fetch("""
            SELECT symbol, status, tradepermission
            FROM tickers
            WHERE status = 'enabled' AND tradepermission = 'enabled'
            ORDER BY symbol
        """)

        return templates.TemplateResponse("strategies_new.html", {
            "request": request,
            "signals": signals,
            "tickers": tickers
        })
    finally:
        await conn.close()
# 🔸 Обработка формы создания стратегии (POST /strategies/new)
@app.post("/strategies/new")
async def create_strategy(request: Request):
    form = await request.form()
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
    try:
        # 🔹 Парсинг основных полей
        name = form.get("name")
        description = form.get("description")
        deposit = float(form.get("deposit") or 0)
        position_limit = float(form.get("position_limit") or 0)
        max_risk = int(form.get("max_risk") or 0)
        leverage = float(form.get("leverage") or 1)
        timeframe = form.get("timeframe")
        use_stoploss = "use_stoploss" in form
        sl_type = form.get("sl_type")
        sl_value = float(form.get("sl_value") or 0)
        reverse = "reverse" in form
        use_all_tickers = "use_all_tickers" in form
        action_signal_id = int(form.get("action_signal_id") or 0)

        # 🔹 Вставка стратегии
        result = await conn.fetchrow("""
            INSERT INTO strategies_v2 (
              name, description, deposit, position_limit, max_risk, leverage,
              use_stoploss, sl_type, sl_value, reverse, use_all_tickers,
              timeframe, allow_open, enabled, archived
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, true, false, false)
            RETURNING id
        """, name, description, deposit, position_limit, max_risk, leverage,
             use_stoploss, sl_type, sl_value, reverse, use_all_tickers, timeframe)
        strategy_id = result["id"]

        # 🔹 Сохранение сигнала
        if action_signal_id > 0:
            await conn.execute("""
                INSERT INTO strategy_signals_v2 (strategy_id, signal_id, role)
                VALUES ($1, $2, 'action')
            """, strategy_id, action_signal_id)

        # 🔹 Привязка тикеров при use_all_tickers = false
        if not use_all_tickers:
            for key in form:
                if key.startswith("ticker_") and form.get(key) == "on":
                    symbol = key.replace("ticker_", "")
                    ticker = await conn.fetchrow("SELECT id FROM tickers WHERE symbol = $1", symbol)
                    if ticker:
                        await conn.execute("""
                            INSERT INTO strategy_tickers_v2 (strategy_id, ticker_id, enabled)
                            VALUES ($1, $2, true)
                        """, strategy_id, ticker["id"])

        return RedirectResponse(url="/strategies", status_code=302)
    finally:
        await conn.close()              