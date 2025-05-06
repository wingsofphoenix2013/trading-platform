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
        # 🔹 Сигналы типа action — для управляющего сигнала
        signals = await conn.fetch("""
            SELECT id, name, signal_type, enabled
            FROM signals_v2
            WHERE signal_type = 'action'
            ORDER BY name
        """)

        # 🔹 Сигналы типа exit — для TP уровней
        exit_signals = await conn.fetch("""
            SELECT id, name, enabled
            FROM signals_v2
            WHERE signal_type = 'exit'
            ORDER BY name
        """)

        # 🔹 Активные тикеры с разрешением на торговлю
        tickers = await conn.fetch("""
            SELECT symbol, status, tradepermission
            FROM tickers
            WHERE status = 'enabled' AND tradepermission = 'enabled'
            ORDER BY symbol
        """)

        return templates.TemplateResponse("strategies_new.html", {
            "request": request,
            "signals": signals,
            "exit_signals": exit_signals,
            "tickers": tickers,
            "reverse": False  # reverse по умолчанию отключён
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

        # 🔹 Сохранение управляющего сигнала
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

        # 🔹 Сохранение TP уровней (включая external_signal)
        tp_count = int(form.get("tp_count") or 0)
        tp_level_ids = []

        for i in range(1, tp_count + 1):
                tp_type = form.get(f"tp_type_{i}")
                volume_percent = float(form.get(f"volume_{i}") or 0)
                tp_value_raw = form.get(f"tp_value_{i}")

                # 🔸 Логика значения TP
                if tp_type == "external_signal":
                        trigger_signal_raw = form.get(f"tp_value_{i}")
                        if trigger_signal_raw == "__USE_ACTION_SIGNAL__":
                                if not reverse:
                                        raise HTTPException(
                                                status_code=400,
                                                detail="Reverse = false: управляющий сигнал нельзя использовать как TP."
                                        )
                                trigger_signal_id = action_signal_id
                                tp_value = None
                        else:
                                trigger_signal_id = int(trigger_signal_raw) if trigger_signal_raw else None
                                tp_value = None
                        tp_trigger_type = "signal"
                else:
                        tp_value = float(tp_value_raw) if tp_value_raw else None
                        trigger_signal_id = None
                        tp_trigger_type = "price"

                result = await conn.fetchrow("""
                        INSERT INTO strategy_tp_levels_v2 (
                                strategy_id, level, tp_type, tp_value,
                                volume_percent, tp_trigger_type, trigger_signal_id
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        RETURNING id
                """, strategy_id, i, tp_type, tp_value, volume_percent, tp_trigger_type, trigger_signal_id)

                tp_level_ids.append(result["id"])

        # 🔹 Подготовка SL-настроек после TP
        sl_behavior = []
        for i in range(1, tp_count):
                sl_mode = form.get(f"sl_mode_{i}")
                sl_value_raw = form.get(f"sl_value_{i}")
                sl_value = None

                if sl_mode in ("atr", "percent"):
                        sl_value = float(sl_value_raw) if sl_value_raw else None

                sl_behavior.append((
                        strategy_id,
                        tp_level_ids[i - 1],
                        sl_mode,
                        sl_value
                ))

        # 🔹 Вставка SL-настроек
        for strategy_id, tp_level_id, sl_mode, sl_value in sl_behavior:
                await conn.execute("""
                        INSERT INTO strategy_tp_sl_v2 (
                                strategy_id, tp_level_id, sl_mode, sl_value
                        )
                        VALUES ($1, $2, $3, $4)
                """, strategy_id, tp_level_id, sl_mode, sl_value)
                
        return RedirectResponse(url="/strategies", status_code=302)
    finally:
        await conn.close()
# 🔸 Проверка уникальности названия стратегии
@app.get("/strategies/check_name")
async def check_strategy_name(name: str):
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
    try:
        exists = await conn.fetchval("""
            SELECT EXISTS (
              SELECT 1 FROM strategies_v2 WHERE LOWER(name) = LOWER($1)
            )
        """, name)
        return {"exists": exists}
    finally:
        await conn.close()        