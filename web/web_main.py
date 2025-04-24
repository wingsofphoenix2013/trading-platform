# web_main.py — интерфейс и управление тикерами (обновлено с ticker_count)

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from decimal import Decimal
from decimal import ROUND_DOWN
import asyncpg
import redis.asyncio as redis
import os
import json

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# 1. Подключение к PostgreSQL и Redis через окружение
async def get_db():
    return await asyncpg.connect(dsn=os.getenv("DATABASE_URL"))

r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    ssl=True
)

# 2. Главная страница (динамическое количество тикеров)
@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    conn = await get_db()
    count = await conn.fetchval("SELECT COUNT(*) FROM tickers")
    await conn.close()
    return templates.TemplateResponse("base.html", {"request": request, "ticker_count": count})

# 3. Список тикеров
@app.get("/tickers", response_class=HTMLResponse)
async def list_tickers(request: Request):
    conn = await get_db()
    rows = await conn.fetch("SELECT * FROM tickers ORDER BY created_at DESC")
    count = await conn.fetchval("SELECT COUNT(*) FROM tickers")
    await conn.close()
    return templates.TemplateResponse("tickers.html", {"request": request, "tickers": rows, "ticker_count": count})

# 4. Отображение формы создания тикера (popup)
@app.get("/tickers/new", response_class=HTMLResponse)
async def new_ticker_form(request: Request):
    conn = await get_db()
    count = await conn.fetchval("SELECT COUNT(*) FROM tickers")
    await conn.close()
    return templates.TemplateResponse("ticker_form.html", {"request": request, "ticker_count": count})

# 5. Детали тикера
@app.get("/tickers/{symbol}", response_class=HTMLResponse)
async def ticker_detail(symbol: str, request: Request):
    conn = await get_db()
    row = await conn.fetchrow("SELECT * FROM tickers WHERE symbol = $1", symbol.upper())
    count = await conn.fetchval("SELECT COUNT(*) FROM tickers")
    await conn.close()
    if row:
        return templates.TemplateResponse("ticker_detail.html", {"request": request, "ticker": row, "strategies": [], "ticker_count": count})
    return HTMLResponse("Тикер не найден", status_code=404)

# 6. Создание нового тикера
@app.post("/tickers")
async def create_ticker(
    request: Request,
    symbol: str = Form(...),
    precision_price: int = Form(...),
    precision_qty: int = Form(...),
    min_qty: float = Form(...)
):
    conn = await get_db()
    await conn.execute("""
        INSERT INTO tickers (symbol, precision_price, precision_qty, min_qty)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (symbol) DO NOTHING
    """, symbol.upper(), precision_price, precision_qty, min_qty)
    await conn.close()
    return templates.TemplateResponse("ticker_success.html", {"request": request})

# 7. Активация тикера (через Redis)
@app.post("/tickers/{symbol}/activate")
async def activate_ticker(symbol: str):
    await r.publish("ticker_activation", f'{{"symbol": "{symbol.upper()}", "action": "activate"}}')
    conn = await get_db()
    await conn.execute("UPDATE tickers SET status = 'enabled' WHERE symbol = $1", symbol.upper())
    await conn.close()
    return RedirectResponse(url="/tickers", status_code=303)
    
# 8. Список всех сигналов
@app.get("/signals", response_class=HTMLResponse)
async def list_signals(request: Request):
    conn = await get_db()
    rows = await conn.fetch("SELECT * FROM signals ORDER BY created_at DESC")
    await conn.close()
    return templates.TemplateResponse("signals_list.html", {
        "request": request,
        "signals": rows
    })

# 9. Форма создания нового сигнала
@app.get("/signals/new", response_class=HTMLResponse)
async def new_signal_form(request: Request):
    return templates.TemplateResponse("signal_form.html", {
        "request": request,
        "mode": "create",
        "signal": {}
    })

# 10. Сохранение нового сигнала
@app.post("/signals")
async def create_signal(
    request: Request,
    name: str = Form(...),
    signal_type: str = Form(...),
    long_phrase: str = Form(None),
    short_phrase: str = Form(None),
    long_exit_phrase: str = Form(None),
    short_exit_phrase: str = Form(None),
    source: str = Form(None),
    description: str = Form(None),
    enabled: str = Form(None)
):
    conn = await get_db()

    # Преобразование чекбокса в булево значение
    enabled_bool = True if enabled == "true" else False

    # Вставка записи
    await conn.execute("""
        INSERT INTO signals (
            name, signal_type, long_phrase, short_phrase,
            long_exit_phrase, short_exit_phrase,
            source, description, enabled, created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, NOW()
        )
    """, name, signal_type, long_phrase, short_phrase,
         long_exit_phrase, short_exit_phrase,
         source, description, enabled_bool)

    await conn.close()
    return RedirectResponse(url="/signals", status_code=303)

# 11. Обновление сигнала
@app.post("/signals/{signal_id}")
async def update_signal(
    signal_id: int,
    request: Request,
    long_phrase: str = Form(None),
    short_phrase: str = Form(None),
    long_exit_phrase: str = Form(None),
    short_exit_phrase: str = Form(None),
    source: str = Form(None),
    description: str = Form(None),
    enabled: str = Form(None)
):
    conn = await get_db()

    # Получаем текущее состояние сигнала из базы
    existing = await conn.fetchrow("SELECT * FROM signals WHERE id = $1", signal_id)
    if not existing:
        await conn.close()
        return HTMLResponse("Сигнал не найден", status_code=404)

    # Определяем: было ли изменение флага enabled
    enabled_bool = True if enabled == "true" else False
    enabled_changed = enabled_bool != existing["enabled"]

    # Проверка уникальности сигнальных фраз
    for field_name, value in [
        ("long_phrase", long_phrase),
        ("short_phrase", short_phrase),
        ("long_exit_phrase", long_exit_phrase),
        ("short_exit_phrase", short_exit_phrase)
    ]:
        if value:
            exists = await conn.fetchval(
                f"SELECT COUNT(*) FROM signals WHERE {field_name} = $1 AND id != $2",
                value, signal_id
            )
            if exists:
                await conn.close()
                return HTMLResponse(
                    f"Ошибка: фраза в поле '{field_name}' уже используется другим сигналом.",
                    status_code=400
                )

    # Обновление разрешённых полей в базе
    await conn.execute("""
        UPDATE signals SET
            long_phrase = $1,
            short_phrase = $2,
            long_exit_phrase = $3,
            short_exit_phrase = $4,
            source = $5,
            description = $6,
            enabled = $7
        WHERE id = $8
    """, long_phrase, short_phrase, long_exit_phrase, short_exit_phrase,
         source, description, enabled_bool, signal_id)

    # Если статус enabled изменился — публикуем сообщение в Redis
    if enabled_changed:
        await r.publish("signal_activation", json.dumps({
            "id": signal_id,
            "enabled": enabled_bool
        }))

    await conn.close()
    return RedirectResponse(url="/signals", status_code=303)
# 12. Форма редактирования сигнала
@app.get("/signals/{signal_id}/edit", response_class=HTMLResponse)
async def edit_signal_form(signal_id: int, request: Request):
    conn = await get_db()
    row = await conn.fetchrow("SELECT * FROM signals WHERE id = $1", signal_id)
    await conn.close()

    if not row:
        return HTMLResponse("Сигнал не найден", status_code=404)

    return templates.TemplateResponse("signal_form.html", {
        "request": request,
        "mode": "edit",
        "signal": row
    })
    
# 13. Обновление кода для получения сигналов из TradingVew
from fastapi.responses import PlainTextResponse
import redis.asyncio as redis
import json

# Подключение к Redis (использует те же переменные окружения)
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    ssl=True
)

@app.post("/webhook", response_class=PlainTextResponse)
async def receive_webhook(request: Request):
    try:
        body = await request.body()
        message = body.decode("utf-8").strip()
    except Exception as e:
        print(f"[webhook] Ошибка чтения тела запроса: {e}", flush=True)
        return PlainTextResponse("Malformed request", status_code=400)

    if " " not in message:
        print(f"[webhook] Некорректный формат сигнала: '{message}'", flush=True)
        return PlainTextResponse("Invalid format", status_code=400)

    # Публикуем в Redis
    payload = {
        "message": message,
        "source": "tradingview"
    }
    await r.publish("incoming_signals", json.dumps(payload))
    print(f"[webhook] Принят и опубликован: {payload}", flush=True)

    return PlainTextResponse("Signal accepted", status_code=200)            
# 14. Отображение списка стратегий
# Страница /strategies — отображает все стратегии в виде таблицы с метриками за текущие сутки (UTC)

@app.get("/strategies", response_class=HTMLResponse)
async def list_strategies(request: Request):
    # Шаг 1. Загружаем стратегии и управляющий сигнал
    conn = await get_db()
    strategy_rows = await conn.fetch("""
        SELECT
            s.id,
            s.name,
            s.enabled,
            sig.name AS signal_name
        FROM strategies s
        LEFT JOIN strategy_signals ss ON ss.strategy_id = s.id AND ss.role = 'action'
        LEFT JOIN signals sig ON sig.id = ss.signal_id
        WHERE s.archived = false
        ORDER BY s.created_at DESC
    """)

    # Шаг 2. Загружаем метрики по закрытым сделкам за сегодня (UTC)
    from datetime import datetime, timedelta
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)

    metrics_rows = await conn.fetch("""
        SELECT
            strategy_id,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE pnl > 0) AS wins,
            SUM(pnl) AS pnl
        FROM positions
        WHERE status = 'closed'
          AND closed_at >= $1 AND closed_at < $2
        GROUP BY strategy_id
    """, today, tomorrow)

    await conn.close()

    # Шаг 3. Построение словаря: strategy_id → метрики
    metrics_map = {
        row["strategy_id"]: {
            "total": row["total"],
            "winrate": f"{int((row['wins'] / row['total']) * 100)}%" if row["total"] > 0 else "n/a",
            "pnl": f"{row['pnl']:.2f}" if row["pnl"] is not None else "n/a"
        }
        for row in metrics_rows
    }

    # Шаг 4. Объединяем стратегии и метрики
    strategies = []
    for s in strategy_rows:
        s = dict(s)
        s["metrics"] = metrics_map.get(s["id"], {
            "total": "n/a", "winrate": "n/a", "pnl": "n/a"
        })
        strategies.append(s)

    # Шаг 5. Отдаём шаблон
    return templates.TemplateResponse("strategies_list.html", {
        "request": request,
        "strategies": strategies
    })
# 15. Форма создания стратегии (GET)
# Отображает пустую форму для добавления новой стратегии
@app.get("/strategies/new", response_class=HTMLResponse)
async def new_strategy_form(request: Request):
    conn = await get_db()
    signals = await conn.fetch("""
        SELECT id, name FROM signals
        WHERE signal_type = 'action' AND enabled = true
        ORDER BY name
    """)
    await conn.close()

    return templates.TemplateResponse("strategy_form.html", {
        "request": request,
        "mode": "create",
        "strategy": {},
        "signals": signals
    })
# 16. Сохранение новой стратегии (POST)
# Принимает данные из формы и сохраняет стратегию + связь с управляющим сигналом (action)

@app.post("/strategies")
async def create_strategy(
    request: Request,
    name: str = Form(...),
    description: str = Form(None),
    deposit: float = Form(...),
    position_limit: float = Form(...),
    use_all_tickers: str = Form(None),
    enabled: str = Form(None),
    action_signal_id: int = Form(...),
    use_stoploss: str = Form(None),
    sl_type: str = Form(None),
    sl_value: float = Form(None)
):
    use_all_tickers_bool = use_all_tickers == "true"
    enabled_bool = enabled == "true"
    use_stoploss_bool = use_stoploss == "true"

    conn = await get_db()

    # Вставка стратегии и возврат её ID
    row = await conn.fetchrow("""
        INSERT INTO strategies (
            name, description, deposit, position_limit,
            use_all_tickers, enabled,
            use_stoploss, sl_type, sl_value,
            created_at
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6,
            $7, $8, $9,
            NOW()
        )
        RETURNING id
    """, name, description, deposit, position_limit,
         use_all_tickers_bool, enabled_bool,
         use_stoploss_bool, sl_type, sl_value)
    strategy_id = row["id"]

    # Привязка управляющего сигнала (action)
    await conn.execute("""
        INSERT INTO strategy_signals (strategy_id, signal_id, role)
        VALUES ($1, $2, 'action')
    """, strategy_id, action_signal_id)

    await conn.close()
    return RedirectResponse(url="/strategies", status_code=303)
# 17. Форма редактирования стратегии (GET)
# Загружает стратегию по ID и отображает форму с автозаполнением + выбор управляющего сигнала

@app.get("/strategies/{strategy_id}/edit", response_class=HTMLResponse)
async def edit_strategy_form(strategy_id: int, request: Request):
    conn = await get_db()

    # Загружаем стратегию
    strategy = await conn.fetchrow("SELECT * FROM strategies WHERE id = $1", strategy_id)
    if not strategy:
        await conn.close()
        return HTMLResponse("Стратегия не найдена", status_code=404)

    # Загружаем все активные action-сигналы
    signals = await conn.fetch("""
        SELECT id, name FROM signals
        WHERE signal_type = 'action' AND enabled = true
        ORDER BY name
    """)

    # Загружаем текущий управляющий сигнал этой стратегии
    current_signal_id = await conn.fetchval("""
        SELECT signal_id FROM strategy_signals
        WHERE strategy_id = $1 AND role = 'action'
    """, strategy_id)

    await conn.close()

    return templates.TemplateResponse("strategy_form.html", {
        "request": request,
        "mode": "edit",
        "strategy": strategy,
        "signals": signals,
        "current_signal_id": current_signal_id
    })
# 18. Сохранение изменений стратегии (POST)
# Обновляет стратегию и её управляющий сигнал
# 18. Сохранение изменений стратегии (POST)
# Обновляет стратегию и её управляющий сигнал

@app.post("/strategies/{strategy_id}")
async def update_strategy(
    strategy_id: int,
    request: Request,
    description: str = Form(None),
    deposit: float = Form(...),
    position_limit: float = Form(...),
    use_all_tickers: str = Form(None),
    enabled: str = Form(None),
    action_signal_id: int = Form(...),
    use_stoploss: str = Form(None),
    sl_type: str = Form(None),
    sl_value: float = Form(None)
):
    use_all_tickers_bool = use_all_tickers == "true"
    enabled_bool = enabled == "true"
    use_stoploss_bool = use_stoploss == "true"

    conn = await get_db()

    # Обновляем стратегию
    await conn.execute("""
        UPDATE strategies SET
            description = $1,
            deposit = $2,
            position_limit = $3,
            use_all_tickers = $4,
            enabled = $5,
            use_stoploss = $6,
            sl_type = $7,
            sl_value = $8
        WHERE id = $9
    """, description, deposit, position_limit,
         use_all_tickers_bool, enabled_bool,
         use_stoploss_bool, sl_type, sl_value,
         strategy_id)

    # Удаляем старую связь сигнала
    await conn.execute("""
        DELETE FROM strategy_signals
        WHERE strategy_id = $1 AND role = 'action'
    """, strategy_id)

    # Вставляем новую
    await conn.execute("""
        INSERT INTO strategy_signals (strategy_id, signal_id, role)
        VALUES ($1, $2, 'action')
    """, strategy_id, action_signal_id)

    await conn.close()
    return RedirectResponse(url="/strategies", status_code=303)
# 19. Страница индикаторов с выбором типа и таймфрейма
@app.get("/indicators", response_class=HTMLResponse)
async def indicators_view(request: Request, tf: str = 'M1', type: str = 'ema'):
    conn = await get_db()
    tickers = await conn.fetch("SELECT symbol FROM tickers WHERE status = 'enabled' ORDER BY symbol")

    data = []
    for row in tickers:
        symbol = row["symbol"]

        if type == 'ema':
            raw = await conn.fetch(
                """
                SELECT param_name, value, open_time
                FROM indicator_values
                WHERE symbol = $1 AND timeframe = $2 AND indicator = 'EMA'
                ORDER BY param_name, open_time DESC
                """,
                symbol, tf
            )
            seen = set()
            latest = {}
            updated = None
            for r in raw:
                name = r["param_name"]
                if name not in seen:
                    latest[name] = r["value"]
                    seen.add(name)
                    if updated is None or r["open_time"] > updated:
                        updated = r["open_time"]

            data.append({
                "symbol": symbol,
                "tf": tf,
                "ema50": latest.get("ema50"),
                "ema100": latest.get("ema100"),
                "ema200": latest.get("ema200"),
                "updated_at": updated if latest else None,
            })

        elif type == 'smi':
            raw = await conn.fetch(
                """
                SELECT param_name, value, open_time
                FROM indicator_values
                WHERE symbol = $1 AND timeframe = $2 AND indicator = 'SMI'
                ORDER BY param_name, open_time DESC
                """,
                symbol, tf
            )
            seen = set()
            latest = {}
            updated = None
            for r in raw:
                name = r["param_name"]
                if name not in seen:
                    latest[name] = r["value"]
                    seen.add(name)
                    if updated is None or r["open_time"] > updated:
                        updated = r["open_time"]

            data.append({
                "symbol": symbol,
                "tf": tf,
                "smi": latest.get("smi"),
                "smi_signal": latest.get("smi_signal"),
                "updated_at": updated if latest else None,
            })

        elif type == 'atr':
            raw = await conn.fetch(
                """
                SELECT value, open_time
                FROM indicator_values
                WHERE symbol = $1 AND timeframe = $2 AND indicator = 'ATR' AND param_name = 'atr'
                ORDER BY open_time DESC
                LIMIT 1
                """,
                symbol, tf
            )
            if raw:
                r = raw[0]
                data.append({
                    "symbol": symbol,
                    "tf": tf,
                    "atr": r["value"],
                    "updated_at": r["open_time"]
                })
            else:
                data.append({
                    "symbol": symbol,
                    "tf": tf,
                    "atr": None,
                    "updated_at": None
                })

        elif type == 'rsi':
            raw = await conn.fetch(
                """
                SELECT value, open_time
                FROM indicator_values
                WHERE symbol = $1 AND timeframe = $2 AND indicator = 'RSI' AND param_name = 'rsi'
                ORDER BY open_time DESC
                LIMIT 1
                """,
                symbol, tf
            )
            if raw:
                r = raw[0]
                data.append({
                    "symbol": symbol,
                    "tf": tf,
                    "rsi": r["value"],
                    "updated_at": r["open_time"]
                })
            else:
                data.append({
                    "symbol": symbol,
                    "tf": tf,
                    "rsi": None,
                    "updated_at": None
                })

        elif type == 'macd':
            raw = await conn.fetch(
                """
                SELECT param_name, value, open_time
                FROM indicator_values
                WHERE symbol = $1 AND timeframe = $2 AND indicator = 'MACD'
                ORDER BY param_name, open_time DESC
                """,
                symbol, tf
            )
            seen = set()
            latest = {}
            updated = None
            for r in raw:
                name = r["param_name"]
                if name not in seen:
                    latest[name] = r["value"]
                    seen.add(name)
                    if updated is None or r["open_time"] > updated:
                        updated = r["open_time"]

            data.append({
                "symbol": symbol,
                "tf": tf,
                "macd": latest.get("macd"),
                "macd_signal": latest.get("macd_signal"),
                "updated_at": updated if latest else None,
            })
            
    await conn.close()

    return templates.TemplateResponse("ticker_indicators.html", {
        "request": request,
        "data": data,
        "tf": tf,
        "type": type
    })
# 23. Просмотр стратегии по ID (GET)
# Загружает стратегию, управляющий сигнал, открытые и закрытые позиции, метрики и историю сделок

@app.get("/strategies/{strategy_id}", response_class=HTMLResponse)
async def view_strategy(strategy_id: int, request: Request, period: str = "today", page: int = 1):
    conn = await get_db()

    # Стратегия
    strategy = await conn.fetchrow("SELECT * FROM strategies WHERE id = $1", strategy_id)
    if not strategy:
        await conn.close()
        return HTMLResponse("Стратегия не найдена", status_code=404)

    # Управляющий сигнал
    signal = await conn.fetchval("""
        SELECT name FROM signals
        WHERE id = (
            SELECT signal_id FROM strategy_signals
            WHERE strategy_id = $1 AND role = 'action'
        )
    """, strategy_id)

    # Открытые позиции
    open_positions = await conn.fetch("""
        SELECT id, symbol, direction, created_at, close_reason, pnl
        FROM positions
        WHERE strategy_id = $1 AND status = 'open'
        ORDER BY created_at DESC
    """, strategy_id)

    # Определение периода
    from datetime import datetime, timedelta
    now = datetime.utcnow()
    date_filter_sql = ""
    params = [strategy_id]
    if period == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        date_filter_sql = "AND closed_at >= $2 AND closed_at < $3"
        params += [start, start + timedelta(days=1)]
    elif period == "yesterday":
        start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        date_filter_sql = "AND closed_at >= $2 AND closed_at < $3"
        params += [start, end]
    elif period == "week":
        start = now - timedelta(days=7)
        date_filter_sql = "AND closed_at >= $2"
        params += [start]

    # Метрики
    metric_result = await conn.fetchrow(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE pnl > 0) AS wins,
            SUM(pnl) AS pnl
        FROM positions
        WHERE strategy_id = $1 AND status = 'closed'
        {date_filter_sql}
    """, *params)

    total = metric_result["total"] or 0
    wins = metric_result["wins"] or 0
    pnl_sum = float(metric_result["pnl"]) if metric_result["pnl"] is not None else None
    winrate = int((wins / total) * 100) if total > 0 else None
    roi = round((pnl_sum / float(strategy["deposit"])) * 100, 4) if pnl_sum is not None and strategy["deposit"] else None

    # Закрытые сделки: пагинация
    limit = 20
    offset = (page - 1) * limit
    closed_query = f"""
        SELECT id, symbol, created_at, entry_price, closed_at, close_reason, pnl
        FROM positions
        WHERE strategy_id = $1 AND status = 'closed'
        {date_filter_sql}
        ORDER BY closed_at DESC
        LIMIT {limit} OFFSET {offset}
    """
    closed_positions = await conn.fetch(closed_query, *params)

    await conn.close()

    return templates.TemplateResponse("strategy_detail.html", {
        "request": request,
        "strategy": strategy,
        "signal_name": signal or "n/a",
        "open_positions": open_positions,
        "period": period,
        "page": page,
        "closed_positions": closed_positions,
        "metrics": {
            "total": total if total > 0 else "n/a",
            "winrate": f"{winrate}%" if winrate is not None else "n/a",
            "pnl": f"{pnl_sum:.2f}" if pnl_sum is not None else "n/a",
            "roi": f"{roi:.4f}%" if roi is not None else "n/a",
        }
    })
# 24. Переключение enabled для стратегии (если нет открытых позиций)
@app.post("/strategies/{strategy_id}/toggle-enabled")
async def toggle_enabled(strategy_id: int):
    conn = await get_db()

    # Проверка: есть ли открытые позиции
    open_count = await conn.fetchval("""
        SELECT COUNT(*) FROM positions
        WHERE strategy_id = $1 AND status = 'open'
    """, strategy_id)

    if open_count > 0:
        await conn.close()
        return JSONResponse(
            status_code=400,
            content={"error": "Невозможно выключить стратегию с открытыми позициями."}
        )

    # Переключаем enabled
    await conn.execute("""
        UPDATE strategies
        SET enabled = NOT enabled
        WHERE id = $1
    """, strategy_id)

    # Публикуем обновление в Redis
    import redis.asyncio as redis_lib
    import os
    r = redis_lib.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD"),
        ssl=True
    )
    await r.publish("strategy_activation", str(strategy_id))

    await conn.close()
    return JSONResponse(content={"ok": True})
    
# 25. Переключение архивации стратегии (если стратегия отключена)
@app.post("/strategies/{strategy_id}/toggle-archive")
async def toggle_archive(strategy_id: int):
    conn = await get_db()

    enabled = await conn.fetchval("""
        SELECT enabled FROM strategies WHERE id = $1
    """, strategy_id)

    if enabled:
        await conn.close()
        return JSONResponse(
            status_code=400,
            content={"error": "Сначала отключите стратегию перед архивированием."}
        )

    await conn.execute("""
        UPDATE strategies
        SET archived = NOT archived
        WHERE id = $1
    """, strategy_id)

    await conn.close()
    return JSONResponse(content={"ok": True})
# 26. Аварийная остановка: закрывает все открытые позиции и отключает стратегию
@app.post("/strategies/{strategy_id}/emergency-stop")
async def emergency_stop(strategy_id: int):
    conn = await get_db()

    # Получаем все открытые позиции по стратегии
    open_positions = await conn.fetch("""
        SELECT positions.id, positions.symbol, direction, entry_price, quantity, precision_price
        FROM positions
        JOIN tickers ON positions.symbol = tickers.symbol
        WHERE positions.strategy_id = $1 AND positions.status = 'open'
    """, strategy_id)

    if not open_positions:
        await conn.execute("""
            UPDATE strategies
            SET enabled = false
            WHERE id = $1
        """, strategy_id)
        await conn.close()
        return JSONResponse(content={"ok": True, "note": "No open positions found, strategy disabled."})

    # Подключаемся к Redis
    import redis.asyncio as redis_lib
    import os
    r = redis_lib.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD"),
        ssl=True
    )

    for pos in open_positions:
        position_id = pos["id"]
        symbol = pos["symbol"]
        direction = pos["direction"]
        entry_price = Decimal(pos["entry_price"])
        quantity = Decimal(pos["quantity"])
        precision_price = int(pos["precision_price"])

        # Получаем цену из Redis
        price_raw = await r.get(f"price:{symbol}")
        if not price_raw:
            continue  # пропускаем, если нет цены

        current_price = Decimal(price_raw.decode()).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)

        # Расчёт PnL
        notional_value = (entry_price * quantity).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)
        commission = (notional_value * Decimal("0.04") / 100).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)

        if direction == "long":
            pnl = ((current_price - entry_price) * quantity - commission).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)
        else:
            pnl = ((entry_price - current_price) * quantity - commission).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)

        # Закрываем позицию
        await conn.execute("""
            UPDATE positions
            SET
                status = 'closed',
                exit_price = $1,
                closed_at = NOW(),
                close_reason = 'emergency',
                pnl = $2,
                quantity_left = 0
            WHERE id = $3
        """, current_price, pnl, position_id)

        # Отменяем все цели
        await conn.execute("""
            UPDATE position_targets
            SET hit = false, hit_at = NULL, canceled = true
            WHERE position_id = $1 AND hit = false AND canceled = false
        """, position_id)

    # Отключаем стратегию
    await conn.execute("""
        UPDATE strategies
        SET enabled = false
        WHERE id = $1
    """, strategy_id)

    await conn.close()
    return JSONResponse(content={"ok": True, "note": "Emergency stop executed. All open positions closed."})        