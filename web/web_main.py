# web_main.py — интерфейс и управление тикерами (обновлено с ticker_count)

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
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
# 19. Страница параметров индикаторов по тикеру
@app.get("/indicators", response_class=HTMLResponse)
async def indicators_main_page(request: Request, symbol: str = None):
    conn = await get_db()
    rows = await conn.fetch("SELECT symbol FROM tickers WHERE status = 'enabled' ORDER BY symbol ASC")
    await conn.close()
    symbols = [row["symbol"] for row in rows]

    selected_symbol = symbol or (symbols[0] if symbols else None)
    indicator_data = {}

    if selected_symbol:
        key = f"indicators:{selected_symbol}"
        raw = await r.get(key)
        if raw:
            try:
                indicator_data = json.loads(raw)
            except Exception as e:
                print(f"[ERROR] Failed to parse Redis JSON for {selected_symbol}: {e}", flush=True)

    return templates.TemplateResponse("ticker_param.html", {
        "request": request,
        "tickers": symbols,
        "selected_symbol": selected_symbol,
        "indicators": indicator_data
    })
# 20. Страница настройки параметров индикаторов
@app.get("/indicators/settings", response_class=HTMLResponse)
async def indicators_param_page(request: Request):
    db_url = os.getenv("DATABASE_URL")
    try:
        conn = await asyncpg.connect(dsn=db_url)
        rows = await conn.fetch("SELECT indicator, param, value FROM indicator_settings")
        await conn.close()
    except Exception as e:
        print(f"[ERROR] Failed to load indicator settings: {e}", flush=True)
        rows = []

    settings = {}
    for row in rows:
        ind = row["indicator"]
        param = row["param"]
        value = row["value"]
        if ind not in settings:
            settings[ind] = {}
        try:
            value = float(value)
            if value.is_integer():
                value = int(value)
        except:
            pass
        settings[ind][param] = value

    return templates.TemplateResponse("indicators_param.html", {
        "request": request,
        "settings": settings
    }) 
# 21. Обновление параметров индикаторов
@app.post("/indicators/settings", response_class=HTMLResponse)
async def update_indicator_settings(
    request: Request,
    rsi_period: int = Form(...),
    smi_k: int = Form(...),
    smi_d: int = Form(...),
    smi_s: int = Form(...),
    atr_period: int = Form(...),
    lr_length: int = Form(...),
    lr_angle_up: float = Form(...),
    lr_angle_down: float = Form(...)
):
    values = {
        "rsi": {"period": rsi_period},
        "smi": {"k": smi_k, "d": smi_d, "s": smi_s},
        "atr": {"period": atr_period},
        "lr": {
            "length": lr_length,
            "angle_up": lr_angle_up,
            "angle_down": lr_angle_down
        }
    }

    conn = await get_db()
    for indicator, params in values.items():
        for param, value in params.items():
            await conn.execute("""
                INSERT INTO indicator_settings (indicator, param, value)
                VALUES ($1, $2, $3)
                ON CONFLICT (indicator, param)
                DO UPDATE SET value = EXCLUDED.value
            """, indicator, param, str(value))
    await conn.close()

    return RedirectResponse("/indicators/settings", status_code=303)
# 22. Страница всех индикаторов по тикерам
@app.get("/indicators/all", response_class=HTMLResponse)
async def all_indicators_page(request: Request):
    conn = await get_db()
    rows = await conn.fetch("""
        SELECT symbol, rsi, smi, smi_signal, atr,
               lr_angle, lr_trend, lr_mid, lr_upper, lr_lower
        FROM ohlcv_m5
        WHERE complete = true
        AND open_time = (
            SELECT MAX(open_time) FROM ohlcv_m5 o2 WHERE o2.symbol = ohlcv_m5.symbol
        )
        ORDER BY symbol
    """)
    await conn.close()

    return templates.TemplateResponse("ticker_indicators.html", {
        "request": request,
        "indicators": rows
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