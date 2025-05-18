import os
import logging
from pathlib import Path
import asyncpg
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
import redis.asyncio as redis
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def get_period_bounds(period: str, now_utc: datetime) -> tuple[datetime | None, datetime | None]:
    now_local = now_utc.astimezone(ZoneInfo("Europe/Kyiv"))

    if period == "today":
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        end_local = now_local
    elif period == "yesterday":
        y = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        start_local = y
        end_local = y.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif period == "week":
        start_local = now_local - timedelta(days=now_local.weekday())
        start_local = start_local.replace(hour=0, minute=0, second=0, microsecond=0)
        end_local = now_local
    elif period == "all":
        return None, None
    else:
        raise ValueError(f"Unknown period: {period}")

    return start_local.astimezone(ZoneInfo("UTC")), end_local.astimezone(ZoneInfo("UTC"))
        
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
# 🔸 Подключение к базе данных
db_pool = None

async def get_db_pool():
    global db_pool
    if db_pool is None:
        import asyncpg
        from os import getenv
        db_pool = await asyncpg.create_pool(getenv("DATABASE_URL"))
    return db_pool
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

# 🔸 Страница стратегий с расширенной статистикой (только за сегодня)
@app.get("/strategies", response_class=HTMLResponse)
async def strategies(request: Request):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        start_utc, end_utc = get_period_bounds("today", datetime.utcnow())

        rows = await conn.fetch("""
            SELECT s.id, s.name, s.human_name, s.enabled, s.deposit,
                   COUNT(p.id) AS total,
                   COUNT(*) FILTER (WHERE p.direction = 'long') AS long_count,
                   COUNT(*) FILTER (WHERE p.direction = 'short') AS short_count,
                   COUNT(*) FILTER (WHERE p.pnl > 0) AS wins,
                   SUM(p.pnl) AS total_pnl
            FROM strategies_v2 s
            LEFT JOIN positions_v2 p ON p.strategy_id = s.id
                AND p.status = 'closed'
                AND p.closed_at BETWEEN $1 AND $2
            GROUP BY s.id, s.name, s.human_name, s.enabled, s.deposit
        """, start_utc.replace(tzinfo=None), end_utc.replace(tzinfo=None))

        strategies = []
        for row in rows:
            total = row["total"] or 0
            wins = row["wins"] or 0
            pnl = float(row["total_pnl"] or 0)
            deposit = float(row["deposit"] or 1)
            roi = pnl / deposit if deposit > 0 else 0
            winrate = (wins / total) * 100 if total else 0

            strategies.append({
                "id": row["id"],
                "name": row["name"],
                "human_name": row["human_name"],
                "enabled": row["enabled"],
                "total": total,
                "long": row["long_count"] or 0,
                "short": row["short_count"] or 0,
                "winrate": f"{winrate:.1f}%" if total else "n/a",
                "roi": roi,
                "roi_display": f"{roi*100:.1f}%" if total else "n/a",
            })

        # 🔽 Сортировка по ROI (по убыванию)
        strategies.sort(key=lambda x: x["roi"], reverse=True)

        return templates.TemplateResponse("strategies.html", {
            "request": request,
            "strategies": strategies
        })
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
# 🔸 Детальная страница стратегии по имени
# 🔸 Детальная страница стратегии по имени
@app.get("/strategies/detail/{strategy_name}", response_class=HTMLResponse)
async def strategy_detail(request: Request, strategy_name: str, period: str = "all", page: int = 1):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # 🔹 Получение стратегии
        strategy = await conn.fetchrow("""
            SELECT id, name, human_name, deposit
            FROM strategies_v2
            WHERE name = $1
        """, strategy_name)
        
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")

        strategy_id = strategy["id"]
        deposit = float(strategy["deposit"] or 1)

        # 🔹 Определение периода фильтрации
        now_utc = datetime.utcnow()
        start_utc, end_utc = get_period_bounds(period, now_utc)

        # 🔹 Подготовка дат последних 10 дней
        from zoneinfo import ZoneInfo
        from datetime import timedelta
        kyiv_tz = ZoneInfo("Europe/Kyiv")
        today_local = now_utc.astimezone(kyiv_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        date_ranges = []
        date_labels = []
        for i in range(9, -1, -1):
            day_start = today_local - timedelta(days=i)
            day_end = day_start + timedelta(days=1) - timedelta(microseconds=1)
            date_ranges.append((day_start.astimezone(ZoneInfo("UTC")), day_end.astimezone(ZoneInfo("UTC"))))
            date_labels.append(day_start.strftime("%Y-%m-%d"))

        # 🔹 Универсальный сбор статистики по диапазону
        async def collect_stats(start: datetime, end: datetime):
            stats = await conn.fetch("""
                SELECT direction,
                       COUNT(*) AS count,
                       COUNT(*) FILTER (WHERE pnl > 0) AS wins,
                       SUM(pnl) AS pnl
                FROM positions_v2
                WHERE strategy_id = $1 AND status = 'closed'
                  AND closed_at BETWEEN $2 AND $3
                GROUP BY direction
            """, strategy_id, start.replace(tzinfo=None), end.replace(tzinfo=None))

            total = sum(r["count"] for r in stats)
            wins = sum(r["wins"] for r in stats)
            pnl = sum(r["pnl"] or 0 for r in stats)
            
            long_total = next((r["count"] for r in stats if r["direction"] == "long"), 0)
            long_wins = next((r["wins"] for r in stats if r["direction"] == "long"), 0)
            short_total = next((r["count"] for r in stats if r["direction"] == "short"), 0)
            short_wins = next((r["wins"] for r in stats if r["direction"] == "short"), 0)

            return {
                "total": total,
                "short": short_total,
                "long": long_total,
                "short_winrate": f"{(short_wins / short_total * 100):.1f}%" if short_total else "n/a",
                "long_winrate": f"{(long_wins / long_total * 100):.1f}%" if long_total else "n/a",
                "winrate": f"{(wins / total * 100):.1f}%" if total else "n/a",
                "roi": f"{(float(pnl) / deposit * 100):.1f}%" if total else "n/a",
                "short_long": f"{short_total} / {long_total}"
            }

        # 🔹 Статистика за весь период (всего)
        full_stats = await collect_stats(datetime.min, datetime.max)

        # 🔹 Статистика по дням (последние 10)
        day_stats = []
        for start, end in date_ranges:
            day_stats.append(await collect_stats(start, end))

        # 🔹 Подготовка таблицы статистики по строкам
        stat_rows = [
            ("Всего сделок", [full_stats["total"]] + [d["total"] for d in day_stats]),
            ("Шорт / Лонг", [full_stats["short_long"]] + [d["short_long"] for d in day_stats]),
            ("Шорт winrate", [full_stats["short_winrate"]] + [d["short_winrate"] for d in day_stats]),
            ("Лонг winrate", [full_stats["long_winrate"]] + [d["long_winrate"] for d in day_stats]),
            ("Winrate", [full_stats["winrate"]] + [d["winrate"] for d in day_stats]),
            ("ROI", [full_stats["roi"]] + [d["roi"] for d in day_stats]),
        ]

        # 🔹 Остальное без изменений
        open_positions = await conn.fetch("""
            SELECT id, symbol, direction, created_at, entry_price, close_reason, pnl
            FROM positions_v2
            WHERE strategy_id = $1 AND status = 'open'
            ORDER BY created_at ASC
        """, strategy_id)

        position_ids = tuple(p["id"] for p in open_positions)
        tp_by_position = {}
        sl_by_position = {}

        if position_ids:
            targets = await conn.fetch(f"""
                SELECT position_id, type, level, price
                FROM position_targets_v2
                WHERE position_id = ANY($1::int[])
                  AND hit = false AND canceled = false
            """, position_ids)

            from collections import defaultdict
            tp_map = defaultdict(list)
            sl_map = {}
            for row in targets:
                pid = row["position_id"]
                if row["type"] == "tp":
                    tp_map[pid].append((row["level"], row["price"]))
                elif row["type"] == "sl":
                    sl_map[pid] = row["price"]

            for pid, levels in tp_map.items():
                if levels:
                    min_level = min(levels, key=lambda x: x[0])
                    tp_by_position[pid] = min_level[1]
            sl_by_position = sl_map

        limit = 20
        offset = (page - 1) * limit

        total_closed = await conn.fetchval("""
            SELECT COUNT(*) FROM positions_v2
            WHERE strategy_id = $1 AND status = 'closed'
        """, strategy_id)

        total_pages = (total_closed + limit - 1) // limit

        closed_positions = await conn.fetch("""
            SELECT id, symbol, entry_price, exit_price, close_reason, closed_at, pnl
            FROM positions_v2
            WHERE strategy_id = $1 AND status = 'closed'
            ORDER BY closed_at DESC
            LIMIT $2 OFFSET $3
        """, strategy_id, limit, offset)

        return templates.TemplateResponse("strategy_detail.html", {
            "request": request,
            "strategy": strategy,
            "period": period,
            "page": page,
            "total_pages": total_pages,
            "timezone": ZoneInfo("Europe/Kyiv"),
            "open_positions": open_positions,
            "closed_positions": closed_positions,
            "tp_by_position": tp_by_position,
            "sl_by_position": sl_by_position,
            "stat_rows": stat_rows,
            "stat_dates": date_labels,
            "stats": full_stats
        })
