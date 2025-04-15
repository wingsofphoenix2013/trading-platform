# web_main.py — интерфейс и управление тикерами (обновлено с ticker_count)

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import asyncpg
import redis.asyncio as redis
import os

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