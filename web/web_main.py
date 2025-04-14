# web/main.py — интерфейс и управление тикерами

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

# 2. Главная страница (заглушка)
@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("base.html", {"request": request})

# 3. Список тикеров
@app.get("/tickers", response_class=HTMLResponse)
async def list_tickers(request: Request):
    conn = await get_db()
    rows = await conn.fetch("SELECT * FROM tickers ORDER BY created_at DESC")
    await conn.close()
    return templates.TemplateResponse("tickers.html", {"request": request, "tickers": rows})

# 4. Отображение формы создания тикера (popup)
@app.get("/tickers/new", response_class=HTMLResponse)
async def new_ticker_form(request: Request):
    return templates.TemplateResponse("ticker_form.html", {"request": request})

# 5. Детали тикера
@app.get("/tickers/{symbol}", response_class=HTMLResponse)
async def ticker_detail(symbol: str, request: Request):
    conn = await get_db()
    row = await conn.fetchrow("SELECT * FROM tickers WHERE symbol = $1", symbol.upper())
    await conn.close()
    if row:
        return templates.TemplateResponse("ticker_detail.html", {"request": request, "ticker": row, "strategies": []})
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
