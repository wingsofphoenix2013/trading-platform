import asyncio
import asyncpg
import os
import redis.asyncio as redis_lib
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

# --- Подключение к Redis ---
def get_redis():
    return redis_lib.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD"),
        ssl=True
    )

# --- Подключение к PostgreSQL ---
async def get_pg():
    db_url = os.getenv("DATABASE_URL")
    return await asyncpg.connect(db_url)

# --- Обработка позиций ---
async def check_positions():
    print("[WORKER] Проверка активных позиций...", flush=True)
    pg = await get_pg()
    r = get_redis()

    try:
        rows = await pg.fetch("""
            SELECT p.id, p.symbol, p.direction, p.entry_price, p.quantity_left,
                   t.precision_price
            FROM positions p
            JOIN tickers t ON p.symbol = t.symbol
            WHERE p.status = 'open'
        """)
        print(f"[WORKER] Найдено позиций для проверки: {len(rows)}", flush=True)

        for row in rows:
            symbol = row["symbol"]
            position_id = row["id"]
            direction = row["direction"]
            entry_price = Decimal(row["entry_price"])
            qty_left = Decimal(row["quantity_left"])
            precision_price = int(row["precision_price"])

            price_raw = await r.get(f"price:{symbol}")
            if not price_raw:
                print(f"[WORKER] Нет цены в Redis для {symbol}", flush=True)
                continue

            price_str = price_raw.decode("utf-8")
            current_price = Decimal(price_str).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)
            print(f"[WORKER] {symbol}: current={current_price}, direction={direction}", flush=True)

            # --- Получение целей позиции ---
            targets = await pg.fetch("""
                SELECT id, type, level, price, quantity
                FROM position_targets
                WHERE position_id = $1 AND hit = false AND canceled = false
            """, position_id)

            print(f"[WORKER] Целей для позиции {position_id}: {len(targets)}", flush=True)
            for t in targets:
                print(f"[WORKER] → {t['type']} L{t['level'] or '-'} @ {t['price']} qty={t['quantity']}", flush=True)
                
            # TODO: здесь будет логика проверки SL/TP

    except Exception as e:
        print(f"[WORKER] Ошибка при проверке позиций: {e}", flush=True)
    finally:
        await pg.close()

# --- Главный цикл ---
async def main_loop():
    while True:
        await check_positions()
        await asyncio.sleep(5)

if __name__ == "__main__":
    print("[WORKER] Запуск worker_main.py", flush=True)
    asyncio.run(main_loop())