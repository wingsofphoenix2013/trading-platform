# indicators_main.py — расчёт технических индикаторов (каркас с логами)

print("🚀 INDICATORS WORKER STARTED", flush=True)

# === Импорты ===
import asyncio
import os
import asyncpg
import redis.asyncio as redis

# === Подключение к Redis ===
print("[INIT] Connecting to Redis...", flush=True)
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    ssl=True
)
print("[OK] Connected to Redis", flush=True)

# === Подключение к PostgreSQL ===
async def get_enabled_tickers():
    print("[DB] Fetching enabled tickers...", flush=True)
    db_url = os.getenv("DATABASE_URL")
    try:
        conn = await asyncpg.connect(dsn=db_url)
        rows = await conn.fetch("SELECT symbol FROM tickers WHERE status = 'enabled'")
        await conn.close()
        symbols = [row["symbol"] for row in rows]
        print(f"[DB] {len(symbols)} tickers fetched: {symbols}", flush=True)
        return symbols
    except Exception as e:
        print(f"[ERROR] DB connection failed: {e}", flush=True)
        return []

# === Основной цикл воркера ===
async def main():
    print("[INIT] Starting indicators loop", flush=True)
    while True:
        tickers = await get_enabled_tickers()
        print("[INFO] Starting calculation cycle (placeholder)", flush=True)

        # Здесь в будущем будет расчёт индикаторов для каждого тикера
        for symbol in tickers:
            print(f"[DEBUG] Would process {symbol} (no logic yet)", flush=True)

        await asyncio.sleep(300)  # Пауза 5 минут

# === Запуск ===
if __name__ == "__main__":
    asyncio.run(main())
