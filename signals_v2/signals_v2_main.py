import os
import asyncio
import logging
import redis.asyncio as redis
import asyncpg
from datetime import datetime
from dateutil import parser

# 🔸 Настройка логирования
logging.basicConfig(level=logging.INFO)

# 🔸 Переменные окружения
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL")

# 🔸 Redis клиент
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
    ssl=True
)

# 🔸 Глобальный словарь тикеров: symbol → tradepermission
TICKERS = {}

# 🔸 Подключение к PostgreSQL
async def get_db():
    return await asyncpg.connect(DATABASE_URL)
# 🔸 Запись события или ошибки в таблицу system_logs
async def log_system_event(level, message, source, details=None, action_flag=None):
    try:
        conn = await get_db()
        await conn.execute("""
            INSERT INTO system_logs (level, message, source, details, action_flag, created_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
        """, level, message, source, details, action_flag)
    except Exception as e:
        logging.error(f"❌ Не удалось записать лог в system_logs: {e}")
    finally:
        await conn.close()
# 🔸 Загрузка тикеров из БД (status = enabled)
async def load_tickers():
    global TICKERS
    try:
        conn = await get_db()
        rows = await conn.fetch("SELECT symbol, tradepermission FROM tickers WHERE status = 'enabled'")
        TICKERS = {row["symbol"]: row["tradepermission"] for row in rows}
        logging.info(f"✅ Загрузка тикеров: {len(TICKERS)} шт.")
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке тикеров: {e}")
    finally:
        await conn.close()

# 🔸 Фоновое обновление тикеров каждые 5 минут
async def refresh_tickers_periodically():
    while True:
        await load_tickers()
        await asyncio.sleep(300)
# 🔸 Обработка одного сигнала из Redis Stream
async def process_signal(entry_id, data):
    logging.info(f"📥 Обработка сигнала: {data}")

    # 🔹 Распаковка данных
    message = data.get("message")
    symbol_raw = data.get("symbol")
    bar_time = data.get("bar_time")
    sent_at = data.get("sent_at")
    received_at = data.get("received_at")
    raw_message = str(data)

    # 🔹 Базовая валидация
    if not message or not symbol_raw:
        await log_system_event(
            level="WARNING",
            message="Сигнал без message или symbol — пропущен",
            source="signal_worker",
            details=raw_message
        )
        return

    symbol = symbol_raw.strip().upper()

    # 🔹 Проверка тикера (по кешу)
    if symbol not in TICKERS or TICKERS[symbol] != "enabled":
        await log_system_event(
            level="WARNING",
            message=f"Тикер {symbol} не разрешён к торговле",
            source="signal_worker",
            details=raw_message
        )
        return

    # 🔹 Преобразование временных полей
    from dateutil import parser
    try:
        bar_time = parser.isoparse(bar_time).replace(tzinfo=None) if bar_time else None
        sent_at = parser.isoparse(sent_at).replace(tzinfo=None) if sent_at else None
        received_at = parser.isoparse(received_at).replace(tzinfo=None) if received_at else datetime.utcnow()
    except Exception as e:
        await log_system_event(
            level="ERROR",
            message="Ошибка преобразования дат",
            source="signal_worker",
            details=str(e)
        )
        return

    conn = await get_db()
    try:
        # 🔹 Поиск сигнала по фразе
        signal_row = await conn.fetchrow("""
            SELECT * FROM signals_v2
            WHERE (long_phrase = $1 OR short_phrase = $1)
              AND enabled = true
        """, message)
        if not signal_row:
            await log_system_event(
                level="WARNING",
                message=f"Фраза '{message}' не зарегистрирована в signals_v2",
                source="signal_worker",
                details=raw_message
            )
            return

        signal_id = signal_row["id"]
        source = signal_row["source"]
        direction = None
        if message == signal_row["long_phrase"]:
            direction = "long"
        elif message == signal_row["short_phrase"]:
            direction = "short"

        # 🔹 UID сигнала (message + symbol + bar_time)
        uid = f"{message}:{symbol}:{bar_time.isoformat()}"
        exists = await conn.fetchval("SELECT id FROM signals_v2_log WHERE uid = $1", uid)
        if exists:
            await log_system_event(
                level="INFO",
                message=f"Повтор сигнала — uid {uid}",
                source="signal_worker"
            )
            return

        # 🔹 Вставка в signals_v2_log
        log_id = await conn.fetchval("""
            INSERT INTO signals_v2_log (
                signal_id, symbol, direction, source, message,
                raw_message, bar_time, sent_at, received_at,
                logged_at, status, uid
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW(),'new',$10)
            RETURNING id
        """, signal_id, symbol, direction, source, message,
             raw_message, bar_time, sent_at, received_at, uid)

        logging.info(f"✅ Сигнал записан в signals_v2_log (id={log_id})")

    except Exception as e:
        logging.error(f"❌ Исключение в process_signal: {e}")
        await log_system_event(
            level="ERROR",
            message="Ошибка при обработке сигнала",
            source="signal_worker",
            details=str(e)
        )
    finally:
        await conn.close()
# 🔸 Цикл чтения сигналов из Redis Stream
async def listen_signals():
    logging.info("🚀 Signal Worker (v2) запущен. Ожидание сигналов...")
    group = "workers"
    consumer = f"consumer-{os.getpid()}"

    try:
        await redis_client.xgroup_create("signals_stream", group, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info("ℹ️ Группа уже существует. Продолжаем.")
        else:
            raise

    while True:
        result = await redis_client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={"signals_stream": ">"},
            count=10,
            block=500
        )

        if result:
            for stream_name, messages in result:
                for entry_id, data in messages:
                    await process_signal(entry_id, data)
                    await redis_client.xack("signals_stream", group, entry_id)

# 🔸 Главная точка запуска: загрузка тикеров + запуск слушателя сигналов
async def main():
    await load_tickers()
    asyncio.create_task(refresh_tickers_periodically())
    await listen_signals()

# 🔸 Точка входа
if __name__ == "__main__":
    asyncio.run(main())