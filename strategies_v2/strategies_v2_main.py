import asyncio
import logging
import redis.asyncio as redis
import os
import sys
import asyncpg
import json
from decimal import Decimal, ROUND_DOWN
from strategy_interface import StrategyInterface
from strategy_1 import Strategy1

# --- Конфигурация Базы Данных ---
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# Импорт и регистрация стратегий

strategy_interface = StrategyInterface(DATABASE_URL)

strategies = {
    "test-1": Strategy1(strategy_interface),
}

# Настройка логирования с немедленным flush в stdout
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
# Хранилище тикеров (глобальное для всего приложения)
tickers_storage = {}
# Хранилище открытых позиций (глобальное)
open_positions = {}
# --- Загрузка открытых позиций из БД при старте ---
async def load_open_positions(redis_client):
    logging.info("Загрузка открытых позиций из базы данных...")
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        open_positions.clear()
        query_positions = """
        SELECT id, symbol, direction, entry_price, quantity_left, strategy_id
        FROM positions
        WHERE status = 'open'
        """
        rows = await conn.fetch(query_positions)

        for row in rows:
            position_id = row['id']
            symbol = row['symbol']
            direction = row['direction']
            entry_price = Decimal(row['entry_price'])
            quantity_left = Decimal(row['quantity_left'])
            strategy_id = row['strategy_id']

            # Загружаем активные цели
            query_targets = """
            SELECT id, type, price, quantity, level
            FROM position_targets
            WHERE position_id = $1 AND hit = false AND canceled = false
            """
            targets_rows = await conn.fetch(query_targets, position_id)

            targets = []
            for target_row in targets_rows:
                targets.append({
                    "id": target_row['id'],
                    "type": target_row['type'],
                    "price": Decimal(target_row['price']),
                    "quantity": Decimal(target_row['quantity']),
                    "level": target_row['level']
                })

            open_positions[position_id] = {
                "symbol": symbol,
                "direction": direction,
                "entry_price": entry_price,
                "quantity_left": quantity_left,
                "strategy_id": strategy_id,
                "targets": targets
            }

        logging.info(f"Открытые позиции загружены: {list(open_positions.keys())}")
    except Exception as e:
        logging.error(f"Ошибка при загрузке открытых позиций: {e}")
    finally:
        await conn.close()
# Асинхронный метод для загрузки актуальных тикеров из БД
async def load_tickers_periodically(strategy_interface, tickers_storage):
    while True:
        new_tickers = await strategy_interface.get_active_tickers()
        if new_tickers:
            tickers_storage.clear()
            tickers_storage.update(new_tickers)
            logging.info(f"Список тикеров обновлён: {list(tickers_storage.keys())}")
        else:
            logging.warning("Не удалось обновить список тикеров.")
        
        await asyncio.sleep(300)  # Повтор каждые 5 минут
# Асинхронный цикл мониторинга текущих цен
async def monitor_prices(redis_client, tickers_storage):
    while True:
        for symbol, params in tickers_storage.items():
            try:
                price_str = await redis_client.get(f'price:{symbol}')
                if price_str:
                    precision = params['precision_price']
                    price = Decimal(price_str).quantize(Decimal(f'1e-{precision}'), rounding=ROUND_DOWN)
                    logging.debug(f"Текущая цена {symbol}: {price}")
                else:
                    logging.warning(f"Цена для {symbol} отсутствует в Redis.")
            except Exception as e:
                logging.error(f"Ошибка при получении цены {symbol} из Redis: {e}")

        await asyncio.sleep(5)
# --- Фоновая задача сопровождения открытых позиций ---
async def follow_positions(redis_client, open_positions):
    logging.info("Запущено сопровождение позиций (follow_positions).")
    while True:
        try:
            for position_id, data in list(open_positions.items()):
                symbol = data["symbol"]
                direction = data["direction"]

                price_str = await redis_client.get(f'price:{symbol}')
                if not price_str:
                    logging.warning(f"Цена для {symbol} отсутствует в Redis.")
                    continue

                current_price = Decimal(price_str)
                logging.info(f"Позиция ID={position_id}, символ={symbol}, направление={direction}, цена={current_price}")

                # --- Проверка срабатывания SL ---
                for target in data["targets"]:
                    if target["type"] == "sl":
                        sl_price = target["price"]

                        if (direction == "long" and current_price <= sl_price) or \
                           (direction == "short" and current_price >= sl_price):
                            logging.info(f"Срабатывание SL по позиции ID={position_id} на цене {current_price}")

                            await strategy_interface.mark_target_hit(target["id"])
                            await strategy_interface.cancel_all_targets(position_id)
                            await strategy_interface.close_position(position_id, current_price, "sl")

                            # Удаляем позицию из памяти
                            del open_positions[position_id]
                            break
        except Exception as e:
            logging.error(f"Ошибка в процессе сопровождения позиций: {e}")
        
        await asyncio.sleep(1)
# Асинхронная функция подписки, парсинга и проверки сигналов по БД
async def listen_signals(redis_client):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe('incoming_signals')
    logging.info("Подписка на канал incoming_signals выполнена.")

    async for message in pubsub.listen():
        if message['type'] == 'message':
            signal_data = message['data']
            logging.info(f"Получен сигнал: {signal_data}")

            # Парсинг сигнала
            try:
                signal_json = json.loads(signal_data)
                signal_text = signal_json.get("message", "")
                source = signal_json.get("source", "")

                phrase, symbol = signal_text.strip().split(" ")
                phrase = phrase.strip().upper()
                symbol = symbol.strip().upper()

                logging.info(f"Парсинг успешен — Фраза: '{phrase}', Тикер: '{symbol}', Источник: '{source}'")

                # Проверка сигнала по базе
                signal_row = await check_signal_in_db(phrase)
                if signal_row:
                    logging.info(f"Сигнал '{phrase}' успешно найден и активен (id={signal_row['id']}).")

                    # Явно определяем направление сигнала
                    if phrase == signal_row['long_phrase']:
                        direction = 'long'
                    elif phrase == signal_row['short_phrase']:
                        direction = 'short'
                    else:
                        logging.error(f"Получена неизвестная фраза сигнала: '{phrase}'")
                        continue  # Пропускаем сигнал, так как не удалось определить направление

                    # Логируем сигнал
                    log_id = await log_signal(
                        signal_id=signal_row['id'],
                        ticker_symbol=symbol,
                        direction=direction,
                        source=source,
                        raw_message=signal_data,
                        status='new'
                    )
                    logging.info(f"Сигнал '{phrase}' залогирован с id={log_id}.")

                    # Проверка связанных стратегий
                    linked_strategies = await get_linked_strategies(signal_row['id'])
                    if linked_strategies:
                        logging.info(f"Найдены активные стратегии для сигнала '{phrase}': {[s['name'] for s in linked_strategies]}")

                        # Запускаем найденные стратегии
                        for strategy in linked_strategies:
                            strategy_name = strategy['name']
                            if strategy_name in strategies:
                                await strategies[strategy_name].on_signal({
                                    'phrase': phrase,
                                    'symbol': symbol,
                                    'log_id': log_id,
                                    'direction': direction
                                })
                                logging.info(f"Стратегия '{strategy_name}' запущена по сигналу '{phrase}' для тикера '{symbol}'.")
                            else:
                                logging.warning(f"Стратегия '{strategy_name}' найдена в БД, но не зарегистрирована в коде.")
                    else:
                        logging.warning(f"Нет активных стратегий для сигнала '{phrase}'.")
                else:
                    logging.warning(f"Сигнал '{phrase}' не найден или неактивен. Игнорируется.")

            except Exception as e:
                logging.error(f"Ошибка обработки сигнала: {e}")
# Функция проверки сигнала в таблице signals
async def check_signal_in_db(phrase):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        query = """
        SELECT id, enabled, long_phrase, short_phrase FROM signals 
        WHERE (long_phrase=$1 OR short_phrase=$1 OR long_exit_phrase=$1 OR short_exit_phrase=$1)
        AND enabled=true
        LIMIT 1
        """
        signal_row = await conn.fetchrow(query, phrase)
        return signal_row
    except Exception as e:
        logging.error(f"Ошибка при запросе к signals: {e}")
        return None
    finally:
        await conn.close()

# Функция проверки связанных стратегий
async def get_linked_strategies(signal_id):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        query = """
        SELECT s.id, s.name FROM strategy_signals ss
        JOIN strategies s ON ss.strategy_id = s.id
        WHERE ss.signal_id = $1 AND s.enabled = true
        """
        strategies = await conn.fetch(query, signal_id)
        return strategies
    except Exception as e:
        logging.error(f"Ошибка при запросе связанных стратегий: {e}")
        return []
    finally:
        await conn.close()
        
# Функция логирования сигнала в таблицу signal_logs
async def log_signal(signal_id, ticker_symbol, direction, source, raw_message, status='new'):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        query = """
        INSERT INTO signal_logs (signal_id, ticker_symbol, direction, source, raw_message, received_at, status)
        VALUES ($1, $2, $3, $4, $5, NOW(), $6)
        RETURNING id
        """
        log_row = await conn.fetchrow(query, signal_id, ticker_symbol, direction, source, raw_message, status)
        return log_row['id']
    except Exception as e:
        logging.error(f"Ошибка логирования сигнала в signal_logs: {e}")
        return None
    finally:
        await conn.close()
                        
# Основной цикл приложения
async def main_loop():
    logging.info("strategies_v2_main.py успешно запустился.")

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
        ssl=True
    )
    await load_open_positions(redis_client)
    
    # Параллельный запуск мониторинга цен, подписки на сигналы и загрузки тикеров
    await asyncio.gather(
        monitor_prices(redis_client, tickers_storage),
        listen_signals(redis_client),
        load_tickers_periodically(strategy_interface, tickers_storage),
        follow_positions(redis_client, open_positions)  # просто указываем функцию
    )

# Запуск основного цикла
if __name__ == "__main__":
    asyncio.run(main_loop())