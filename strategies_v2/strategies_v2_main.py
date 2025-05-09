import asyncio
import logging
import redis.asyncio as redis
import os
import sys
import asyncpg
import json
from decimal import Decimal, ROUND_DOWN
from strategy_interface import StrategyInterface
from strategy_5 import Strategy5
from strategy_5_1 import Strategy5_1
from strategy_5_2 import Strategy5_2
from strategy_5_3 import Strategy5_3
from strategy_5_4 import Strategy5_4
from strategy_5_5 import Strategy5_5
from strategy_6 import Strategy6
from strategy_6_1 import Strategy6_1
from strategy_6_2 import Strategy6_2
from strategy_6_3 import Strategy6_3
from strategy_6_4 import Strategy6_4
from strategy_6_5 import Strategy6_5

# --- Глобальный флаг отладки ---
debug = os.getenv("DEBUG_MODE", "0") == "1"

# --- Конфигурация Базы Данных ---
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# Хранилище тикеров (глобальное для всего приложения)
tickers_storage = {}
# Хранилище открытых позиций (глобальное)
open_positions = {}
# Импорт и регистрация стратегий
strategy_interface = StrategyInterface(DATABASE_URL, open_positions=open_positions)

strategies = {
    "test-5": Strategy5(strategy_interface, debug=debug),
    "test-5_1": Strategy5_1(strategy_interface, debug=debug),
    "test-5_2": Strategy5_2(strategy_interface, debug=debug),
    "test-5_3": Strategy5_3(strategy_interface, debug=debug),
    "test-5_4": Strategy5_4(strategy_interface, debug=debug),
    "test-5_5": Strategy5_5(strategy_interface, debug=debug),
    "test-6": Strategy6(strategy_interface, debug=debug),
    "test-6_1": Strategy6_1(strategy_interface, debug=debug),
    "test-6_2": Strategy6_2(strategy_interface, debug=debug),
    "test-6_3": Strategy6_3(strategy_interface, debug=debug),
    "test-6_4": Strategy6_4(strategy_interface, debug=debug),
    "test-6_5": Strategy6_5(strategy_interface, debug=debug),
}

# Настройка логирования с немедленным flush в stdout
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
# --- Загрузка новых позиций из базы в память (с обновлением целей) ---
async def load_open_positions(redis_client):
    logging.info("Загрузка новых открытых позиций из базы данных...")
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        query_positions = """
        SELECT p.id, p.symbol, p.direction, p.entry_price, p.quantity_left,
               p.strategy_id, s.name AS strategy_name
        FROM positions p
        JOIN strategies s ON p.strategy_id = s.id
        WHERE p.status = 'open'
        """
        rows = await conn.fetch(query_positions)

        fresh_position_ids = set()

        for row in rows:
            position_id = row['id']
            fresh_position_ids.add(position_id)

            symbol = row['symbol']
            direction = row['direction']
            entry_price = Decimal(row['entry_price'])
            quantity_left = Decimal(row['quantity_left'])
            strategy_id = row['strategy_id']
            strategy_name = row['strategy_name']

            query_targets = """
            SELECT id, type, price, quantity, level, hit
            FROM position_targets
            WHERE position_id = $1 AND canceled = false
            """
            targets_rows = await conn.fetch(query_targets, position_id)

            targets = []
            for target_row in targets_rows:
                targets.append({
                    "id": target_row['id'],
                    "type": target_row['type'].lower(),  # нормализуем
                    "price": Decimal(target_row['price']),
                    "quantity": Decimal(target_row['quantity']),
                    "level": target_row['level'],
                    "hit": target_row['hit']
                })

            if position_id in open_positions:
                open_positions[position_id]["targets"] = targets
                logging.info(f"Обновлены цели позиции ID={position_id}.")
            else:
                open_positions[position_id] = {
                    "symbol": symbol,
                    "direction": direction,
                    "entry_price": entry_price,
                    "quantity_left": quantity_left,
                    "strategy_id": strategy_id,
                    "strategy_name": strategy_name,
                    "targets": targets
                }

        # Очистка позиций, которых больше нет в БД
        to_delete = [pid for pid in open_positions.keys() if pid not in fresh_position_ids]
        for pid in to_delete:
            del open_positions[pid]
            logging.info(f"Удалена позиция ID={pid} из памяти (отсутствует в базе).")

        logging.info("Синхронизация новых позиций завершена.")
    except Exception as e:
        logging.error(f"Ошибка при загрузке открытых позиций: {e}")
    finally:
        await conn.close()
# --- Периодическая синхронизация open_positions с базой ---
async def sync_open_positions_periodically(redis_client, interval_seconds=60):
    while True:
        try:
            await load_open_positions(redis_client)
            logging.info("Синхронизация open_positions с базой завершена.")
        except Exception as e:
            logging.error(f"Ошибка при синхронизации open_positions: {e}")
        await asyncio.sleep(interval_seconds)        
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
                # logging.info(f"Позиция ID={position_id}, символ={symbol}, направление={direction}, цена={current_price}") - ОТКЛЮЧЕНО
                # logging.info(f"Цели позиции ID={position_id}: {data['targets']}") - ОТКЛЮЧЕНО

                # --- Проверка срабатывания TP уровней ---
                for target in data["targets"]:
                    if target["type"] == "tp" and not target.get("hit", False) and not target.get("canceled", False):
                        tp_price = target["price"]

                        if (direction == "long" and current_price >= tp_price) or \
                           (direction == "short" and current_price <= tp_price):
                            logging.info(f"Срабатывание TP уровня {target['level']} по позиции ID={position_id} на цене {current_price}")

                            # Помечаем TP как выполненный
                            await strategy_interface.mark_target_hit(target["id"])

                            # Пересчёт позиции: уменьшение количества
                            await strategy_interface.reduce_position_quantity(position_id, target["quantity"], current_price)

                            # Обновляем данные в памяти
                            data["quantity_left"] -= target["quantity"]

                            # Отмена старого SL
                            await strategy_interface.cancel_all_targets(position_id, sl_only=True)

                            entry_price = data["entry_price"]
                            strategy_name = data["strategy_name"]

                            # Получаем ATR (если потребуется стратегии)
                            ema, atr = await strategy_interface.get_ema_atr(symbol, timeframe="M1")

                            # Запрашиваем у стратегии цену нового SL после срабатывания TP
                            sl_price = strategies[strategy_name].get_sl_after_tp(
                                level=target["level"],
                                entry_price=entry_price,
                                atr=atr,
                                direction=direction
                            )

                            if sl_price is not None:
                                await strategy_interface.create_new_sl(position_id, sl_price, data["quantity_left"])
                                logging.info(f"Создан новый SL на уровне {sl_price} после TP уровня {target['level']} для позиции ID={position_id}")
                            else:
                                logging.info(f"После TP уровня {target['level']} стратегия не требует постановки нового SL (позиция ID={position_id})")

                            # Пометка TP как исполненного в памяти
                            for t in data["targets"]:
                                if t["id"] == target["id"]:
                                    t["hit"] = True
                                    t["hit_at"] = True

                            logging.info(f"Позиция ID={position_id} после TP: новый остаток {data['quantity_left']}")

                            # --- Закрытие позиции, если всё исполнено ---
                            if data["quantity_left"] <= Decimal("0.00000001"):
                                await strategy_interface.cancel_all_targets(position_id)
                                await strategy_interface.close_position(position_id, current_price, "tp-full")
                                del open_positions[position_id]
                                logging.info(f"Позиция ID={position_id} закрыта автоматически при quantity_left = 0.")
                                break

                            break  # После одного TP прекращаем обработку этой позиции в этом цикле

                # --- Проверка срабатывания SL ---
                for target in data["targets"]:
                    if target["type"] == "sl":
                        sl_price = target["price"]

                        if (direction == "long" and current_price <= sl_price) or \
                           (direction == "short" and current_price >= sl_price):
                            logging.info(f"Срабатывание SL по позиции ID={position_id} на цене {current_price}")

                            await strategy_interface.mark_target_hit(target["id"])
                            await strategy_interface.cancel_all_targets(position_id)

                            # Определяем: был ли до этого выполнен хотя бы один TP
                            tp_hit_found = any(
                                t["type"] == "tp" and t.get("hit", False)
                                for t in data["targets"]
                            )
                            close_reason = "sl-tp-hit" if tp_hit_found else "sl"

                            await strategy_interface.close_position(position_id, current_price, close_reason)

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
        follow_positions(redis_client, open_positions),
        sync_open_positions_periodically(redis_client)  # новый синк
    )

# Запуск основного цикла
if __name__ == "__main__":
    asyncio.run(main_loop())