import asyncpg
import logging
import redis.asyncio as redis
from decimal import Decimal, ROUND_DOWN
import os


# 🔸 Универсальный интерфейс для стратегий v3
class StrategyInterface:
    def __init__(
        self,
        redis_client,
        database_url,
        strategies_cache,
        strategy_allowed_tickers,
        open_positions,
        tickers_storage
    ):
        # 🔸 Подключение к Redis и база данных
        self.redis = redis_client
        self.database_url = database_url

        # 🔸 Хранилища в памяти
        self.strategies_cache = strategies_cache
        self.strategy_allowed_tickers = strategy_allowed_tickers
        self.open_positions = open_positions
        self.tickers_storage = tickers_storage

    # 🔸 Логирование действия стратегии в signal_log_entries_v2
    async def log_strategy_action(self, strategy_id: int, log_id: int, status: str, note: str, position_id: int = None):
        try:
            conn = await asyncpg.connect(self.database_url)
            await conn.execute("""
                INSERT INTO signal_log_entries_v2 (strategy_id, log_id, status, note, position_id, logged_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
            """, strategy_id, log_id, status, note, position_id)
            await conn.close()

            logging.info(f"📝 Запись в лог: strategy_id={strategy_id}, log_id={log_id}, status={status}")
        except Exception as e:
            logging.error(f"❌ Ошибка при записи в signal_log_entries_v2: {e}")

    # 🔸 Базовые проверки перед открытием позиции
    async def run_basic_checks(self, task: dict) -> tuple[bool, str]:
        strategy_name = task.get("strategy")
        symbol = task.get("symbol")
        direction = task.get("direction")

        # 🔹 Найти стратегию
        strategy_id = None
        for sid, data in self.strategies_cache.items():
            if data["name"] == strategy_name:
                strategy_id = sid
                strategy = data
                break

        if strategy_id is None:
            return False, "Базовые проверки не пройдены — стратегия не найдена"

        # 🔹 Проверка разрешённого тикера
        allowed = self.strategy_allowed_tickers.get(strategy_id, set())
        if symbol not in allowed:
            return False, "Базовые проверки не пройдены — тикер не разрешён для этой стратегии"

        # 🔹 Поиск открытой позиции
        for pos in self.open_positions.values():
            if pos["strategy_id"] == strategy_id and pos["symbol"] == symbol:
                if pos["direction"] == direction:
                    return False, "Базовые проверки не пройдены — позиция в этом направлении уже открыта"
                else:
                    if not strategy.get("reverse", False):
                        return False, "Базовые проверки не пройдены — противоположная позиция уже открыта, реверс запрещён"
                    else:
                        return True, "Разрешён реверс — дальнейшие действия определяются стратегией"

        return True, "Базовые проверки пройдены"

    # 🔸 Поиск ID стратегии по имени
    async def get_strategy_id_by_name(self, strategy_name: str) -> int:
        for sid, data in self.strategies_cache.items():
            if data["name"] == strategy_name:
                return sid
        return None
    # 🔸 Получение значения индикатора из Redis по ключу
    async def get_indicator_value(self, symbol: str, timeframe: str, *path_parts: str) -> Decimal | None:
        try:
            key = f"{symbol}:{timeframe}:" + ":".join(path_parts)
            value = await self.redis.get(key)
            if value is None:
                logging.warning(f"⚠️ Индикатор не найден: {key}")
                return None
            return Decimal(value)
        except Exception as e:
            logging.error(f"❌ Ошибка при получении индикатора {key}: {e}")
            return None
    # 🔸 Открытие позиции: расчёт planned_risk, объёма и запись в positions_v2
    async def open_position(self, task: dict) -> int | None:
        strategy_name = task.get("strategy")
        symbol = task.get("symbol")
        direction = task.get("direction")
        log_id = int(task.get("log_id"))
        timeframe = task.get("timeframe", "M1")

        strategy_id = await self.get_strategy_id_by_name(strategy_name)
        if strategy_id is None:
            logging.error("❌ Стратегия не найдена для открытия позиции")
            return None

        strategy = self.strategies_cache.get(strategy_id)
        ticker = self.tickers_storage.get(symbol)
        entry_price = latest_prices.get(symbol)

        if strategy is None or ticker is None or entry_price is None:
            logging.warning("⚠️ Не найдены данные стратегии, тикера или цены")
            return None

        precision_price = ticker["precision_price"]
        precision_qty = ticker["precision_qty"]

        # 🔹 Расчёт SL-цены
        sl_type = strategy["sl_type"]
        sl_value = Decimal(str(strategy["sl_value"]))

        if sl_type == "percent":
            sl_percent = sl_value / Decimal("100")
            delta = entry_price * sl_percent
        elif sl_type == "atr":
            atr = await self.get_indicator_value(symbol, timeframe, "ATR", "atr")
            if atr is None:
                logging.warning(f"⚠️ Не удалось получить ATR для {symbol}")
                return None
            delta = atr
        else:
            logging.error(f"❌ Неизвестный тип SL: {sl_type}")
            return None

        if direction == "long":
            stop_loss_price = entry_price - delta
        elif direction == "short":
            stop_loss_price = entry_price + delta
        else:
            logging.error("❌ Неверное направление позиции")
            return None

        stop_loss_price = stop_loss_price.quantize(Decimal(f"1e-{precision_price}"), rounding=ROUND_DOWN)

        # 🔹 Максимальный допустимый planned_risk
        deposit = Decimal(str(strategy["deposit"]))
        max_risk_pct = Decimal(str(strategy["max_risk"])) / Decimal("100")
        max_allowed_risk = deposit * max_risk_pct

        # 🔹 Расчёт уже занятого риска
        current_risk = sum(
            Decimal(str(p.get("planned_risk", 0)))
            for p in self.open_positions.values()
            if p["strategy_id"] == strategy_id
        )

        available_risk = max_allowed_risk - current_risk
        if available_risk <= 0:
            logging.warning("⚠️ Превышен лимит риска по стратегии")
            return None

        risk_per_unit = abs(entry_price - stop_loss_price)
        if risk_per_unit == 0:
            logging.warning("⚠️ SL слишком близко к цене, риск не определён")
            return None

        quantity = (available_risk / risk_per_unit).quantize(Decimal(f"1e-{precision_qty}"), rounding=ROUND_DOWN)
        notional = (quantity * entry_price).quantize(Decimal(f"1e-{precision_price}"), rounding=ROUND_DOWN)

        # 🔹 Проверка по лимиту позиции
        position_limit = Decimal(str(strategy["position_limit"]))
        if (quantity * entry_price) > position_limit:
            notional = position_limit
            quantity = (notional / entry_price).quantize(Decimal(f"1e-{precision_qty}"), rounding=ROUND_DOWN)

        planned_risk = (quantity * risk_per_unit).quantize(Decimal("1e-8"), rounding=ROUND_DOWN)

        # 🔹 Запись в positions_v2
        try:
            conn = await asyncpg.connect(self.database_url)
            row = await conn.fetchrow("""
                INSERT INTO positions_v2 (
                    strategy_id, log_id, symbol, direction, entry_price,
                    quantity, notional_value, quantity_left, status,
                    created_at, planned_risk
                ) VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $6, 'open',
                    NOW(), $8
                ) RETURNING id
            """, strategy_id, log_id, symbol, direction, entry_price,
                 quantity, notional, planned_risk)
            await conn.close()

            position_id = row["id"]
            logging.info(f"📌 Позиция открыта: ID={position_id}, {symbol}, {direction}, qty={quantity}, risk={planned_risk}")
            return position_id

        except Exception as e:
            logging.error(f"❌ Ошибка при записи позиции: {e}")
            return None                    