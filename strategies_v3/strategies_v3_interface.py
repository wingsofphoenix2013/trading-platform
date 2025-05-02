# 🔸 Импорты и базовая настройка
import os
import asyncpg
import redis.asyncio as redis
from decimal import Decimal, ROUND_DOWN

# 🔸 Интерфейс стратегий v3
class StrategyInterface:
    def __init__(self):
        self.pg_dsn = os.getenv("DATABASE_URL")
        self._pg_pool = None
        self._redis = None

    # 🔸 Получение подключения к Redis (Upstash / локально)
    async def get_redis(self):
        if not self._redis:
            host = os.getenv("REDIS_HOST")
            port = int(os.getenv("REDIS_PORT", "6379"))
            password = os.getenv("REDIS_PASSWORD")
            self._redis = redis.Redis(
                host=host,
                port=port,
                password=password,
                decode_responses=True,
                ssl=True
            )
        return self._redis

    # 🔸 Получение пула PostgreSQL
    async def get_pg(self):
        if not self._pg_pool:
            self._pg_pool = await asyncpg.create_pool(dsn=self.pg_dsn, min_size=1, max_size=5)
        return self._pg_pool
        
    # 🔸 Загрузка тикеров с precision
    async def load_tickers(self):
        pg = await self.get_pg()
        query = """
        SELECT symbol, precision_price, precision_qty, min_qty, tradepermission
        FROM tickers
        WHERE status = 'enabled'
        """
        rows = await pg.fetch(query)
        return {
            row["symbol"]: {
                "precision_price": row["precision_price"],
                "precision_qty": row["precision_qty"],
                "min_qty": row["min_qty"],
                "tradepermission": row["tradepermission"],
            }
            for row in rows
        }

    # 🔸 Получение параметров стратегии
    async def get_strategy_params(self, strategy_name: str):
        pg = await self.get_pg()
        row = await pg.fetchrow("""
            SELECT * FROM strategies_v2 WHERE name = $1 AND enabled = true AND archived = false
        """, strategy_name)
        return dict(row) if row else None
      
    # 🔸 Логирование действия стратегии
    async def log_strategy_action(self, *, log_id: int, strategy_id: int, status: str, position_id: int = None, note: str = None):
        pg = await self.get_pg()
        await pg.execute("""
            INSERT INTO signal_log_entries_v2 (log_id, strategy_id, status, position_id, note)
            VALUES ($1, $2, $3, $4, $5)
        """, log_id, strategy_id, status, position_id, note)
        
    # 🔸 Исключение тикера из стратегии (моментально + в БД)
    async def disable_symbol_for_strategy(self, strategy_name: str, symbol: str):
        pg = await self.get_pg()

        # 🔹 Получение ID стратегии
        strategy_row = await pg.fetchrow(
            "SELECT id FROM strategies_v2 WHERE name = $1", strategy_name
        )
        if not strategy_row:
            raise ValueError(f"Стратегия не найдена: {strategy_name}")
        strategy_id = strategy_row["id"]

        # 🔹 Получение ID тикера
        ticker_row = await pg.fetchrow(
            "SELECT id FROM tickers WHERE symbol = $1", symbol
        )
        if not ticker_row:
            raise ValueError(f"Тикер не найден: {symbol}")
        ticker_id = ticker_row["id"]

        # 🔹 Обновление strategy_tickers_v2
        await pg.execute("""
            UPDATE strategy_tickers_v2
            SET enabled = false
            WHERE strategy_id = $1 AND ticker_id = $2
        """, strategy_id, ticker_id)

        # 🔹 Моментальное исключение из памяти
        from strategies_v3_main import allowed_symbols
        allowed_symbols.get(strategy_name, set()).discard(symbol)
                
    # 🔸 Открытие позиции: расчёт объёма и запись в БД
    async def open_position(self, strategy_name: str, symbol: str, direction: str, entry_price: float, log_id: int) -> int:
        from strategies_v3_main import tickers_storage, open_positions, strategies_cache

        pg = await self.get_pg()

        # 🔹 Получение параметров стратегии
        strategy = strategies_cache[strategy_name]
        leverage = strategy["leverage"]
        deposit = Decimal(strategy["deposit"])
        position_limit = Decimal(strategy["position_limit"])
        max_risk_pct = Decimal(strategy["max_risk"])
        sl_type = strategy["sl_type"]
        sl_value = Decimal(strategy["sl_value"])

        # 🔹 Получение precision по тикеру
        ticker = tickers_storage[symbol]
        precision_price = ticker["precision_price"]
        precision_qty = ticker["precision_qty"]

        # 🔹 Округление цены
        entry_price = Decimal(entry_price).quantize(Decimal(f"1e-{precision_price}"))

        # 🔹 Расчёт SL-цены
        if sl_type == "percent":
            if direction == "long":
                sl_price = entry_price * (Decimal("1") - sl_value / Decimal("100"))
            else:
                sl_price = entry_price * (Decimal("1") + sl_value / Decimal("100"))
            sl_price = sl_price.quantize(Decimal(f"1e-{precision_price}"))
        elif sl_type == "atr":
            raise NotImplementedError("SL по ATR пока не реализован")
        else:
            raise ValueError(f"Неизвестный тип SL: {sl_type}")

        delta = abs(entry_price - sl_price)
        if delta == 0:
            raise ValueError("SL-уровень совпадает с входом — невозможный риск")

        # 🔹 Расчёт максимально допустимого риска
        max_risk_abs = deposit * max_risk_pct / Decimal("100")

        # 🔹 Расчёт уже занятого риска
        total_existing_risk = sum(
            pos["planned_risk"] for pos in open_positions.values()
            if pos["strategy"] == strategy_name
        )
        remaining_risk = max_risk_abs - total_existing_risk
        if remaining_risk <= 0:
            raise ValueError("Превышен лимит риска по стратегии")

        # 🔹 Расчёт максимально возможного объёма
        quantity = remaining_risk / delta

        # 🔹 Ограничение по position_limit (маржа)
        max_quantity_by_limit = position_limit / entry_price
        quantity = min(quantity, max_quantity_by_limit)

        # 🔹 Округление объёма
        quantity = quantity.quantize(Decimal(f"1e-{precision_qty}"), rounding=ROUND_DOWN)

        # 🔹 Финальные значения
        notional_value = (quantity * entry_price).quantize(Decimal("1.0000"))
        planned_risk = (delta * quantity).quantize(Decimal("1.0000"))

        # 🔹 Комиссия
        commission_rate = Decimal("0.001")
        commission = (notional_value * commission_rate).quantize(Decimal("1.0000"))
        pnl = -commission

        # 🔹 Получение strategy_id
        strategy_id = strategy["id"]

        # 🔹 Вставка в таблицу позиций
        result = await pg.fetchrow("""
            INSERT INTO positions_v2 (
                strategy_id, log_id, symbol, direction,
                entry_price, quantity, notional_value,
                quantity_left, status, created_at,
                planned_risk, pnl
            ) VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,
                $6, 'open', NOW(),
                $8, $9
            )
            RETURNING id
        """, strategy_id, log_id, symbol, direction,
             entry_price, quantity, notional_value,
             planned_risk, pnl)

        position_id = result["id"]

        # 🔹 Сохранение в память
        open_positions[(strategy_name, symbol)] = {
            "position_id": position_id,
            "strategy": strategy_name,
            "symbol": symbol,
            "direction": direction,
            "entry_price": entry_price,
            "quantity": quantity,
            "quantity_left": quantity,
            "planned_risk": planned_risk,
            "pnl": pnl
        }

        return position_id