import logging
from decimal import Decimal, ROUND_DOWN
from debug_utils import debug_log

# 🔸 Стратегия strategy_3 с проверкой EMA50 и ATR
class Strategy3:
    def __init__(self):
        pass

    # 🔸 Обработка сигнала с учётом специфических условий
    async def on_signal(self, task: dict, interface):
        debug_log("📈 Продолжение логики внутри strategy_3...")

        symbol = task["symbol"]
        direction = task["direction"]
        timeframe = task.get("timeframe", "M1")
        entry_price = interface.latest_prices.get(symbol)

        if entry_price is None:
            logging.warning(f"⚠️ Нет актуальной цены для {symbol}")
            return

        # 🔹 Получение EMA50, ATR и median(30)
        ema_50 = await interface.get_indicator_value(symbol, timeframe, "EMA", "50")
        atr = await interface.get_indicator_value(symbol, timeframe, "ATR", "atr")
        atr_median = await interface.get_indicator_value(symbol, timeframe, "ATR", "median_30")

        if ema_50 is None or atr is None or atr_median is None:
            logging.warning("⚠️ Не удалось получить EMA50, ATR или median(30)")
            return

        # 🔹 Получение RSI 14
        rsi_14 = await interface.get_indicator_value(symbol, timeframe, "RSI", "14")
        if rsi_14 is None:
            logging.warning("⚠️ Не удалось получить RSI 14")
            return

        # 🔹 Проверка условий входа
        if direction == "long":
            threshold = ema_50 - (atr * Decimal("0.5"))
            if entry_price < threshold:
                logging.info(f"⛔ Вход в long запрещён: цена {entry_price} < {threshold}")
                return
            if atr <= atr_median:
                logging.info(f"⛔ Вход в long запрещён: ATR {atr} <= median(30) {atr_median}")
                return
            if rsi_14 <= Decimal("50"):
                logging.info(f"⛔ Вход в long запрещён: RSI {rsi_14} <= 50")
                return

        elif direction == "short":
            threshold = ema_50 + (atr * Decimal("0.5"))
            if entry_price > threshold:
                logging.info(f"⛔ Вход в short запрещён: цена {entry_price} > {threshold}")
                return
            if atr <= atr_median:
                logging.info(f"⛔ Вход в short запрещён: ATR {atr} <= median(30) {atr_median}")
                return
            if rsi_14 >= Decimal("50"):
                logging.info(f"⛔ Вход в short запрещён: RSI {rsi_14} >= 50")
                return

        # 🔹 Расчёт параметров позиции
        result = await interface.calculate_position_size(task)

        if result is None:
            logging.warning("⚠️ Расчёт позиции завершён без результата — позиция не будет открыта")
            return

        debug_log(f"📊 Расчёт позиции (strategy_3): "
                     f"qty={result['quantity']}, notional={result['notional_value']}, "
                     f"risk={result['planned_risk']}, margin={result['margin_used']}, "
                     f"sl={result['stop_loss_price']}")

        # 🔹 Создание позиции в базе
        position_id = await interface.open_position(task, result)

        if position_id:
            logging.info(f"✅ Позиция открыта strategy_3, ID={position_id}")
        else:
            logging.warning("⚠️ Позиция не была открыта")