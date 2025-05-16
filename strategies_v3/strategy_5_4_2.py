import logging
from decimal import Decimal, ROUND_DOWN
from debug_utils import debug_log

# 🔸 Стратегия strategy_5_4_2 с проверкой EMA50 и ATR
class Strategy5_4_2:
    def __init__(self):
        pass

    # 🔸 Обработка сигнала в strategy_5_4_2 (таймфрейм M5, фильтры EMA50, RSI, MFI)
    async def on_signal(self, task: dict, interface):
        debug_log("📈 Обработка сигнала в strategy_5_4_2 (M5 / EMA + RSI + MFI)")

        symbol = task["symbol"]
        direction = task["direction"]
        timeframe = "M5"
        entry_price = interface.latest_prices.get(symbol)

        if entry_price is None:
            logging.warning(f"⚠️ Нет актуальной цены для {symbol}")
            return

        # 🔹 Получение EMA50, RSI, MFI
        ema_50 = await interface.get_indicator_value(symbol, timeframe, "EMA", "50")
        rsi = await interface.get_indicator_value(symbol, timeframe, "RSI", "14")
        mfi = await interface.get_indicator_value(symbol, timeframe, "MFI", "14")

        if ema_50 is None or rsi is None or mfi is None:
            logging.warning("⚠️ Не удалось получить один из индикаторов: EMA50, RSI, MFI")
            return

        # 🔹 Проверка условий входа
        if direction == "long":
            if entry_price >= ema_50:
                debug_log(f"⛔ Вход в long запрещён: цена {entry_price} >= EMA50 {ema_50}")
                return
            if rsi >= Decimal("45"):
                debug_log(f"⛔ Вход в long запрещён: RSI {rsi} >= 45")
                return
            if mfi >= Decimal("25"):
                debug_log(f"⛔ Вход в long запрещён: MFI {mfi} >= 25")
                return

        elif direction == "short":
            if entry_price <= ema_50:
                debug_log(f"⛔ Вход в short запрещён: цена {entry_price} <= EMA50 {ema_50}")
                return
            if rsi <= Decimal("55"):
                debug_log(f"⛔ Вход в short запрещён: RSI {rsi} <= 55")
                return
            if mfi <= Decimal("75"):
                debug_log(f"⛔ Вход в short запрещён: MFI {mfi} <= 75")
                return

        # 🔹 Расчёт параметров позиции
        result = await interface.calculate_position_size(task)

        if result is None:
            logging.warning("⚠️ Расчёт позиции завершён без результата — позиция не будет открыта")
            return

        debug_log(f"📊 Расчёт позиции (strategy_5_4_2): "
                     f"qty={result['quantity']}, notional={result['notional_value']}, "
                     f"risk={result['planned_risk']}, margin={result['margin_used']}, "
                     f"sl={result['stop_loss_price']}")

        # 🔹 Создание позиции в базе
        position_id = await interface.open_position(task, result)

        if position_id:
            debug_log(f"✅ Позиция открыта strategy_5_4_2, ID={position_id}")
        else:
            logging.warning("⚠️ Позиция не была открыта")