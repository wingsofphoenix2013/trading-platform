import logging
from decimal import Decimal, ROUND_DOWN
from debug_utils import debug_log

# 🔸 Стратегия strategy_8_2
class Strategy8_2:
    def __init__(self):
        pass

    # 🔸 Обработка сигнала в strategy_8_2 (M5 / EMA50 ± ATR, RSI, MFI, угол LR)
    async def on_signal(self, task: dict, interface):
        debug_log("📈 Обработка сигнала в strategy_8_2 (M5 / EMA + ATR + RSI + MFI + LR50)")

        symbol = task["symbol"]
        direction = task["direction"]
        timeframe = "M5"
        entry_price = interface.latest_prices.get(symbol)

        if entry_price is None:
            logging.warning(f"⚠️ Нет актуальной цены для {symbol}")
            return

        # 🔹 Получение индикаторов
        ema_50 = await interface.get_indicator_value(symbol, timeframe, "EMA", "50")
        atr = await interface.get_indicator_value(symbol, timeframe, "ATR", "atr")
        rsi = await interface.get_indicator_value(symbol, timeframe, "RSI", "14")
        mfi = await interface.get_indicator_value(symbol, timeframe, "MFI", "14")
        lr_angle = await interface.get_indicator_value(symbol, timeframe, "LR", "lr_angle")

        if None in (ema_50, atr, rsi, mfi, lr_angle):
            logging.warning("⚠️ Не удалось получить один из индикаторов: EMA, ATR, RSI, MFI, LR")
            return

        # 🔹 Проверка условий входа
        if direction == "long":
#             threshold = ema_50 - (atr * Decimal("0.5"))
#             if entry_price <= threshold:
#                 debug_log(f"⛔ Вход в long запрещён: цена {entry_price} <= {threshold}")
#                 return
#             if rsi >= Decimal("50"):
#                 debug_log(f"⛔ Вход в long запрещён: RSI {rsi} >= 50")
#                 return
#             if mfi >= Decimal("50"):
#                 debug_log(f"⛔ Вход в long запрещён: MFI {mfi} >= 50")
#                 return
            if lr_angle <= Decimal("0.02"):
                debug_log(f"⛔ Вход в long запрещён: угол LR50 {lr_angle} <= 0.02")
                return

        elif direction == "short":
#             threshold = ema_50 + (atr * Decimal("0.5"))
#             if entry_price >= threshold:
#                 debug_log(f"⛔ Вход в short запрещён: цена {entry_price} >= {threshold}")
#                 return
#             if rsi <= Decimal("50"):
#                 debug_log(f"⛔ Вход в short запрещён: RSI {rsi} <= 50")
#                 return
#             if mfi <= Decimal("50"):
#                 debug_log(f"⛔ Вход в short запрещён: MFI {mfi} <= 50")
#                 return
            if lr_angle <= Decimal("-0.02"):
                debug_log(f"⛔ Вход в short запрещён: угол LR50 {lr_angle} >= 0.02")
                return

        # 🔹 Расчёт параметров позиции
        result = await interface.calculate_position_size(task)

        if result is None:
            logging.warning("⚠️ Расчёт позиции завершён без результата — позиция не будет открыта")
            return

        debug_log(f"📊 Расчёт позиции (strategy_8_2): "
                     f"qty={result['quantity']}, notional={result['notional_value']}, "
                     f"risk={result['planned_risk']}, margin={result['margin_used']}, "
                     f"sl={result['stop_loss_price']}")

        # 🔹 Создание позиции в базе
        position_id = await interface.open_position(task, result)

        if position_id:
            debug_log(f"✅ Позиция открыта strategy_8_2, ID={position_id}")
        else:
            logging.warning("⚠️ Позиция не была открыта")