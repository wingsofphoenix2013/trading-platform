import logging
from decimal import Decimal, ROUND_DOWN
from debug_utils import debug_log

# 🔸 Стратегия strategy_1 с проверкой EMA50 и ATR
class Strategy2:
    def __init__(self):
        pass

    # 🔸 Обработка сигнала с учётом специфических условий
    async def on_signal(self, task: dict, interface):
        debug_log("📈 Продолжение логики внутри strategy_2...")

        symbol = task["symbol"]
        direction = task["direction"]
        timeframe = task.get("timeframe", "M1")
        entry_price = interface.latest_prices.get(symbol)

        if entry_price is None:
            logging.warning(f"⚠️ Нет актуальной цены для {symbol}")
            return

        # 🔹 Получение EMA50 и ATR
        ema_50 = await interface.get_indicator_value(symbol, timeframe, "EMA", "50")
        atr = await interface.get_indicator_value(symbol, timeframe, "ATR", "atr")

        if ema_50 is None or atr is None:
            logging.warning("⚠️ Не удалось получить EMA50 или ATR")
            return

        # 🔹 Получение дополнительных индикаторов
        lr_angle_m1 = await interface.get_indicator_value(symbol, "M1", "LR", "lr_angle")
        lr_angle_m5 = await interface.get_indicator_value(symbol, "M5", "LR", "lr_angle")
        rsi_14 = await interface.get_indicator_value(symbol, "M1", "RSI", "14")

        if lr_angle_m1 is None or lr_angle_m5 is None or rsi_14 is None:
            logging.warning("⚠️ Не удалось получить один из индикаторов: lr_angle или RSI")
            return

        # 🔹 Проверка условий входа
        if direction == "long":
            threshold = ema_50 - (atr * Decimal("0.5"))
            if entry_price < threshold:
                debug_log(f"⛔ Вход в long запрещён: цена {entry_price} < {threshold}")
                return
            if lr_angle_m1 <= Decimal("0.07") or lr_angle_m5 <= Decimal("0.035"):
                debug_log(f"⛔ Вход в long запрещён: угол регрессии слишком мал (M1={lr_angle_m1}, M5={lr_angle_m5})")
                return
            if rsi_14 >= Decimal("50"):
                debug_log(f"⛔ Вход в long запрещён: RSI {rsi_14} >= 50")
                return

        elif direction == "short":
            threshold = ema_50 + (atr * Decimal("0.5"))
            if entry_price > threshold:
                debug_log(f"⛔ Вход в short запрещён: цена {entry_price} > {threshold}")
                return
            if lr_angle_m1 >= Decimal("0.07") or lr_angle_m5 >= Decimal("0.035"):
                debug_log(f"⛔ Вход в short запрещён: угол регрессии слишком велик (M1={lr_angle_m1}, M5={lr_angle_m5})")
                return
            if rsi_14 <= Decimal("50"):
                debug_log(f"⛔ Вход в short запрещён: RSI {rsi_14} <= 50")
                return

        # 🔹 Расчёт параметров позиции
        result = await interface.calculate_position_size(task)

        if result is None:
            logging.warning("⚠️ Расчёт позиции завершён без результата — позиция не будет открыта")
            return

        debug_log(f"📊 Расчёт позиции (strategy_2): "
                     f"qty={result['quantity']}, notional={result['notional_value']}, "
                     f"risk={result['planned_risk']}, margin={result['margin_used']}, "
                     f"sl={result['stop_loss_price']}")

        # 🔹 Создание позиции в базе
        position_id = await interface.open_position(task, result)

        if position_id:
            debug_log(f"✅ Позиция открыта strategy_2, ID={position_id}")
        else:
            logging.warning("⚠️ Позиция не была открыта")