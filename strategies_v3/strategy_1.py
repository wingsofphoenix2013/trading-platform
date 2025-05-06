import logging

# 🔸 Стратегия strategy_1 с проверкой EMA50 и ATR
class Strategy1:
    def __init__(self):
        pass

    # 🔸 Обработка сигнала с учётом специфических условий
    async def on_signal(self, task: dict, interface):
        logging.info("📈 Продолжение логики внутри strategy_1...")

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

        # 🔹 Проверка условий входа
        if direction == "long":
            threshold = ema_50 - (atr * 0.5)
            if entry_price < threshold:
                logging.info(f"⛔ Вход в long запрещён: цена {entry_price} < {threshold}")
                return

        elif direction == "short":
            threshold = ema_50 + (atr * 0.5)
            if entry_price > threshold:
                logging.info(f"⛔ Вход в short запрещён: цена {entry_price} > {threshold}")
                return

        # 🔹 Расчёт параметров позиции
        result = await interface.calculate_position_size(task)

        if result is None:
            logging.warning("⚠️ Расчёт позиции завершён без результата — позиция не будет открыта")
            return

        logging.info(f"📊 Расчёт позиции (strategy_1): "
                     f"qty={result['quantity']}, notional={result['notional_value']}, "
                     f"risk={result['planned_risk']}, margin={result['margin_used']}, "
                     f"sl={result['stop_loss_price']}")