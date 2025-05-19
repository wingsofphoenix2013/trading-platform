import logging
from decimal import Decimal, ROUND_DOWN
from datetime import datetime
from debug_utils import debug_log

# 🔸 Стратегия strategy_5_3 с проверкой EMA50, RSI, MFI + фильтрация по истории SL/MFI
class Strategy5_3:
    def __init__(self):
        pass

    # 🔸 Обработка сигнала в strategy_5_3 (таймфрейм M5, фильтры EMA + RSI + MFI + SL-защита)
    async def on_signal(self, task: dict, interface):
        debug_log("📈 Обработка сигнала в strategy_5_3 (M5 / EMA + RSI + MFI + SL-фильтр)")

        symbol = task["symbol"]
        direction = task["direction"]
        timeframe = "M5"

        # 🔹 Получение и преобразование цены входа
        entry_price = interface.latest_prices.get(symbol)
        if entry_price is None:
            logging.warning(f"⚠️ Нет актуальной цены для {symbol}")
            return
        entry_price = Decimal(str(entry_price))

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
            if rsi >= Decimal("40"):
                debug_log(f"⛔ Вход в long запрещён: RSI {rsi} >= 40")
                return
            if mfi >= Decimal("25"):
                debug_log(f"⛔ Вход в long запрещён: MFI {mfi} >= 25")
                return

#             # 🔸 Доп. фильтр: проверка предыдущей long-сделки по SL и поведения MFI
#             last_close_time = await interface.get_last_sl_close_time(symbol, "long")
#             if last_close_time is not None:
#                 signal_time = datetime.fromisoformat(task.get("sent_at") or task["bar_time"])
#                 mfi_values = await interface.get_mfi_values_between(symbol, last_close_time, signal_time)
#                 logging.info(f"📊 LONG SL-защита [{symbol}]: найдено {len(mfi_values)} значений MFI между {last_close_time} и {signal_time}")
#                 if not mfi_values:
#                     logging.info(f"⚠️ MFI отсутствует в диапазоне SL-фильтрации по {symbol} — фильтр пропущен")
#                 else:
#                     mfi_max = max(mfi_values)
#                     logging.info(f"📊 MFI максимум по {symbol}: {mfi_max}")
#                     if mfi_max <= Decimal("35"):
#                         logging.info(f"⛔ Вход в long по {symbol} отклонён: после SL MFI ни разу не поднимался выше 35")
#                         return

        elif direction == "short":
            if entry_price <= ema_50:
                debug_log(f"⛔ Вход в short запрещён: цена {entry_price} <= EMA50 {ema_50}")
                return
            if rsi <= Decimal("60"):
                debug_log(f"⛔ Вход в short запрещён: RSI {rsi} <= 60")
                return
            if mfi <= Decimal("75"):
                debug_log(f"⛔ Вход в short запрещён: MFI {mfi} <= 75")
                return

#             # 🔸 Доп. фильтр: проверка предыдущей short-сделки по SL и поведения MFI
#             last_close_time = await interface.get_last_sl_close_time(symbol, "short")
#             if last_close_time is not None:
#                 signal_time = datetime.fromisoformat(task.get("sent_at") or task["bar_time"])
#                 mfi_values = await interface.get_mfi_values_between(symbol, last_close_time, signal_time)
#                 logging.info(f"📊 SHORT SL-защита [{symbol}]: найдено {len(mfi_values)} значений MFI между {last_close_time} и {signal_time}")
#                 if not mfi_values:
#                     logging.info(f"⚠️ MFI отсутствует в диапазоне SL-фильтрации по {symbol} — фильтр пропущен")
#                 else:
#                     mfi_min = min(mfi_values)
#                     logging.info(f"📊 MFI минимум по {symbol}: {mfi_min}")
#                     if mfi_min >= Decimal("65"):
#                         logging.info(f"⛔ Вход в short по {symbol} отклонён: после SL MFI ни разу не опускался ниже 65")
#                         return

        # 🔹 Расчёт параметров позиции
        result = await interface.calculate_position_size(task)

        if result is None:
            logging.warning("⚠️ Расчёт позиции завершён без результата — позиция не будет открыта")
            return

        debug_log(f"📊 Расчёт позиции (strategy_5_3): "
                  f"qty={result['quantity']}, notional={result['notional_value']}, "
                  f"risk={result['planned_risk']}, margin={result['margin_used']}, "
                  f"sl={result['stop_loss_price']}")

        # 🔹 Создание позиции в базе
        position_id = await interface.open_position(task, result)

        if position_id:
            debug_log(f"✅ Позиция открыта strategy_5_3, ID={position_id}")
        else:
            logging.warning("⚠️ Позиция не была открыта")