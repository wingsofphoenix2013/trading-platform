from debug_utils import debug_log

# 🔸 Обработка сигнала EMA для генерации сигнала пересечения
async def process_ema_cross_signal(symbol: str, timeframe: str, params: dict, value: float, ts: str, state: dict, publish):
    try:
        # 🔸 Длина EMA (например, 9 или 21)
        length = str(params.get("length"))
        if not length:
            return

        key = (symbol, timeframe)
        indicator_key = f"EMA:{length}"

        # 🔸 Инициализация хранилища по тикеру и ТФ
        if key not in state:
            state[key] = {}
        if indicator_key not in state[key]:
            state[key][indicator_key] = [None, None]

        # 🔸 Обновление in-memory состояния
        state[key][indicator_key][0] = state[key][indicator_key][1]
        state[key][indicator_key][1] = value

        # 🔸 Проверка наличия данных EMA9 и EMA21
        ema9 = state[key].get("EMA:9")
        ema21 = state[key].get("EMA:21")

        if not ema9 or not ema21:
            return
        if None in ema9 or None in ema21:
            return

        # 🔸 Проверка условия пересечения снизу вверх
        if ema9[0] < ema21[0] and ema9[1] > ema21[1]:
            message = f"EMA_{timeframe}_LONG"
            debug_log(f"✔ Условие выполнено: {message} для {symbol}")
            await publish(symbol=symbol, message=message, time=ts)

    except Exception as e:
        debug_log(f"Ошибка в process_ema_cross_signal: {e}")