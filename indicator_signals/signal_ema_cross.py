from debug_utils import debug_log

# 🔸 Кэш instance_id по (length, timeframe)
instance_cache = {}

# 🔸 Получение instance_id для EMA по длине и таймфрейму
async def get_instance_id(db_pool, length: str, timeframe: str) -> int:
    key = (length, timeframe)
    if key in instance_cache:
        return instance_cache[key]

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT i.id
            FROM indicator_instances_v2 i
            JOIN indicator_parameters_v2 p ON i.id = p.instance_id
            WHERE i.indicator = 'EMA'
              AND i.timeframe = $1
              AND p.param = 'length'
              AND p.value = $2
              AND i.enabled = true
            LIMIT 1
        """, timeframe, length)

        if row:
            instance_cache[key] = row["id"]
            return row["id"]
        else:
            debug_log(f"⚠️ Не найден instance_id для EMA {length} / {timeframe}")
            return None

# 🔸 Получение двух последних значений по указанному параметру
async def get_last_two_values(db_pool, instance_id: int, symbol: str, param_name: str):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT value
            FROM indicator_values_v2
            WHERE instance_id = $1
              AND symbol = $2
              AND param_name = $3
            ORDER BY open_time DESC
            LIMIT 2
        """, instance_id, symbol, param_name)

        if len(rows) == 2:
            return rows[1]["value"], rows[0]["value"]  # prev, curr
        return None, None

# 🔸 Обработка сигнала EMA для генерации сигнала пересечения
async def process_ema_cross_signal(symbol: str, timeframe: str, params: dict, ts: str, state: dict, publish, db_pool):
    try:
        # 🔹 Получение instance_id для EMA9 и EMA21
        ema9_id = await get_instance_id(db_pool, "9", timeframe)
        ema21_id = await get_instance_id(db_pool, "21", timeframe)

        if not ema9_id or not ema21_id:
            return

        # 🔹 Получение последних двух значений для ema9 и ema21
        ema9_prev, ema9_curr = await get_last_two_values(db_pool, ema9_id, symbol, 'ema9')
        ema21_prev, ema21_curr = await get_last_two_values(db_pool, ema21_id, symbol, 'ema21')

        if None in [ema9_prev, ema9_curr, ema21_prev, ema21_curr]:
            return

        # 🔹 Проверка условия пересечения вверх (LONG)
        if ema9_prev < ema21_prev and ema9_curr > ema21_curr:
            message = f"EMA_{timeframe}_LONG"
            debug_log(f"✔ Пересечение вверх: {symbol} / {timeframe}")
            await publish(symbol=symbol, message=message, time=ts)

        # 🔹 Проверка условия пересечения вниз (SHORT)
        elif ema9_prev > ema21_prev and ema9_curr < ema21_curr:
            message = f"EMA_{timeframe}_SHORT"
            debug_log(f"✔ Пересечение вниз: {symbol} / {timeframe}")
            await publish(symbol=symbol, message=message, time=ts)

    except Exception as e:
        debug_log(f"Ошибка в process_ema_cross_signal: {e}")