📌 План работ по генерации сигналов на основе технических индикаторов (EMA-cross и др.)

⸻

🔷 I. Базовая архитектура

Шаг 1. Создание координатора indicator_signals_main.py
	•	Настроить логирование и переменные окружения.
	•	Создать Redis-клиент и подключение.
	•	Объявить глобальное in-memory хранилище signal_state_storage.
	•	Реализовать функцию публикации сигнала в signals_stream с учётом формата signals_v2_main.py.
	•	Реализовать цикл чтения из indicators_ready_stream через xreadgroup.
	•	Разбирать входящие сообщения и направлять их в соответствующий обработчик по типу индикатора.

⸻

🔷 II. Логика EMA-cross (файл signal_ema_cross.py)

Шаг 2. Создание модуля логики
	•	Объявить функцию process_ema_cross_signal(symbol, timeframe, params, value, ts, state, publish).
	•	Хранить последние 2 значения по каждому EMA:length, symbol, tf.
	•	При наличии предыдущего и текущего значения EMA9 и EMA21:
	•	Проверить условие пересечения снизу вверх (EMA9_prev < EMA21_prev и EMA9_now > EMA21_now).
	•	Если условие выполнено — сформировать message EMA_<TF>_LONG.
	•	Вызвать функцию publish(...).

⸻

🔷 III. Интеграция логик и маршрутизация

Шаг 3. Обработка dispatch в indicator_signals_main.py
	•	Создать карту диспетчеризации по типу индикатора:

INDICATOR_DISPATCH = {
    "EMA": process_ema_cross_signal,
    # RSI: process_rsi_signal, ...
}


	•	В main_loop вызывать соответствующую функцию, если тип поддерживается.

⸻

🔷 IV. Публикация сигнала в формате TradingView

Шаг 4. Унифицированная публикация
	•	Реализовать функцию publish_to_signals_stream(symbol, message, time):
	•	Добавить поле sent_at = datetime.utcnow().isoformat().
	•	Опубликовать сообщение в Redis Stream signals_stream с полями:
	•	message, symbol, time, sent_at.

⸻

🔷 V. Регистрация сигналов в signals_v2

Шаг 5. Добавление фраз
	•	Для всех генерируемых сигналов (например, EMA_M1_LONG, EMA_M5_LONG):
	•	Добавить строку в signals_v2 с полями:
	•	signal_type = 'action'
	•	long_phrase = 'EMA_M1_LONG' (и т. д.)
	•	enabled = true
	•	source = 'indicator_signals'
	•	short_phrase = NULL или фиктивное значение
	•	description = 'EMA пересечение снизу вверх для M1' (и т. д.)

⸻

🔷 VI. Точка входа и запуск

Шаг 6. main() и __main__
	•	Функция main() запускает цикл прослушивания.
	•	Добавить if __name__ == "__main__": asyncio.run(main())

⸻

🧩 Дополнительно (на будущее)
	•	Очистка старых значений из памяти (например, старше 10 мин).
	•	Защита от повторной генерации одного и того же сигнала.
	•	Возможность указания нужных комбинаций через конфиг или таблицы.