# 📋 План работ: indicators_v2

---

## 🔷 ЭТАП I. Структура базы данных

### 1. indicator_instances

```sql
CREATE TABLE public.indicator_instances (
    id serial PRIMARY KEY,
    indicator text NOT NULL,
    timeframe text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    stream_publish boolean NOT NULL DEFAULT false
);
```

### 2. indicator_parameters

```sql
CREATE TABLE public.indicator_parameters (
    id serial PRIMARY KEY,
    instance_id integer NOT NULL REFERENCES indicator_instances(id) ON DELETE CASCADE,
    param text NOT NULL,
    value text NOT NULL,
    UNIQUE (instance_id, param)
);
```

### 3. indicator_values_v2

```sql
CREATE TABLE public.indicator_values_v2 (
    id serial PRIMARY KEY,
    instance_id integer NOT NULL REFERENCES indicator_instances(id) ON DELETE CASCADE,
    symbol text NOT NULL,
    open_time timestamp NOT NULL,
    param_name text NOT NULL,
    value double precision NOT NULL,
    updated_at timestamp DEFAULT now(),
    UNIQUE (instance_id, symbol, open_time, param_name)
);
```

#### Индексы:

```sql
CREATE INDEX idx_iv2_symbol_time ON indicator_values_v2(symbol, open_time);
CREATE INDEX idx_iv2_instance_id ON indicator_values_v2(instance_id);
CREATE INDEX idx_iv2_param_name ON indicator_values_v2(param_name);
```

---

## 🔷 ЭТАП II. Загрузка конфигурации

- Загрузка всех `enabled` `indicator_instances`
- Загрузка параметров из `indicator_parameters`
- Построение in-memory словаря конфигурации

---

## 🔷 ЭТАП III. Механизм запуска через Redis Pub/Sub

- Подписка на каналы: `ohlcv_m1_ready`, `ohlcv_aggregate`
- Разбор сообщений:
  - `ohlcv_m1_ready`: action = m1_ready
  - `ohlcv_aggregate`: action = aggregate
- Вызов расчёта по каждому instance, совпадающему по таймфрейму

---

## 🔷 ЭТАП IV. Модули расчёта и сохранение

- Центральный координатор: `indicators_v2_main.py`
- Отдельные модули: `ema.py`, `atr.py`, `lr.py`, ...
- Формат Redis ключей: `<symbol>:<timeframe>:<indicator>:<param>`
- Запись в БД: `indicator_values_v2`
- Удаление старых значений: хранить только 100 последних

---

## 🔷 ЭТАП V. Публикация в Redis Stream

- Поток: `indicators_ready_stream`
- Структура JSON:
```json
{
  "symbol": "BTCUSDT",
  "timeframe": "M5",
  "indicator": "EMA",
  "params": {"length": "100"},
  "calculated_at": "2025-05-13T13:42:00Z"
}
```
- Публикация разрешена только если `stream_publish = true` для `indicator_instance`

---

## 🔷 ЭТАП VI. Изоляция и логирование

- Ключи Redis и формат сообщений совместимы с `v1`
- Вся отладка — через `debug_log(...)` и `print(...)`
