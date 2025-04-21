import asyncio
import asyncpg
import os
import redis.asyncio as redis_lib
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

# --- Подключение к Redis ---
def get_redis():
    return redis_lib.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD"),
        ssl=True
    )

# --- Подключение к PostgreSQL ---
async def get_pg():
    db_url = os.getenv("DATABASE_URL")
    return await asyncpg.connect(db_url)

# --- Обработка позиций ---
async def check_positions():
    print("[WORKER] Проверка активных позиций...", flush=True)
    pg = await get_pg()
    r = get_redis()

    try:
        rows = await pg.fetch("""
            SELECT p.id, p.symbol, p.direction, p.entry_price, p.quantity_left, p.quantity,
                   t.precision_price
            FROM positions p
            JOIN tickers t ON p.symbol = t.symbol
            WHERE p.status = 'open'
        """)
        print(f"[WORKER] Найдено позиций для проверки: {len(rows)}", flush=True)

        for row in rows:
            symbol = row["symbol"]
            position_id = row["id"]
            direction = row["direction"]
            entry_price = Decimal(row["entry_price"])
            qty_left = Decimal(row["quantity_left"])
            quantity = Decimal(row["quantity"])
            precision_price = int(row["precision_price"])
            notional_value = (entry_price * qty_left).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)

            price_raw = await r.get(f"price:{symbol}")
            if not price_raw:
                print(f"[WORKER] Нет цены в Redis для {symbol}", flush=True)
                continue

            price_str = price_raw.decode("utf-8")
            current_price = Decimal(price_str).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)
            print(f"[WORKER] {symbol}: current={current_price}, direction={direction}", flush=True)

            # --- Получение целей позиции ---
            targets = await pg.fetch("""
                SELECT id, type, level, price, quantity
                FROM position_targets
                WHERE position_id = $1 AND hit = false AND canceled = false
            """, position_id)

            print(f"[WORKER] Целей для позиции {position_id}: {len(targets)}", flush=True)
            for t in targets:
                print(f"[WORKER] → {t['type']} L{t['level'] or '-'} @ {t['price']} qty={t['quantity']}", flush=True)
                target_price = Decimal(t["price"]).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)

                if t["type"] == "sl":
                    if (direction == "long" and current_price <= target_price) or \
                       (direction == "short" and current_price >= target_price):
                        print(f"[WORKER] SL сработал @ {current_price} {'<=' if direction == 'long' else '>='} {target_price}", flush=True)
                        
                        # --- Отметить SL как исполненный ---
                        await pg.execute("""
                            UPDATE position_targets
                            SET hit = true, hit_at = NOW(), canceled = true
                            WHERE id = $1
                        """, t["id"])

                        # --- Закрытие позиции полностью ---
                        # комиссия считается от notional_value всей позиции
                        commission = (notional_value * Decimal("0.04") / 100).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)

                        if direction == "long":
                            pnl = ((current_price - entry_price) * quantity).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN) - commission
                        else:
                            pnl = ((entry_price - current_price) * quantity).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN) - commission

                        # определить причину закрытия
                        close_reason = "sl-after-take" if qty_left < quantity else "sl"

                        await pg.execute("""
                            UPDATE positions
                            SET status = 'closed',
                                exit_price = $1,
                                closed_at = NOW(),
                                pnl = $2,
                                close_reason = $3,
                                quantity_left = 0
                            WHERE id = $4
                        """, current_price, pnl, close_reason, position_id)

                        print(f"[WORKER] Позиция {position_id} закрыта по SL @ {current_price}, pnl={pnl}", flush=True)
                        
                        # --- Очистить оставшиеся цели (например, TP) после закрытия позиции ---
                        await pg.execute("""
                            UPDATE position_targets
                            SET canceled = true
                            WHERE position_id = $1 AND hit = false AND canceled = false
                        """, position_id)
                    
                elif t["type"] == "tp":
                    if (direction == "long" and current_price >= target_price) or \
                       (direction == "short" and current_price <= target_price):
                        print(f"[WORKER] TP сработал @ {current_price} {'>=' if direction == 'long' else '<='} {target_price}", flush=True)

                        # --- Отметить цель как исполненную ---
                        await pg.execute("""
                            UPDATE position_targets
                            SET hit = true, hit_at = NOW(), canceled = true
                            WHERE id = $1
                        """, t["id"])

                        # --- Расчёт PnL за эту цель ---
                        realized_qty = Decimal(t["quantity"])
                        tp_price = Decimal(t["price"]).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)
                        price_diff = (tp_price - entry_price) if direction == "long" else (entry_price - tp_price)
                        pnl_gain = (price_diff * realized_qty).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)

                        # --- Учитываем часть комиссии (на выход) ---
                        commission = (realized_qty * Decimal(t["price"]) * Decimal("0.04") / 100).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)
                        pnl_gain -= commission

                        # --- Обновить позицию: quantity_left и накопленный pnl ---
                        updated = await pg.fetchrow("""
                            UPDATE positions
                            SET
                                quantity_left = quantity_left - $1,
                                pnl = pnl + $2,
                                close_reason = $3
                            WHERE id = $4
                            RETURNING quantity_left, quantity
                        """, realized_qty, pnl_gain, "tp1-hit" if t["level"] == 1 else "tp-hit", position_id)

                        new_left = Decimal(updated["quantity_left"])
                        original_qty = Decimal(updated["quantity"])

                        # --- Перенос SL в безубыток при TP1 ---
                        if t["level"] == 1:
                            await pg.execute("""
                                UPDATE position_targets
                                SET price = $1, quantity = $2
                                WHERE position_id = $3 AND type = 'sl' AND hit = false AND canceled = false
                            """, entry_price, new_left, position_id)

                        # --- Закрытие позиции, если всё реализовано ---
                        if new_left <= 0:
                            await pg.execute("""
                                UPDATE positions
                                SET status = 'closed', closed_at = NOW(), exit_price = $1, close_reason = $2
                                WHERE id = $3
                            """, current_price, "tp", position_id)

                            print(f"[WORKER] Позиция {position_id} полностью закрыта по TP", flush=True)
                        else:
                            print(f"[WORKER] Позиция {position_id} частично реализована: осталось {new_left}", flush=True)

    except Exception as e:
        print(f"[WORKER] Ошибка при проверке позиций: {e}", flush=True)
    finally:
        await pg.close()

# --- Главный цикл ---
async def main_loop():
    while True:
        await check_positions()
        await asyncio.sleep(5)

if __name__ == "__main__":
    print("[WORKER] Запуск worker_main.py", flush=True)
    asyncio.run(main_loop())