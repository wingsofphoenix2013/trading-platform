# 🔸 Обработка задач на закрытие позиции
async def position_close_loop(db_pool):
    stream_name = "position:close"
    group_name = "position_closer"
    consumer_name = "position_closer_worker"

    try:
        await redis_client.xgroup_create(name=stream_name, groupname=group_name, id="0", mkstream=True)
        debug_log("✅ Группа position_closer создана")
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            debug_log("ℹ️ Группа position_closer уже существует")
        else:
            raise

    while True:
        try:
            entries = await redis_client.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={stream_name: ">"},
                count=10,
                block=1000
            )

            for stream, messages in entries:
                for msg_id, data in messages:
                    logging.info(f"📥 Получена задача на закрытие позиции: {data}")
                try:
                    position_id = int(data["position_id"])
                    target_id = int(data["target_id"])
                except (KeyError, ValueError):
                    logging.error("❌ Некорректные данные: отсутствует position_id или target_id")
                    await redis_client.xack(stream_name, group_name, msg_id)
                    continue

                position = open_positions.get(position_id)
                if not position:
                    logging.warning(f"⚠️ Позиция {position_id} не найдена в памяти")
                    await redis_client.xack(stream_name, group_name, msg_id)
                    continue

                targets = targets_by_position.get(position_id, [])
                debug_log(f"🧪 Память целей позиции {position_id}: {json.dumps(targets, default=str)}")
                debug_log(f"🧪 Ищем target_id = {target_id}")
                
                target = next((t for t in targets if t.get("id") == target_id), None)
                
                if data.get("type") == "sl":
                    try:
                        async with db_pool.acquire() as conn:
                            await conn.execute("""
                                UPDATE position_targets_v2
                                SET hit = true, hit_at = NOW()
                                WHERE id = $1
                            """, target_id)

                        debug_log(f"✅ SL цель ID={target_id} помечена как hit")

                        try:
                            sl_price = Decimal(target["price"])
                            # 🔸 Определение типа SL: первичный или переставленный
                            try:
                                async with db_pool.acquire() as conn:
                                    is_replaced_sl = await conn.fetchval("""
                                        SELECT EXISTS (
                                            SELECT 1 FROM position_targets_v2
                                            WHERE position_id = $1 AND type = 'sl' AND canceled = true
                                        )
                                    """, position_id)
                            except Exception as e:
                                logging.warning(f"⚠️ Не удалось проверить тип SL для позиции {position_id}: {e}")
                                is_replaced_sl = False

                            close_reason = "sl-tp-hit" if is_replaced_sl else "sl"
                            sl_log_message = "Сработал переставленный SL" if is_replaced_sl else "Позиция закрыта по SL"

                            # 🔹 Пересчёт pnl при закрытии по SL
                            try:
                                entry_price = Decimal(position["entry_price"])
                                qty = Decimal(position["quantity_left"])  # сохранить ДО обнуления
                                direction = position["direction"]
                                precision = Decimal("1e-8")

                                if direction == "long":
                                    delta = sl_price - entry_price
                                else:
                                    delta = entry_price - sl_price

                                pnl_increment = delta * qty
                                current_pnl = Decimal(position["pnl"])
                                new_pnl = (current_pnl + pnl_increment).quantize(precision, rounding=ROUND_DOWN)

                                async with db_pool.acquire() as conn:
                                    await conn.execute("""
                                        UPDATE positions_v2
                                        SET pnl = $1
                                        WHERE id = $2
                                    """, new_pnl, position_id)

                                position["pnl"] = new_pnl
                                debug_log(f"💰 Обновлён pnl: {current_pnl} → {new_pnl} (SL по {qty} @ {sl_price})")

                            except Exception as e:
                                logging.error(f"❌ Ошибка при пересчёте pnl по SL: {e}")
                                await redis_client.xack(stream_name, group_name, msg_id)
                                continue

                            # 🔸 Закрытие позиции
                            async with db_pool.acquire() as conn:
                                await conn.execute("""
                                    UPDATE positions_v2
                                    SET status = 'closed',
                                        planned_risk = 0,
                                        quantity_left = 0,
                                        exit_price = $1,
                                        closed_at = NOW(),
                                        close_reason = $2
                                    WHERE id = $3
                                """, sl_price, close_reason, position_id)

                            position["status"] = "closed"
                            position["planned_risk"] = Decimal("0")
                            position["exit_price"] = sl_price
                            position["close_reason"] = close_reason
                            position["quantity_left"] = Decimal("0")

                            debug_log(f"🛑 Позиция ID={position_id} закрыта по SL на уровне {sl_price}")
                            # 🔸 Удаление из памяти и отмена оставшихся целей
                            try:
                                open_positions.pop(position_id, None)
                                targets_by_position.pop(position_id, None)

                                async with db_pool.acquire() as conn:
                                    await conn.execute("""
                                        UPDATE position_targets_v2
                                        SET canceled = true
                                        WHERE position_id = $1 AND hit = false
                                    """, position_id)

                                debug_log(f"🚫 Цели позиции ID={position_id} помечены как canceled (SL)")
                                
                                try:
                                    log_details = json.dumps({
                                        "position_id": position_id,
                                        "sl_price": str(sl_price),
                                        "pnl": str(position["pnl"]),
                                        "quantity": str(position["quantity"])
                                    })

                                    async with db_pool.acquire() as conn:
                                        await conn.execute("""
                                            INSERT INTO system_logs (
                                                level, message, source, details, action_flag
                                            ) VALUES (
                                                'INFO', $1, 'position_close_worker', $2, 'ignore'
                                            )
                                        """, sl_log_message, log_details)

                                    debug_log(f"🧾 Запись в system_logs: Позиция ID={position_id} закрыта по SL")

                                except Exception as e:
                                    logging.warning(f"⚠️ Не удалось записать system_log для позиции {position_id}: {e}")
                            except Exception as e:
                                logging.error(f"❌ Ошибка при отмене целей позиции {position_id}: {e}")
                                await redis_client.xack(stream_name, group_name, msg_id)
                                continue

                            await redis_client.xack(stream_name, group_name, msg_id)
                            continue

                        except Exception as e:
                            logging.error(f"❌ Ошибка при закрытии позиции по SL: {e}")
                            await redis_client.xack(stream_name, group_name, msg_id)
                            continue

                    except Exception as e:
                        logging.error(f"❌ Ошибка при обновлении SL цели {target_id}: {e}")
                        await redis_client.xack(stream_name, group_name, msg_id)
                        continue
                        
                # Удаление цели из памяти
                targets_by_position[position_id] = [
                    t for t in targets if t.get("id") != target_id
                ]

                # Обновление цели TP в БД — помечаем как hit
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute("""
                            UPDATE position_targets_v2
                            SET hit = true, hit_at = NOW()
                            WHERE id = $1
                        """, target_id)
                    debug_log(f"✅ Цель TP ID={target_id} помечена как hit")
                except Exception as e:
                    logging.warning(f"⚠️ Не удалось обновить цель TP {target_id} как hit: {e}")

                try:
                    symbol = position["symbol"]
                    precision_qty = Decimal(f"1e-{tickers_storage[symbol]['precision_qty']}")
                    qty_left_before = Decimal(position["quantity_left"])
                    qty_hit = Decimal(target["quantity"])
                    new_quantity_left = (qty_left_before - qty_hit).quantize(precision_qty, rounding=ROUND_DOWN)

                    async with db_pool.acquire() as conn:
                        await conn.execute("""
                            UPDATE positions_v2
                            SET quantity_left = $1
                            WHERE id = $2
                        """, new_quantity_left, position_id)

                    position["quantity_left"] = new_quantity_left  # обновить in-memory

                    debug_log(f"📉 Обновлено quantity_left: {qty_left_before} → {new_quantity_left} для позиции ID={position_id}")

                except Exception as e:
                    logging.error(f"❌ Ошибка при обновлении quantity_left: {e}")
                    await redis_client.xack(stream_name, group_name, msg_id)
                    continue

                strategy_id = position["strategy_id"]
                strategy = strategies_cache.get(strategy_id)
                tp_sl_rules = strategy.get("tp_sl_rules", [])

                tp_level = target.get("level")
                sl_rule = next((r for r in tp_sl_rules if r["tp_level_id"] == tp_level), None)

                if not sl_rule or sl_rule["sl_mode"] == "none":
                    debug_log(f"ℹ️ Для TP {target_id} политика SL не требует перестановки")
                else:
                    # 🔸 Найдём текущий SL в памяти
                    current_sl = next((t for t in targets_by_position.get(position_id, []) if t["type"] == "sl" and not t["hit"] and not t["canceled"]), None)

                    if not current_sl:
                        logging.warning(f"⚠️ Текущий SL не найден, невозможно переставить")
                    else:
                        # 🔹 Отменить старый SL в БД
                        async with db_pool.acquire() as conn:
                            await conn.execute("""
                                UPDATE position_targets_v2
                                SET canceled = true
                                WHERE position_id = $1 AND type = 'sl' AND hit = false AND canceled = false
                            """, position_id)

                        # 🔹 Удалить старый SL из памяти
                        targets_by_position[position_id] = [
                            t for t in targets_by_position[position_id]
                            if not (t["type"] == "sl" and not t["hit"] and not t["canceled"])
                        ]

                        # Обновляем переменную targets
                        targets = targets_by_position[position_id]

                        debug_log(f"🔁 Старый SL отменён — подготовка к пересчёту нового")
                        
                    # 🔹 Расчёт нового SL
                    sl_mode = sl_rule["sl_mode"]
                    sl_value = Decimal(str(sl_rule["sl_value"])) if sl_mode in ("percent", "atr") else None
                    entry_price = Decimal(position["entry_price"])
                    direction = position["direction"]

                    sl_price = None

                    if sl_mode == "entry":
                        sl_price = entry_price

                    elif sl_mode == "percent":
                        delta = entry_price * (sl_value / Decimal("100"))
                        sl_price = (entry_price - delta if direction == "long" else entry_price + delta)

                    elif sl_mode == "atr":
                        atr = await get_indicator_value(symbol, strategy["timeframe"], "ATR", "atr")
                        if atr is not None:
                            sl_price = (entry_price - atr * sl_value if direction == "long" else entry_price + atr * sl_value)

                    if sl_price is None:
                        logging.warning("⚠️ Не удалось рассчитать SL — пропуск перестановки")
                    else:
                        precision_price = Decimal(f"1e-{tickers_storage[symbol]['precision_price']}")
                        sl_price = sl_price.quantize(precision_price, rounding=ROUND_DOWN)

                        quantity = Decimal(position["quantity_left"])

                        async with db_pool.acquire() as conn:
                            await conn.execute("""
                                INSERT INTO position_targets_v2 (
                                    position_id, type, price, quantity,
                                    hit, canceled, tp_trigger_type
                                ) VALUES (
                                    $1, 'sl', $2, $3,
                                    false, false, 'price'
                                )
                            """, position_id, sl_price, quantity)

                        targets_by_position[position_id].append({
                            "type": "sl",
                            "price": sl_price,
                            "quantity": quantity,
                            "hit": False,
                            "canceled": False
                        })

                        debug_log(f"📌 SL переставлен после TP {target_id}: новый уровень = {sl_price}")
                # 🔹 Пересчёт planned_risk
                try:
                    entry_price = Decimal(position["entry_price"])
                    quantity_left = Decimal(position["quantity_left"])

                    # Найдём актуальный SL
                    sl = next((t for t in targets_by_position.get(position_id, [])
                               if t["type"] == "sl" and not t["hit"] and not t["canceled"]), None)

                    if not sl:
                        logging.warning(f"⚠️ SL не найден для пересчёта planned_risk (позиция {position_id})")
                    else:
                        sl_price = Decimal(sl["price"])
                        risk = abs(entry_price - sl_price) * quantity_left
                        risk = risk.quantize(Decimal("1e-8"), rounding=ROUND_DOWN)

                        async with db_pool.acquire() as conn:
                            await conn.execute("""
                                UPDATE positions_v2
                                SET planned_risk = $1
                                WHERE id = $2
                            """, risk, position_id)

                        position["planned_risk"] = risk
                        debug_log(f"📐 Пересчитан planned_risk: {risk} для позиции ID={position_id}")

                except Exception as e:
                    logging.error(f"❌ Ошибка при пересчёте planned_risk: {e}")
                    await redis_client.xack(stream_name, group_name, msg_id)
                    continue
                # 🔹 Пересчёт pnl
                try:
                    entry_price = Decimal(position["entry_price"])
                    tp_price = Decimal(target["price"])
                    qty = Decimal(target["quantity"])
                    direction = position["direction"]
                    precision = Decimal("1e-8")

                    if direction == "long":
                        delta = tp_price - entry_price
                    else:
                        delta = entry_price - tp_price

                    pnl_increment = delta * qty
                    current_pnl = Decimal(position["pnl"])
                    new_pnl = (current_pnl + pnl_increment).quantize(precision, rounding=ROUND_DOWN)

                    async with db_pool.acquire() as conn:
                        await conn.execute("""
                            UPDATE positions_v2
                            SET pnl = $1
                            WHERE id = $2
                        """, new_pnl, position_id)

                    position["pnl"] = new_pnl
                    debug_log(f"💰 Обновлён pnl: {current_pnl} → {new_pnl} (TP по {qty} @ {tp_price})")

                except Exception as e:
                    logging.error(f"❌ Ошибка при пересчёте pnl: {e}")
                    await redis_client.xack(stream_name, group_name, msg_id)
                    continue
                # 🔹 Обновление close_reason
                try:
                    level = target.get("level")
                    reason = f"tp-{level}-hit"

                    async with db_pool.acquire() as conn:
                        await conn.execute("""
                            UPDATE positions_v2
                            SET close_reason = $1
                            WHERE id = $2
                        """, reason, position_id)

                        position["close_reason"] = reason
                        debug_log(f"📝 Установлен close_reason: {reason} для позиции ID={position_id}")

                        # 🔹 Логирование в system_logs
                        tp_price = str(target.get("price"))
                        qty_for_log = str(target.get("quantity"))

                        log_details = json.dumps({
                            "position_id": position_id,
                            "target_id": target_id,
                            "tp_price": tp_price,
                            "quantity": qty_for_log
                        })

                        await conn.execute("""
                            INSERT INTO system_logs (
                                level, message, source, details, action_flag
                            ) VALUES (
                                'INFO', $1, 'position_close_worker', $2, 'ignore'
                            )
                        """, f"Сработал TP уровень {level}", log_details)

                        debug_log(f"🧾 Запись в system_logs: TP {level} для позиции ID={position_id}")

                except Exception as e:
                    logging.error(f"❌ Ошибка при обновлении close_reason или записи в system_logs: {e}")
                    await redis_client.xack(stream_name, group_name, msg_id)
                    continue
                    
                # 🔸 Проверка на полное закрытие позиции
                if position["quantity_left"] == 0:
                    try:
                        tp_price = Decimal(target["price"])

                        async with db_pool.acquire() as conn:
                            await conn.execute("""
                                UPDATE positions_v2
                                SET status = 'closed',
                                    planned_risk = 0,
                                    exit_price = $1,
                                    closed_at = NOW(),
                                    close_reason = 'tp-full-hit'
                                WHERE id = $2
                            """, tp_price, position_id)

                        position["status"] = "closed"
                        position["close_reason"] = "tp-full-hit"
                        position["planned_risk"] = Decimal("0")
                        position["exit_price"] = tp_price
                        
                        # 🔹 Отметить оставшиеся цели как canceled
                        async with db_pool.acquire() as conn:
                            await conn.execute("""
                                UPDATE position_targets_v2
                                SET canceled = true
                                WHERE position_id = $1 AND hit = false
                            """, position_id)

                        debug_log(f"🚫 Цели позиции ID={position_id} помечены как canceled")                        
                        # 🔹 Удаление позиции и целей из памяти
                        open_positions.pop(position_id, None)
                        targets_by_position.pop(position_id, None)
                        
                        debug_log(f"🧹 Позиция ID={position_id} и её цели удалены из памяти")
                        
                        debug_log(f"🚫 Позиция ID={position_id} полностью закрыта по TP (tp-full-hit)")
                        # 🔹 Лог в system_logs: tp-full-hit
                        try:
                            log_details = json.dumps({
                                "position_id": position_id,
                                "tp_price": str(tp_price),
                                "pnl": str(position["pnl"]),
                                "quantity": str(position["quantity"])
                            })

                            async with db_pool.acquire() as conn:
                                await conn.execute("""
                                    INSERT INTO system_logs (
                                        level, message, source, details, action_flag
                                    ) VALUES (
                                        'INFO', $1, 'position_close_worker', $2, 'audit'
                                    )
                                """, "Позиция закрыта по TP (полностью)", log_details)

                            debug_log(f"🧾 Запись в system_logs: TP-full-hit для позиции ID={position_id}")

                        except Exception as e:
                            logging.warning(f"⚠️ Не удалось записать system_log для tp-full-hit: {e}")
                            
                    except Exception as e:
                        logging.error(f"❌ Ошибка при полном закрытии позиции ID={position_id}: {e}")
                        await redis_client.xack(stream_name, group_name, msg_id)
                        continue                    
                                                                                                                            
                await redis_client.xack(stream_name, group_name, msg_id)

        except Exception as e:
            logging.error(f"❌ Ошибка в position_close_loop: {e}")
            await asyncio.sleep(1)                                                                  
