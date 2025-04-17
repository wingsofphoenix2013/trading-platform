# ... предшествующий код до строки расчёта RSI остаётся без изменений ...

            # === Расчёт RSI ===
            rsi_period = int(settings.get('rsi', {}).get('period', 14))
            delta = df['close'].diff()
            gain = np.where(delta > 0, delta, 0)
            loss = np.where(delta < 0, -delta, 0)
            avg_gain = pd.Series(gain).rolling(window=rsi_period).mean()
            avg_loss = pd.Series(loss).rolling(window=rsi_period).mean()
            rs = avg_gain / avg_loss
            rsi_series = 100 - (100 / (1 + rs))
            rsi_value = round(rsi_series.iloc[-1], 2)
            print(f"[RSI] {symbol}: {rsi_value}", flush=True)

            # === Расчёт SMI ===
            try:
                smi_k = int(settings.get('smi', {}).get('k', 13))
                smi_d = int(settings.get('smi', {}).get('d', 5))
                smi_s = int(settings.get('smi', {}).get('s', 3))

                hl2 = (df['high'] + df['low']) / 2
                min_low = hl2.rolling(window=smi_k).min()
                max_high = hl2.rolling(window=smi_k).max()
                smi_raw = hl2 - (max_high + min_low) / 2
                smi_div = (max_high - min_low) / 2
                smi_base = 100 * smi_raw / smi_div.replace(0, np.nan)

                smi_ema = smi_base.ewm(span=smi_s).mean()
                smi_signal = smi_ema.ewm(span=smi_d).mean()

                smi_value = round(smi_ema.iloc[-1], 2)
                smi_sig_value = round(smi_signal.iloc[-1], 2)

                print(f"[SMI] {symbol}: {smi_value}, сигнальная: {smi_sig_value}", flush=True)

            except Exception as e:
                print(f"[ERROR] SMI calculation failed for {symbol}: {e}", flush=True)
                continue

            # === Обновление ohlcv_m5 ===
            update_query = """
                UPDATE ohlcv_m5
                SET rsi = $1, smi = $2, smi_signal = $3
                WHERE symbol = $4 AND open_time = $5
            """
            ts_dt = datetime.fromisoformat(ts_str)
            await pg_conn.execute(update_query, rsi_value, smi_value, smi_sig_value, symbol, ts_dt)
            print(f"[DB] RSI + SMI записаны в ohlcv_m5 для {symbol} @ {ts_str}", flush=True)

            # === Публикация в Redis ===
            publish_data = {
                "symbol": symbol,
                "rsi": rsi_value,
                "smi": smi_value,
                "smi_signal": smi_sig_value
            }
            await redis_client.publish(REDIS_CHANNEL_OUT, json.dumps(publish_data))
            print(f"[REDIS → {REDIS_CHANNEL_OUT}] Публикация RSI+SMI: {publish_data}", flush=True)