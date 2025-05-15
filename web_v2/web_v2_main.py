# 🔸 Детальная страница стратегии по имени
@app.get("/strategies/detail/{strategy_name}", response_class=HTMLResponse)
async def strategy_detail(request: Request, strategy_name: str, period: str = "all", page: int = 1):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # 🔹 Получение стратегии
        strategy = await conn.fetchrow("""
            SELECT id, name, human_name, deposit
            FROM strategies_v2
            WHERE name = $1
        """, strategy_name)
        
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")

        strategy_id = strategy["id"]
        deposit = float(strategy["deposit"] or 1)

        # 🔹 Определение периода фильтрации
        now_utc = datetime.utcnow()
        start_utc, end_utc = get_period_bounds(period, now_utc)

        # 🔹 Сбор статистики по закрытым сделкам
        query = """
            SELECT direction, COUNT(*) AS count, SUM(pnl) AS total_pnl,
                   COUNT(*) FILTER (WHERE pnl > 0) AS wins
            FROM positions_v2
            WHERE strategy_id = $1 AND status = 'closed'
            {time_filter}
            GROUP BY direction
        """
        if start_utc and end_utc:
            time_filter = "AND closed_at BETWEEN $2 AND $3"
            rows = await conn.fetch(query.format(time_filter=time_filter), strategy_id, start_utc.replace(tzinfo=None), end_utc.replace(tzinfo=None))
        else:
            time_filter = ""
            rows = await conn.fetch(query.format(time_filter=time_filter), strategy_id)

        total = sum(r["count"] for r in rows)
        long_trades = next((r["count"] for r in rows if r["direction"] == "long"), 0)
        short_trades = next((r["count"] for r in rows if r["direction"] == "short"), 0)
        wins = sum(r["wins"] for r in rows)
        total_pnl = sum(r["total_pnl"] or 0 for r in rows)

        winrate = f"{(wins / total * 100):.1f}%" if total else "n/a"
        roi = f"{(float(total_pnl) / deposit * 100):.1f}%" if total else "n/a"

        # 🔹 Получение открытых сделок
        open_positions = await conn.fetch("""
            SELECT id, symbol, created_at, entry_price, close_reason, pnl
            FROM positions_v2
            WHERE strategy_id = $1 AND status = 'open'
            ORDER BY created_at ASC
        """, strategy_id)

        # 🔹 Пагинация закрытых сделок
        limit = 20
        offset = (page - 1) * limit

        total_closed = await conn.fetchval("""
            SELECT COUNT(*) FROM positions_v2
            WHERE strategy_id = $1 AND status = 'closed'
        """, strategy_id)

        total_pages = (total_closed + limit - 1) // limit

        closed_positions = await conn.fetch("""
            SELECT id, symbol, entry_price, exit_price, close_reason, closed_at, pnl
            FROM positions_v2
            WHERE strategy_id = $1 AND status = 'closed'
            ORDER BY closed_at DESC
            LIMIT $2 OFFSET $3
        """, strategy_id, limit, offset)

        return templates.TemplateResponse("strategy_detail.html", {
            "request": request,
            "strategy": strategy,
            "period": period,
            "page": page,
            "total_pages": total_pages,
            "timezone": ZoneInfo("Europe/Kyiv"),
            "open_positions": open_positions,
            "closed_positions": closed_positions,
            "stats": {
                "total": total or "n/a",
                "long": long_trades or "n/a",
                "short": short_trades or "n/a",
                "winrate": winrate,
                "roi": roi,
            }
        })