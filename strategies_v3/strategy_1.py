class Strategy1:
    def __init__(self):
        pass

    async def on_signal(self, task: dict, interface):
        ok, note = await interface.run_basic_checks(task)
        print(f"✅ Проверка: {ok}, Причина: {note}")

        if not ok:
            strategy_id = await interface.get_strategy_id_by_name(task["strategy"])
            log_id = int(task["log_id"])

            await interface.log_strategy_action(
                strategy_id=strategy_id,
                log_id=log_id,
                status="ignored_by_check",
                note=note
            )
            return

        print("📈 Продолжение логики...")