# 🔸 Заглушка стратегии strategy_1
async def on_signal(task: dict, interface) -> dict:
    # Всегда даёт разрешение на вход
    return {
        "action": "open"
    }