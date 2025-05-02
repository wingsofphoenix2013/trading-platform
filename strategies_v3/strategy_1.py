# 🔸 Стратегия v3: Strategy1
class Strategy1:
    def __init__(self, interface):
        self.interface = interface

    async def on_signal(self, task: dict) -> dict:
        # Всегда даёт разрешение на вход
        return {
            "action": "open"
        }