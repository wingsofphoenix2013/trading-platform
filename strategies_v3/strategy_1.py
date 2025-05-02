class Strategy1:
    def __init__(self, interface):
        self.interface = interface

    async def on_signal(self, task: dict) -> dict:
        try:
            # Заглушка — всегда разрешает вход
            return {
                "action": "open"
            }
        except Exception as e:
            print(f"💥 Ошибка внутри Strategy1.on_signal: {e}")
            return {
                "action": "skip",
                "note": f"internal error: {str(e)}"
            }