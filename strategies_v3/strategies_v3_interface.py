# 🔸 Универсальный интерфейс для стратегий v3

class StrategyInterface:
    def __init__(self, redis_client, database_url):
        # 🔸 Подключение к Redis и базе данных
        self.redis = redis_client
        self.database_url = database_url

    async def log_strategy_action(self, strategy_id: int, log_id: int, status: str, note: str, position_id: int = None):
        # 🔸 Заглушка логирования действия стратегии
        print(f"📝 Лог: strategy_id={strategy_id}, log_id={log_id}, status={status}, note={note}")