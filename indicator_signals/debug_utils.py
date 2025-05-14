import logging

# 🔸 Флаг режима отладки
DEBUG_MODE = True  # Включи True при разработке

def debug_log(message: str):
    if DEBUG_MODE:
        logging.info(message)