import logging

# 🔸 Флаг режима отладки
DEBUG_MODE = False  # Включи True при разработке

def debug_log(message: str):
    if DEBUG_MODE:
        logging.info(message)