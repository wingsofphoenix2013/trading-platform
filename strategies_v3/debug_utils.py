# 🔸 Флаг режима отладки
DEBUG_MODE = True  # Включай True при разработке

def debug_log(message: str):
    if DEBUG_MODE:
        import logging
        logging.info(message)