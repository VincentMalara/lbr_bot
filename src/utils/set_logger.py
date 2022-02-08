import logging
from datetime import datetime

def main():
    fileh = logging.FileHandler(f"main_log_{datetime.now().strftime('%d_%m_%Y')}.log", "a")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fileh.setFormatter(formatter)

    logger = logging.getLogger()  # root logger
    for hdlr in logger.handlers[:]:  # remove all old handlers
        logger.removeHandler(hdlr)
    logger.addHandler(fileh)
    logger.setLevel(logging.INFO)
    return logger
