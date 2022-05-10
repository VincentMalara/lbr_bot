import logging
from logging.handlers import TimedRotatingFileHandler

from datetime import datetime

def main():
    fileh = logging.FileHandler(f"main_log_{datetime.now().strftime('%d_%m_%Y')}.log", "a")
    #fileh = TimedRotatingFileHandler(filename='main.log', when='D', interval=1, backupCount=3, encoding='utf-8', delay=False)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fileh.setFormatter(formatter)

    logger = logging.getLogger()  # root logger
    for hdlr in logger.handlers[:]:  # remove all old handlers
        logger.removeHandler(hdlr)
    logger.addHandler(fileh)
    logger.setLevel(logging.INFO)
    return logger
