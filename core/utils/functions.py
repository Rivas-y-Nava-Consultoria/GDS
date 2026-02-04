import logging

def get_logger(logger_name: str, level: int, fullpath: str = './core/logs/logfile.log') -> logging.Logger:

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if not logger.hasHandlers():
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)

        fl = logging.FileHandler(fullpath)
        fl.setLevel(level)
        fl.setFormatter(formatter)
       
        logger.addHandler(ch)
        logger.addHandler(fl)
    return logger