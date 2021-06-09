import logging


def init_log(name=__name__):
    # create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # create formatter
    formatter = logging.Formatter('[%(processName)s-%(threadName)s] %(levelname)s - %(asctime)s - [%(filename)s:%(lineno)d] - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

    return logger
