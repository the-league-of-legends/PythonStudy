__author__ = 'zouhl'

import os
import time
import logging

class Log:
    def __init__(self, log_path, log_level):
        self.log_level = log_level
        self.log_name = os.path.join(log_path, '{0}.log'.format(time.strftime("%Y-%m-%d")))

    def __print_console(self, level, message):
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(self.log_name, mode="a", encoding="utf-8")
        fh.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

        if level == 'info':
            logger.info(message)
        elif level == 'debug':
            logger.debug(message)
        elif level == 'warning':
            logger.warning(message)
        elif level == 'error':
            logger.error(message)
        logger.removeHandler(ch)
        logger.removeHandler(fh)

        fh.close()

    def debug(self, message):
        if self.log_level == "debug":
            self.__print_console('debug', message)

    def info(self, message):
        self.__print_console('info', message)

    def warning(self, message):
        self.__print_console('warning', message)

    def error(self, message):
        self.__print_console('error', message)