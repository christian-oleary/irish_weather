"""Logging configuration"""

import sys

from loguru import logger


class Logs:
    """Logging setup"""

    DEFAULT_FORMAT = ''.join(
        '<green>{time:YYYY-MM-DD HH:mm:ss}</green>|<level>{level: <8}</level>|'
        '<cyan>{name}</cyan>:<cyan>{line}</cyan>|<level>{message}</level>'
    )

    def __init__(self, enable=False):
        logger.remove()
        if enable:
            logger.enable(__name__)
        else:
            logger.disable(__name__)

    def log_to_stderr(
        self,
        log_format=DEFAULT_FORMAT,
        level='DEBUG',
        colorize=True,
        backtrace=False,
        diagnose=False,
        enqueue=True,
    ) -> 'Logs':
        """Set up logging to stderr"""
        logger.add(
            sys.stderr,
            format=log_format,
            level=level,
            colorize=colorize,
            backtrace=backtrace,
            diagnose=diagnose,
            enqueue=enqueue,
        )
        logger.enable(__name__)
        return self

    def log_to_file(
        self,
        sink='logs/irish_weather.log',
        level='DEBUG',
        backtrace=True,
        diagnose=True,
        rotation='30 MB',
        retention='14 days',
        compression='zip',
        enqueue=True
    ) -> 'Logs':
        """Set up logging to file"""
        logger.add(
            sink=sink,
            level=level,
            backtrace=backtrace,
            diagnose=diagnose,
            rotation=rotation,
            retention=retention,
            compression=compression,
            enqueue=enqueue,
        )
        logger.enable(__name__)
        return self
