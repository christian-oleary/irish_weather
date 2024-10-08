"""Logging configuration"""

import sys

from loguru import logger


class Logs:
    """Logging setup"""

    DEFAULT_FORMAT = ''.join(
        '<green>{time:YYYY-MM-DD HH:mm:ss}</green>| <level>{level: <8}</level>|'
        '<cyan>{name}</cyan>:<cyan>{line: <3}</cyan>| <level>{message}</level>'
    )

    @classmethod
    def log_to_stderr(
        cls,
        log_format=DEFAULT_FORMAT,
        level='DEBUG',
        colorize=True,
        backtrace=False,
        diagnose=False,
        enqueue=True,
    ):
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

    @classmethod
    def log_to_file(
        cls,
        log_format=DEFAULT_FORMAT,
        sink='logs/irish_weather.log',
        level='DEBUG',
        backtrace=True,
        diagnose=True,
        rotation='30 MB',
        retention='14 days',
        compression='zip',
        enqueue=True,
    ):
        """Set up logging to file"""
        logger.add(
            sink=sink,
            format=log_format,
            level=level,
            backtrace=backtrace,
            diagnose=diagnose,
            rotation=rotation,
            retention=retention,
            compression=compression,
            enqueue=enqueue,
        )
