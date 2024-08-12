"""Unit test configuration"""

import pytest
from loguru import logger


@pytest.fixture(scope="session", autouse=True)
def setup_logger():
    """Configure Loguru logger"""
    logger.remove()
    logger.add(
        'tests/logs/test.log',
        level='DEBUG',
        backtrace=True,
        diagnose=True,
        colorize=True,
        rotation='10 MB',
        retention='10 days',
        compression='zip',
        enqueue=True,
    )
    yield
    logger.remove()
