import logging
import sys
from typing import Optional

import colorlog
from logstash_async.handler import AsynchronousLogstashHandler


_loggers = {}


def setup_logger(
    name: str,
    level: int = logging.INFO,
    logstash_host: Optional[str] = None,
    logstash_port: int = 5959,
    enable_stdout: bool = True,
    enable_logstash: bool = True,
) -> logging.Logger:
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if logger.hasHandlers():
        logger.handlers.clear()

    if enable_stdout:
        console_handler = colorlog.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_formatter = colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s %(levelname)-8s%(reset)s %(blue)s[%(name)s]%(reset)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    if enable_logstash and logstash_host:
        logstash_handler = AsynchronousLogstashHandler(
            host=logstash_host,
            port=logstash_port,
            database_path=None,
        )
        logstash_handler.setLevel(level)
        logger.addHandler(logstash_handler)

    _loggers[name] = logger
    return logger


def get_logger(name: str) -> logging.Logger:
    if name in _loggers:
        return _loggers[name]
    return setup_logger(name)
