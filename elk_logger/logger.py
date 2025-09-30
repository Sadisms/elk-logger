import json
import logging
import sys
from typing import Any, Literal, Optional

import colorlog
from logstash_async.formatter import LogstashFormatter
from logstash_async.handler import AsynchronousLogstashHandler


_loggers = {}


class SafeLogstashFormatter(LogstashFormatter):
    def _serialize(self, message: dict) -> str:
        return json.dumps(message, ensure_ascii=self._ensure_ascii, default=self._json_default)
    
    def _json_default(self, obj: Any) -> str:
        try:
            return str(obj)
        except Exception:
            return f"<non-serializable: {type(obj).__name__}>"


class EnvironmentFilter(logging.Filter):
    def __init__(self, environment: str):
        super().__init__()
        self.environment = environment

    def filter(self, record):
        record.environment = self.environment
        return True


def setup_logger(
    name: str,
    level: int = logging.INFO,
    logstash_host: Optional[str] = None,
    logstash_port: int = 5959,
    enable_stdout: bool = True,
    enable_logstash: bool = True,
    environment: Literal["dev", "prod", "staging", "test"] = "dev",
    project_name: Optional[str] = None,
) -> logging.Logger:
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if logger.hasHandlers():
        logger.handlers.clear()

    env_filter = EnvironmentFilter(environment)
    logger.addFilter(env_filter)

    if enable_stdout:
        console_handler = colorlog.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_formatter = colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s %(levelname)-8s%(reset)s %(purple)s[%(environment)s]%(reset)s %(blue)s[%(name)s]%(reset)s %(message)s",
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
        if project_name:
            logstash_handler.setFormatter(
                SafeLogstashFormatter(
                    message_type=project_name,
                    extra_prefix=project_name,
                    extra={"environment": environment},
                )
            )
        logstash_handler.setLevel(level)
        logger.addHandler(logstash_handler)

    _loggers[name] = logger
    return logger


def get_logger(name: str) -> logging.Logger:
    if name in _loggers:
        return _loggers[name]
    return setup_logger(name)
