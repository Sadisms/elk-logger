import json
import logging
import sys
import threading
from typing import Any, Literal, Optional, List

from logstash_async.formatter import LogstashFormatter
from logstash_async.handler import AsynchronousLogstashHandler


_loggers = {}
_lock = threading.Lock()


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


class ConsoleFormatterWithExtra(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%', allowed_fields=None):
        super().__init__(fmt, datefmt, style)
        self.allowed_fields = allowed_fields if allowed_fields is not None else ['raw_json']
    
    def format(self, record):
        base_msg = super().format(record)
        
        extra_fields = {
            k: v for k, v in record.__dict__.items()
            if k in self.allowed_fields
        }
        
        if extra_fields:
            extra_str = ' '.join(f'{k}={v}' for k, v in extra_fields.items())
            return f"{base_msg} | {extra_str}"
        return base_msg


def setup_logger(
    name: str,
    level: int = logging.INFO,
    logstash_host: Optional[str] = None,
    logstash_port: int = 5959,
    enable_stdout: bool = True,
    enable_logstash: bool = True,
    environment: Literal["dev", "prod", "staging", "test"] = "prod",
    project_name: Optional[str] = None,
    stdout_extra_fields: Optional[List[str]] = None,
) -> logging.Logger:
    with _lock:
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
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            console_formatter = ConsoleFormatterWithExtra(
                fmt="[%(asctime)s][%(name)s][%(levelname)s][%(environment)s] %(message)s",
                datefmt="%m/%d/%Y %H:%M:%S",
                allowed_fields=(
                    stdout_extra_fields 
                    if stdout_extra_fields is not None else 
                    ['raw_json']
                ),
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
    with _lock:
        if name in _loggers:
            return _loggers[name]
    return setup_logger(name)


def truncate_large_data(obj, max_len=100):
    if isinstance(obj, dict):
        return {k: truncate_large_data(v, max_len) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [truncate_large_data(item, max_len) for item in obj]
    elif isinstance(obj, str) and len(obj) > max_len:
        return f"<truncated: {len(obj)} chars>"
    return obj


def get_extra_from_json(data: dict, max_len: int = 100) -> dict:
    return {
        "raw_json": json.dumps(truncate_large_data(data, max_len), ensure_ascii=False, indent=2),
    }
