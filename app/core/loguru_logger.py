import sys
import logging
from pathlib import Path
from loguru import logger

# Global log level switcher
# Possible values: "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
DEFAULT_LEVEL = "DEBUG"

# Paths
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "project.log"

# Remove default handlers
logger.remove()

# Formatters
CONSOLE_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level:<8}</level> | "
    "<cyan>{file}:{function}:{line}</cyan> | "
    "<level>{message}</level>"
)

FILE_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | "
    "{file}:{function}:{line} | {message}"
)

# Console output
logger.add(
    sys.stdout,
    level=DEFAULT_LEVEL,
    format=CONSOLE_FORMAT,
    colorize=True,
)

# File logging
logger.add(
    str(LOG_FILE),
    level=DEFAULT_LEVEL,
    rotation="5 MB",
    retention=10,
    compression="zip",
    encoding="utf-8",
    format=FILE_FORMAT,
    backtrace=True,
    diagnose=True,
    enqueue=True,
)

# Intercept standard logging → loguru
class InterceptHandler(logging.Handler):
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        logger.opt(depth=6, exception=record.exc_info).log(
            level, record.getMessage()
        )

# Redirect all stdlib logging to loguru
logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

# External loggers uvicorn, asyncpg, sqlalchemy ...
external_loggers = [
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "asyncpg",
    "tortoise",
    "sqlalchemy",
    "sqlalchemy.engine",
]

for name in external_loggers:
    ext_logger = logging.getLogger(name)
    ext_logger.handlers.clear()
    ext_logger.addHandler(InterceptHandler())
    ext_logger.setLevel(DEFAULT_LEVEL)
    ext_logger.propagate = False


# Usage example
if __name__ == "__main__":
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    logger.critical("Critical message")

    # stdlib logging (redirected to loguru)
    std_logger = logging.getLogger("test")
    std_logger.warning("This warning from logging will be redirected to loguru")
