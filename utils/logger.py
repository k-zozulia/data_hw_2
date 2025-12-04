"""
Centralized logging configuration for ETL pipeline
"""

import logging
import sys
from datetime import datetime
from configs.config import LOGS_DIR


class ColoredFormatter(logging.Formatter):
    """Colored log formatter for console output"""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logger(name: str, log_file: str = None, level=logging.INFO):
    """Setup logger with both file and console handlers"""

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    # Format for logs
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_format = ColoredFormatter("%(levelname)s - %(message)s")

    # Console handler (colored)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler (if log_file provided)
    if log_file:
        log_path = LOGS_DIR / log_file
        file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

    return logger


def setup_pipeline_logger():
    """Setup main pipeline logger"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return setup_logger(
        "pipeline", log_file=f"pipeline_{timestamp}.log", level=logging.INFO
    )


def log_step(logger, step_name: str, func):
    """
    Decorator to log function execution
    """

    def wrapper(*args, **kwargs):
        logger.info(f"{'=' * 80}")
        logger.info(f"STEP: {step_name}")
        logger.info(f"{'=' * 80}")

        try:
            result = func(*args, **kwargs)
            logger.info(f"✓ {step_name} completed successfully")
            return result
        except Exception as e:
            logger.error(f"✗ {step_name} failed: {str(e)}", exc_info=True)
            raise

    return wrapper


def log_performance(logger, operation: str, duration: float, records: int = None):
    """Log performance metrics"""
    msg = f"⏱  {operation}: {duration:.3f}s"
    if records:
        msg += f" ({records} records, {records / duration:.0f} records/sec)"
    logger.info(msg)


def log_stats(logger, title: str, stats: dict):
    """Log statistics in formatted way"""
    logger.info(f"\n{title}")
    logger.info("-" * 60)
    for key, value in stats.items():
        logger.info(f"  • {key:30s}: {value}")
    logger.info("-" * 60)
