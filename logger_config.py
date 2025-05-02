import logging
import colorlog # type: ignore
import sys


def get_logger(log_file: str = "task.log") -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    if logger.hasHandlers():
        return logger

    log_colors = {
        "DEBUG": "cyan",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "bld_red",
    }

    color_formatter = colorlog.ColoredFormatter(
        "%(blue)s%(asctime)s%(reset)s - %(log_color)s%(levelname)s%(reset)s - %(message)s",
        log_colors=log_colors
    )

    file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    stdout_handle = logging.StreamHandler(sys.stdout)
    stdout_handle.setFormatter(color_formatter)

    file_handle = logging.FileHandler(log_file, mode="a")
    file_handle.setFormatter(file_formatter)

    logger.addHandler(stdout_handle)
    logger.addHandler(file_handle)

    return logger