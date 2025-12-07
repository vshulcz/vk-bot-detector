import logging
import time

from contextlib import contextmanager


def setup_logger(
    level: int = logging.INFO,
    fmt: str = "[%(asctime)s] %(levelname)s %(name)s %(message)s",
    datefmt: str = "%Y-%m-%d %H:%M:%S",
    logfile: str | None = None,
) -> None:
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    root = logging.getLogger()
    root.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    if logfile:
        file_handler = logging.FileHandler(logfile, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    root.setLevel(level)


@contextmanager
def log_timer(
    msg: str,
    *,
    logger: logging.Logger,
    level: int = logging.DEBUG,
):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = (time.perf_counter() - t0) * 1000.0
        logger.log(level, f"{msg} in {dt:.1f} ms")
