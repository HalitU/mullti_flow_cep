import pickle
import time
from typing import Any


def estimate_size_kbytes(data: list[Any]) -> float:
    return len(pickle.dumps(data)) / 1024.0


def get_elapsed_ms(start_time: int) -> float:
    return (time.perf_counter_ns() - start_time) * 1.0 / 1000000.0


def get_start_time() -> int:
    return time.perf_counter_ns()
