"""Process memory measurement for the WB_IDS benchmark.

``tracemalloc`` sees only Python's allocator, and pandas 3 keeps ``str`` columns
in Arrow buffers that Arrow allocates itself, so the pipeline's memory is
measured as resident set size instead, sampled in a background thread.
"""

import threading
import time
from types import TracebackType
from typing import Self

import psutil

from benchmarks.records import Machine, machine_info

SAMPLE_INTERVAL_S: float = 0.01


def rss_bytes() -> int:
    """Return this process's resident set size, in bytes."""
    return int(psutil.Process().memory_info().rss)


def machine() -> Machine:
    """Describe this machine, total memory included on every platform.

    Returns:
        ``benchmarks.records.machine_info()``, with ``memory_bytes`` from psutil
        where ``/proc/meminfo`` is not available (Windows, macOS).
    """
    info = machine_info()
    if info["memory_bytes"] is None:
        info["memory_bytes"] = int(psutil.virtual_memory().total)
    return info


class PeakRss:
    """Track the highest resident set size reached inside a ``with`` block.

    A background thread samples RSS every ``interval_s`` seconds, so a spike
    shorter than that can be missed; the start and end are always sampled.

    Attributes:
        start_bytes: RSS on entering the block.
        end_bytes: RSS on leaving it.
        peak_bytes: The highest RSS sampled in between.
    """

    def __init__(self, interval_s: float = SAMPLE_INTERVAL_S) -> None:
        self._interval_s = interval_s
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self.start_bytes = 0
        self.end_bytes = 0
        self.peak_bytes = 0

    def __enter__(self) -> Self:
        self.start_bytes = self.peak_bytes = rss_bytes()
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stop.set()
        self._thread.join()
        self.end_bytes = rss_bytes()
        self.peak_bytes = max(self.peak_bytes, self.end_bytes)

    @property
    def peak_delta_bytes(self) -> int:
        """How far RSS rose above its starting level inside the block."""
        return self.peak_bytes - self.start_bytes

    def _sample(self) -> None:
        while not self._stop.is_set():
            self.peak_bytes = max(self.peak_bytes, rss_bytes())
            time.sleep(self._interval_s)
