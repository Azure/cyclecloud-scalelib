from time import sleep as _sleep
from datetime import datetime, timezone


class Clock:
    def time(self) -> float:
        return datetime.now(timezone.utc).timestamp()

    def sleep(self, n: float) -> None:
        return _sleep(n)


class MockClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def time(self) -> float:
        return self.now
    
    def sleep(self, n: float) -> None:
        self.now += n


_CLOCK = Clock()


def use_mock_clock() -> MockClock:
    global _CLOCK
    _CLOCK = MockClock()
    return _CLOCK


def time() -> float:
    return _CLOCK.time()


def sleep(n: float) -> None:
    return _CLOCK.sleep(n)
