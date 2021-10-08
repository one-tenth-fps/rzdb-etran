import asyncio
import logging


class CancellableSleep:
    """Обёртка asyncio.sleep, позволяющая прерывать сон"""

    def __init__(self):
        self.tasks = set()
        self.terminate = False

    async def sleep(self, delay, result=None):
        task = asyncio.create_task(asyncio.sleep(delay, result))
        self.tasks.add(task)
        try:
            return await task
        except asyncio.CancelledError as e:
            if self.terminate:
                raise
            else:
                return result
        finally:
            self.tasks.remove(task)

    def cancel_all(self):
        for t in self.tasks:
            t.cancel()


class RerunMeException(Exception):
    pass


async def rerun_on_exception(coro, *args, sleep_for=0, **kwargs):
    """Обёртка корутины, перезапускающая её, поймав RerunMeException"""
    while True:
        try:
            await coro(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except RerunMeException as e:
            logging.warning(
                f"rerunning {coro.__name__} after {sleep_for}s sleep because of {repr(e)}"
            )
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)


def xml_escape(val: str) -> str:
    return (
        val.replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def get_code6(val: int) -> str:
    s = 0
    n = val
    for i in range(5, 0, -1):
        s += n % 10 * i
        n //= 10
    digit = s % 11
    if digit == 10:
        s = 0
        n = val
        for i in range(7, 2, -1):
            s += n % 10 * i
            n //= 10
        digit = s % 11
        if digit == 10:
            digit = 0
    n = val * 10 + digit
    return f"{n:06d}"
