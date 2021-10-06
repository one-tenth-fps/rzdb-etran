import asyncio
import logging


class RerunMeException(Exception):
    pass


async def rerun_on_exception(coro, *args, sleep_for=0, **kwargs):
    while True:
        try:
            await coro(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except RerunMeException as e:
            logging.warning(f'reruning {coro.__name__} after {sleep_for}s sleep because of {repr(e)}')
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)


def xml_escape(val: str) -> str:
    return val.replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", "&apos;")


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
    n = val*10 + digit
    return f'{n:06d}'
