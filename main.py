import asyncio
import logging
import signal
import time
from dataclasses import dataclass

import aiohttp
import aioodbc
import pyodbc
from aiohttp import web

import config
import etran_requests
import utils


@dataclass(order=True)
class RequestPacket:
    priority: int  # приоритет должен быть первым полем для работы PriorityQueue
    request_id: int
    body: str
    dos_counter: int


@dataclass()
class ResponsePacket:
    request_id: int
    is_error: bool
    body: bytes
    request_packet: RequestPacket


async def producer_db(queue_in, queue_out):
    """Наполняет очередь обработки queue_in запросами из БД"""
    task_name = "producer"

    db_conn = await aioodbc.connect(dsn=config.db_connection_string, autocommit=True)
    db_cursor = await db_conn.cursor()
    need_close = True

    try:
        while True:
            try:
                await db_cursor.execute(
                    "EXEC etran.GetRequestQueue @MaxCount=?", config.QUEUE_MAXSIZE
                )
                # забираем все записи, чтобы не держать курсор, если очередь заполнится
                rows = await db_cursor.fetchall()
                for row in rows:
                    request_body = None
                    request_type = row.TypeID
                    request_id = row.ID
                    request_priority = row.Priority
                    logging.info(
                        f"{task_name} id={request_id} type={request_type} priority={request_priority}"
                    )

                    # формируем тело запроса в соответствии с его типом
                    try:
                        if request_type in etran_requests.request_map:
                            request_body = etran_requests.request_map[request_type](
                                row.Query
                            )
                        else:
                            raise ValueError(f"Неизвестный тип запроса: {request_type}")
                    except ValueError as e:
                        # чтобы не получать некорректный запрос бесконечно, сразу помещаем ошибку в очередь ответов
                        logging.warning(f"{task_name} id={request_id} {repr(e)}")
                        await queue_out.put(
                            ResponsePacket(
                                request_id,
                                is_error=True,
                                body=repr(e).encode(),
                                request_packet=None,
                            )
                        )

                    # отправляем в очередь обработки запросов
                    if request_body is not None:
                        request_packet = RequestPacket(
                            request_priority, request_id, request_body, dos_counter=0
                        )
                        await queue_in.put(request_packet)

                # цикл работы producer'а закончился; засыпаем, чтобы не тиранить БД
                if len(rows) > 0:
                    sleep_for = config.DB_QUERYING_INTERVAL
                else:
                    sleep_for = config.DB_POLLING_INTERVAL
                logging.info(f"{task_name} going to sleep for {sleep_for}s")
                await db_polling_sleep.sleep(sleep_for)

            except pyodbc.Error as e:
                # перезапускаем producer'а, если соединение с БД прервалось
                if e.args[0] == "The cursor's connection has been closed.":
                    need_close = False
                    raise utils.RerunMeException(repr(e))
                else:
                    logging.error(f"{task_name} {repr(e)}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.error(f"{task_name} {repr(e)}")
    finally:
        # не пытаемся закрыть прерванное соединение
        if need_close:
            await db_cursor.close()
            await db_conn.close()


async def worker(queue_in, queue_out):
    """Разбирает очередь запросов queue_in, отправляет их в ЭТРАН, помещает ответы в queue_out"""
    global etran_is_down
    task_name = asyncio.current_task().get_name()

    conn = aiohttp.TCPConnector(ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn, raise_for_status=True) as session:
        while True:
            request_packet = await queue_in.get()
            request_id = request_packet.request_id

            # делаем инкрементальную паузу, если запрос повторно оказался в очереди из-за ошибки отказа в обслуживании
            if etran_is_down:
                sleep_for = config.SLEEP_ON_DOS_MAX
            else:
                sleep_for = min(
                    request_packet.dos_counter * config.SLEEP_ON_DOS,
                    config.SLEEP_ON_DOS_MAX,
                )
            if sleep_for > 0:
                logging.warning(
                    f"{task_name} id={request_id} going to sleep for {sleep_for}s "
                    f"because of {'an outage' if etran_is_down else 'DoS'}"
                )
                await asyncio.sleep(sleep_for)

            try:
                start_time = time.time()
                async with session.post(
                    config.etran_url,
                    data=request_packet.body,
                    headers=config.etran_headers,
                ) as response:
                    response_body = await response.read()

                    # with open("dump.xml", "wb") as f:
                    #     f.write(response_body)

                    # отправляем в очередь обработки ответов # TODO: пустое тело ответа?
                    logging.info(
                        f"{task_name} id={request_id}"
                        f" status={response.status} len={len(response_body)}"
                        f" duration={(time.time()-start_time)*1000:.0f}ms"
                        f" queue_in={queue_in.qsize()} queue_out={queue_out.qsize()}"
                    )
                    response_packet = ResponsePacket(
                        request_id, False, response_body, request_packet
                    )
                    await queue_out.put(response_packet)

            except asyncio.CancelledError:
                raise
            except aiohttp.ClientError as e:
                # в случае сетевой ошибки возвращаем запрос в очередь и делаем паузу
                logging.warning(
                    f"{task_name} id={request_id} going to sleep for "
                    f"{config.SLEEP_ON_DISCONNECT}s because of {repr(e)}"
                )
                await queue_in.put(request_packet)
                await asyncio.sleep(config.SLEEP_ON_DISCONNECT)

            # задачу нужно завершить при любом, даже неудачном исходе, иначе join() повиснет
            queue_in.task_done()


async def consumer_db(queue_in, queue_out):
    """Разбирает очередь ответов queue_out, записывает результаты в БД"""
    global etran_is_down
    task_name = "consumer"

    db_conn = await aioodbc.connect(dsn=config.db_connection_string, autocommit=True)
    db_cursor = await db_conn.cursor()
    need_close = True

    try:
        while True:
            response_packet = await queue_out.get()
            try:
                request_id = response_packet.request_id
                if response_is_error := response_packet.is_error:
                    # ошибка напрямую из producer'а
                    response_text = response_packet.body.decode()
                else:
                    # декодируем ответ
                    etran_response = etran_requests.decode_response(
                        response_packet.body
                    )
                    response_is_error = etran_response.is_error
                    response_text = etran_response.text

                if response_is_error and response_text.startswith("504"):
                    # возвращаем запрос в очередь и приостанавливаем обработку новых в случае остановки ЭТРАН
                    etran_is_down, return_to_queue = True, True
                elif (
                    response_is_error
                    and response_packet.request_packet is not None
                    and response_text.startswith(
                        "400 Дождитесь окончания предыдущего запроса от"
                    )
                ):
                    # возвращаем запрос в очередь в случае ошибки отказа в обслуживании
                    etran_is_down, return_to_queue = False, True
                    response_packet.request_packet.dos_counter += 1
                else:
                    # записываем ответ в БД
                    etran_is_down, return_to_queue = False, False
                    logging.info(
                        f"{task_name} id={request_id} is_error={response_is_error} len={len(response_text)}"
                        f"{f' error: {response_text}' if response_is_error else ''}"
                    )
                    if not response_text.startswith("<"):
                        response_text = f"<root>{response_text}</root>"
                    await db_cursor.execute(
                        "EXEC etran.SetRequestResponse @RequestID=?, @IsError=?, @Response=?",
                        request_id,
                        response_is_error,
                        response_text,
                    )

                if return_to_queue:
                    logging.warning(
                        f"{task_name} id={request_id} returning "
                        f"into the queue because of {response_text}"
                    )
                    await queue_in.put(response_packet.request_packet)

            except pyodbc.Error as e:
                # возвращаем ответ в очередь и перезапускаем consumer'а, если соединение с БД прервалось
                if e.args[0] == "The cursor's connection has been closed.":
                    await queue_out.put(response_packet)
                    need_close = False
                    raise utils.RerunMeException(repr(e))
                else:
                    logging.error(f"{task_name} {repr(e)}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.error(f"{task_name} {repr(e)}")

            # задачу нужно завершить при любом, даже неудачном исходе, иначе join() повиснет
            queue_out.task_done()
    finally:
        # не пытаемся закрыть прерванное соединение
        if need_close:
            await db_cursor.close()
            await db_conn.close()


async def main():
    global producers
    global workers
    global consumers

    # HTTP-сервер для получения внешних команд
    web_server = web.Server(web_handler)
    web_runner = web.ServerRunner(web_server)
    await web_runner.setup()
    web_site = web.TCPSite(web_runner, "localhost", config.HTTP_ENDPOINT_PORT)
    await web_site.start()

    # при запуске сбрасываем статусы всем ранее взятым, но не обработанным записям
    async with aioodbc.connect(
        dsn=config.db_connection_string, autocommit=True
    ) as db_conn:
        async with db_conn.cursor() as db_cursor:
            await db_cursor.execute("EXEC etran.ResetRequestStatuses")

    queue_in = asyncio.PriorityQueue(maxsize=config.QUEUE_MAXSIZE)
    queue_out = asyncio.Queue()

    producers = [
        asyncio.create_task(
            utils.rerun_on_exception(
                producer_db, queue_in, queue_out, sleep_for=config.SLEEP_ON_DISCONNECT
            )
        )
    ]
    workers = [
        asyncio.create_task(worker(queue_in, queue_out), name=f"worker-{i}")
        for i in range(config.WORKERS_COUNT)
    ]
    consumers = [
        asyncio.create_task(
            utils.rerun_on_exception(
                consumer_db, queue_in, queue_out, sleep_for=config.SLEEP_ON_DISCONNECT
            )
        )
    ]

    await asyncio.gather(*producers)
    await queue_in.join()
    await queue_out.join()
    await asyncio.gather(*consumers)


async def web_handler(request):
    # эндоинт, за который можно дёрнуть, чтобы разбудить продюсера
    if request.path == "/wakeup":
        logging.info("waking up the producer")
        db_polling_sleep.cancel_all()
    return web.Response(text="OK")


def terminate():
    db_polling_sleep.cancel_all()
    for t in producers:
        t.cancel()
    for t in workers:
        t.cancel()
    for t in consumers:
        t.cancel()


def signal_handler(sig, frame):
    db_polling_sleep.terminate = True
    raise KeyboardInterrupt


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=logging.DEBUG if config.DEBUG else logging.WARNING,
        filename="main.log",
        encoding="utf-8",
    )
    if config.DEBUG:
        logging.getLogger().addHandler(logging.StreamHandler())

    db_polling_sleep = utils.CancellableSleep()
    signal.signal(signal.SIGINT, signal_handler)

    producers = []
    workers = []
    consumers = []
    etran_is_down = False

    try:
        logging.warning("app start")
        asyncio.run(main())
    except KeyboardInterrupt as e:
        logging.warning("KeyboardInterrupt")
    except Exception as e:
        logging.error(f"{repr(e)}")
        raise
    finally:
        terminate()
