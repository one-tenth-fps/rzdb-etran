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


async def producer_db(db_cursor, queue_in, queue_out):
    """Наполняет очередь обработки queue_in запросами из БД"""
    task_name = "producer"

    while True:
        try:
            # Запрашиваем ровно недостающее до полной очереди количество записей. Это нужно на случай, если неожиданно
            # придут высокоприоритетные запросы, чтобы они быстро, как только освободятся воркеры, попали в очередь.
            await db_cursor.execute("EXEC etran.GetRequestQueue @MaxCount=?", config.QUEUE_MAXSIZE - queue_in.qsize())
            rows = await db_cursor.fetchall()
            for row in rows:
                request_id, request_type, request_priority, request_body = row.ID, row.TypeID, row.Priority, None
                logging.info(f"{task_name} id={request_id} type={request_type} priority={request_priority}")

                # формируем тело запроса в соответствии с его типом
                try:
                    if request_type in etran_requests.request_map:
                        request_body = etran_requests.request_map[request_type](row.Query)
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
                    request_packet = RequestPacket(request_priority, request_id, request_body, dos_counter=0)
                    await queue_in.put(request_packet)

            # цикл работы producer'а закончился; засыпаем, чтобы не тиранить БД
            sleep_for = config.DB_QUERYING_INTERVAL if len(rows) else config.DB_POLLING_INTERVAL
            logging.info(f"{task_name} going to sleep for {sleep_for}s")
            await db_polling_sleep.sleep(sleep_for)

        except Exception as e:
            logging.error(f"{task_name} {repr(e)}")
            if isinstance(e, pyodbc.Error):
                raise_on_pyodbc_disconnect(e)


async def worker(queue_in, queue_out):
    """Разбирает очередь запросов queue_in, отправляет их в ЭТРАН, помещает ответы в queue_out"""
    global etran_is_down
    task_name = asyncio.current_task().get_name()

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ttl_dns_cache=300),
        timeout=aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT),
        raise_for_status=True,
    ) as session:
        while True:
            request_packet = await queue_in.get()
            request_id = request_packet.request_id

            if (
                sleep_for := config.SLEEP_ON_DOS_MAX
                # делаем длинную паузу в случае остановки ЭТРАН
                if etran_is_down
                # делаем инкрементальную паузу, если запрос повторно оказался в очереди при ошибке отказа в обслуживании
                else min(
                    request_packet.dos_counter * config.SLEEP_ON_DOS,
                    config.SLEEP_ON_DOS_MAX,
                )
            ):
                logging.warning(
                    f"{task_name} id={request_id} going to sleep for {sleep_for}s "
                    f"because of {'an outage' if etran_is_down else 'DoS'}"
                )
                await asyncio.sleep(sleep_for)

            try:
                start_time = time.monotonic()
                async with session.post(
                    config.ETRAN_URL,
                    data=request_packet.body,
                    headers=config.ETRAN_HEADERS,
                ) as response:
                    response_body = await response.read()

                    # with open("dump.xml", "wb") as f:
                    #     f.write(response_body)

                    # отправляем в очередь обработки ответов # TODO: пустое тело ответа?
                    logging.info(
                        "%s id=%d status=%s len=%d duration=%.0fms queue_in=%d queue_out=%d",
                        task_name,
                        request_id,
                        response.status,
                        len(response_body),
                        (time.monotonic() - start_time) * 1000,
                        queue_in.qsize(),
                        queue_out.qsize(),
                    )
                    response_packet = ResponsePacket(request_id, False, response_body, request_packet)
                    await queue_out.put(response_packet)

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


async def consumer_db(db_cursor, queue_in, queue_out):
    """Разбирает очередь ответов queue_out, записывает результаты в БД"""
    task_name = "consumer"

    while True:
        try:
            response_packet = await queue_out.get()
            return_to_queue, request_id, response_is_error, response_text = decode_response_packet(response_packet)

            if return_to_queue:
                logging.warning(f"{task_name} id={request_id} returning to the queue because of {response_text}")
                await queue_in.put(response_packet.request_packet)
            else:
                logging.info(
                    f"{task_name} id={request_id} is_error={response_is_error} len={len(response_text)}"
                    f"{f' error: {response_text}' if response_is_error else ''}"
                )
                await db_cursor.execute(
                    "EXEC etran.SetRequestResponse @RequestID=?, @IsError=?, @Response=?",
                    request_id,
                    response_is_error,
                    response_text,
                )

        except Exception as e:
            logging.error(f"{task_name} {repr(e)}")
            if isinstance(e, pyodbc.Error):
                raise_on_pyodbc_disconnect(e)

        # задачу нужно завершить при любом, даже неудачном исходе, иначе join() повиснет
        queue_out.task_done()


def raise_on_pyodbc_disconnect(exc: pyodbc.Error):
    """Проверяет, вызвана ли ошибка БД разрывом соединения, и запрашивает реконнект"""
    if exc.args[0] == "The cursor's connection has been closed.":
        raise


async def db_runner(coro, *args, **kwargs):
    """Обёртка для корутин с (пере)подключением к БД"""
    while True:
        try:
            db_conn = await aioodbc.connect(dsn=config.DB_CONNECTION_STRING, autocommit=True)
            db_cursor = await db_conn.cursor()
            need_close = True

            await coro(db_cursor, *args, **kwargs)

        except pyodbc.Error as e:
            # перезапускаем корутину, если соединение с БД прервалось
            need_close = False
            logging.warning(f"rerunning {coro.__name__} after {config.SLEEP_ON_DISCONNECT}s sleep because of {repr(e)}")
            await asyncio.sleep(config.SLEEP_ON_DISCONNECT)
        finally:
            # не пытаемся закрыть прерванное соединение
            if need_close:
                await db_cursor.close()
                await db_conn.close()


def decode_response_packet(response_packet: ResponsePacket):
    """Разбирает ResponsePacket и определяет необходимость возврата в очередь"""
    global etran_is_down

    request_id = response_packet.request_id
    if response_is_error := response_packet.is_error:
        # ошибка напрямую из producer'а
        response_text = response_packet.body.decode()
    else:
        # декодируем ответ
        etran_response = etran_requests.decode_response(response_packet.body)
        response_is_error = etran_response.is_error
        response_text = etran_response.text

    if response_is_error and response_text.startswith("504"):
        # возвращаем запрос в очередь и приостанавливаем обработку новых в случае остановки ЭТРАН
        etran_is_down, return_to_queue = True, True
    elif (
        response_is_error
        and response_packet.request_packet is not None
        and response_text.startswith("400 Дождитесь окончания предыдущего запроса")
    ):
        # возвращаем запрос в очередь в случае ошибки отказа в обслуживании
        etran_is_down, return_to_queue = False, True
        response_packet.request_packet.dos_counter += 1
    else:
        # записываем ответ в БД
        etran_is_down, return_to_queue = False, False
        if not response_text.startswith("<"):
            response_text = f"<root>{response_text}</root>"

    return (return_to_queue, request_id, response_is_error, response_text)


async def reset_db_queue():
    """Сбрасывает статусы в БД всем ранее взятым, но не обработанным записям"""
    async with aioodbc.connect(dsn=config.DB_CONNECTION_STRING, autocommit=True) as db_conn:
        async with db_conn.cursor() as db_cursor:
            await db_cursor.execute("EXEC etran.ResetProcessingQueue")


async def init_web_server():
    """Запускает HTTP-сервер для получения внешних команд"""
    logging.info(f"starting HTTP server on port {config.HTTP_ENDPOINT_PORT}")
    web_runner = web.ServerRunner(web.Server(web_handler))
    await web_runner.setup()
    web_site = web.TCPSite(web_runner, "localhost", config.HTTP_ENDPOINT_PORT)
    await web_site.start()


async def web_handler(request):
    """Обрабатывает запросы к HTTP-серверу"""
    # эндпоинт, за который можно дёрнуть, чтобы разбудить продюсера
    if request.path == "/wakeup":
        logging.info("waking up the producer")
        db_polling_sleep.cancel_all()
    return web.Response(text="OK")


async def heartbeat():
    """Подаёт признаки жизни для сторожевого таймера"""
    while True:
        logging.info("heartbeat")
        # TODO: неблокирующая работа с FS
        with open(config.HEARTBEAT_PATH, "w") as f:
            f.write("")
        await asyncio.sleep(config.HEARTBEAT_INTERVAL)


def signal_handler(sig, frame):
    """Обрабатывает Ctrl+C"""
    db_polling_sleep.terminate = True
    raise KeyboardInterrupt


async def main():
    """Точка входа"""
    global etran_is_down
    global db_polling_sleep

    await init_web_server()
    await reset_db_queue()

    etran_is_down = False
    db_polling_sleep = utils.CancellableSleep()
    signal.signal(signal.SIGINT, signal_handler)

    queue_in = asyncio.PriorityQueue(maxsize=config.QUEUE_MAXSIZE)
    queue_out = asyncio.Queue()

    await asyncio.gather(
        asyncio.create_task(db_runner(producer_db, queue_in, queue_out)),
        asyncio.create_task(db_runner(consumer_db, queue_in, queue_out)),
        asyncio.create_task(heartbeat()),
        *(asyncio.create_task(worker(queue_in, queue_out), name=f"worker-{i+1}") for i in range(config.WORKERS_COUNT)),
    )


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=logging.DEBUG if config.DEBUG else logging.WARNING,
        filename="main.log",
        encoding="utf-8",
    )
    if config.DEBUG:
        logging.getLogger().addHandler(logging.StreamHandler())

    try:
        logging.warning("app start")
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.warning("KeyboardInterrupt")
    except Exception as e:
        logging.error(f"{repr(e)}")
