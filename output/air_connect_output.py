import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from threading import Lock

from data_hub.data_hub_item import DataHubItem
from output.output_module import OutputModule

__author__ = "Thorsten Biermann"
__copyright__ = "Copyright 2015, Thorsten Biermann"
__email__ = "thorsten.biermann@gmail.com"


async def input_processor(loop, data_input_queue):
    logger = logging.getLogger('AirConnectOutput.InputProcessor')

    while True:
        # get executor that can run in the background (and is asyncio-enabled)
        executor = ThreadPoolExecutor()

        # get new item from data hub
        data_hub_item = await loop.run_in_executor(executor, data_input_queue.get)

        # check if item is a poison pill
        if data_hub_item is None:
            logger.debug('Received poison pill')

            # exit loop
            break

        if type(data_hub_item) is DataHubItem:
            logger.debug('Received ' + str(data_hub_item))


class AirConnectServerClientProtocol(asyncio.Protocol):
    def __init__(self, clients, clients_lock):
        self._logger = logging.getLogger('AirConnectOutput.Server')
        self._logger.debug('Initializing')

        # store arguments in object variables
        self._clients = clients
        self._clients_lock = clients_lock

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        self._logger.debug('New connection from {}'.format(peername))

        # keep transport object
        self._transport = transport

        # add this client to global client set
        with self._clients_lock:
            self._clients.add(self)

    def connection_lost(self, exc):
        self._logger.debug('Connection closed to {}'.format(self._transport.get_extra_info('peername')))

        # remove this client from global client set
        with self._clients_lock:
            self._clients.remove(self)

    def data_received(self, data):
        message = data.decode()

        message_strip_lower = message.strip().lower()

        if message_strip_lower == 'exit':
            self._transport.close()
        elif message_strip_lower == 'list_clients':
            self._transport.write(str.encode(str(self._clients) + '\r\n'))
        else:
            self._transport.write(data)


class AirConnectOutput(OutputModule):
    def __init__(self):
        # call parent constructor
        super().__init__()

        # configure logging
        self._logger = logging.getLogger('AirConnectOutput')
        self._logger.info('Initializing')

    def run(self):
        self._logger.info('Running')

        # get asyncio loop
        loop = asyncio.get_event_loop()

        # initialize client set
        clients_lock = Lock()
        clients = set()

        # create server coroutine
        air_connect_server = loop.create_server(lambda: AirConnectServerClientProtocol(clients=clients, clients_lock=clients_lock), host='127.0.0.1', port=2000)

        # compile task list that will run in loop
        tasks = [
            asyncio.ensure_future(input_processor(loop=loop, data_input_queue=self._data_input_queue)),
            asyncio.ensure_future(air_connect_server)
        ]

        try:
            # start loop
            loop.run_until_complete(asyncio.wait(tasks))
        except(KeyboardInterrupt, SystemExit):
            pass
        finally:
            air_connect_server.close()
            loop.stop()

        # close data input queue
        self._data_input_queue.close()

        self._logger.info('Terminating')

    def get_desired_content_types(self):
        return(['ANY'])
