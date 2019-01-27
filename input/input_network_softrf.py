import asyncio
import socket
import logging
import setproctitle

from data_hub.data_hub_item import DataHubItem
from input.input_module import InputModule

__author__ = "Thorsten Biermann"
__copyright__ = "Copyright 2015, Thorsten Biermann"
__email__ = "thorsten.biermann@gmail.com"


def recvfrom(loop, sock, n_bytes, fut=None, registed=False):
    fd = sock.fileno()
    if fut is None:
        fut = loop.create_future()
    if registed:
        loop.remove_reader(fd)

    try:
        data, addr = sock.recvfrom(n_bytes)
    except (BlockingIOError, InterruptedError):
        loop.add_reader(fd, recvfrom, loop, sock, n_bytes, fut, True)
    else:
        fut.set_result((data, addr))
    return fut

async def udp_server(loop, sock, data_hub):
    logger = logging.getLogger('InputNetworksoftrf')
    logger.info('udp_server')
    while True:
        data, addr = await recvfrom(loop, sock, 1024)
        data_string = data.decode().strip()
       # logger.info('Data received: {!r}'.format(data_string))
       # generate new data hub item and hand over to data hub
        data_hub_item = DataHubItem('nmea', data_string)
        data_hub.put(data_hub_item)

class InputNetworksoftrf(InputModule):
    """
    Input module that connects to ADS-B receiver that has an SBS1 interface, like dump1090.
    """


    def __init__(self, data_hub, host_name, port = 10110, message_types = None):
        # call parent constructor
        super().__init__(data_hub=data_hub)

        # configure logging
        self._logger = logging.getLogger('InputNetworksoftrf')
        self._logger.info('Initializing')

        # store parameters in object variables
        self._host_name = host_name
        self._port = port
        self._message_types = message_types
        self._data_hub = data_hub
    def run(self):
        self._logger.info('Running')
        loop = asyncio.get_event_loop()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(False)
        sock.bind(('', self._port))

        
        try:
            loop.run_until_complete(udp_server(loop, sock, self._data_hub))
        finally:
            loop.close()

