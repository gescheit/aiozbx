import asyncio
import logging
import time

from .data_protocol import SenderData, ZBXException

_logger = logging.getLogger(__name__)


class ZBXTrapperException(Exception):
    pass


class ZBXNotSupported(ZBXException):
    pass


class ZBXEmptyRead(ZBXException):
    pass


class Sender:
    def __init__(self, hostname, port, timeout: int=30):
        self.hostname = hostname
        self.port = port
        self.timeout = timeout

    async def send(self, data: list) -> dict:
        """

        :param data: [{"host": "host", "key": "key", "value": "olo", "clock": ts, "ns": 0}]
        :return: {'info': 'processed: 1; failed: 0; total: 1; seconds spent: 0.000075', 'response': 'success'}
        """
        return await asyncio.wait_for(self._send(data), timeout=self.timeout)

    async def _send(self, data: list) -> dict:
        reader, writer = await asyncio.open_connection(self.hostname, self.port)
        msg = SenderData.encode_msg(data)
        _logger.debug("write %s", msg)
        writer.write(msg)
        try:
            header = await reader.readexactly(5)
            if header != SenderData.header:
                raise ZBXException("received wrong header %s" % header)
        except asyncio.streams.IncompleteReadError:
            raise ZBXEmptyRead()
        data_size = await reader.readexactly(8)
        data_size = SenderData.pack_fmt.unpack(data_size)[0]
        data = await reader.readexactly(data_size)
        data = SenderData.decode_msg(data)
        writer.close()
        return data


async def main():
    sender = Sender("localhost", 10051)
    data = [{"host": "testhost",
             "key": "key[param1, param2]",
             "value": "OK",
             "clock": int(time.time()),
             "ns": 0}]

    res = await sender.send(data)
    print(res)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
