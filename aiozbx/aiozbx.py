import asyncio
import json
import logging
import struct
import time

_logger = logging.getLogger(__name__)

ZBX_PROTO_VALUE_SENDER_DATA = "sender data"
ZBX_TCP_HEADER_DATA = b"ZBXD"
ZBX_TCP_HEADER_VERSION = b"\1"


class ZBXException(Exception):
    pass


class ZBXTrapperException(Exception):
    pass


class ZBXNotSupported(ZBXException):
    pass


class ZBXEmptyRead(ZBXException):
    pass


class Sender:
    pack_fmt = struct.Struct("<q")
    header = ZBX_TCP_HEADER_DATA + ZBX_TCP_HEADER_VERSION

    def __init__(self, hostname, port, timeout: int=30):
        self.hostname = hostname
        self.port = port
        self.timeout = timeout

    @classmethod
    def encode_msg(cls, msg: bytes):
        res = bytearray()
        res += cls.header
        res += cls.pack_fmt.pack(len(msg))
        res += msg
        return bytes(res)

    async def send(self, data: list) -> dict:
        """

        :param data: [{"host": "host", "key": "key", "value": "olo", "clock": ts, "ns": 0}]
        :return: {'info': 'processed: 1; failed: 0; total: 1; seconds spent: 0.000075', 'response': 'success'}
        """
        return await asyncio.wait_for(self._send(data), timeout=self.timeout)

    async def _send(self, data: list) -> dict:
        reader, writer = await asyncio.open_connection(self.hostname, self.port)
        req = {"request": ZBX_PROTO_VALUE_SENDER_DATA,
               "data": data,
               "clock": int(time.time()),
               "ns": 0}
        req = json.dumps(req, ensure_ascii=False).encode()
        msg = self.encode_msg(req)
        _logger.debug("write %s", msg)
        writer.write(msg)
        try:
            header = await reader.readexactly(5)
            if header != self.header:
                raise ZBXException("received wrong header %s" % header)
        except asyncio.streams.IncompleteReadError:
            raise ZBXEmptyRead()
        data_size = await reader.readexactly(8)
        data_size = self.pack_fmt.unpack(data_size)[0]
        data = await reader.readexactly(data_size)
        if data == b"ZBX_NOTSUPPORTED":
            raise ZBXNotSupported()
        writer.write(msg)
        writer.close()
        data = json.loads(data.decode())
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
