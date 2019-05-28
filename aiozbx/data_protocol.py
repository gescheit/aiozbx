import struct
import time
import json

ZBX_PROTO_VALUE_SENDER_DATA = "sender data"
ZBX_TCP_HEADER_DATA = b"ZBXD"
ZBX_TCP_HEADER_VERSION = b"\1"


class ZBXException(Exception):
    pass


class ZBXNotSupported(ZBXException):
    pass


class SenderData:
    pack_fmt = struct.Struct("<q")
    header = ZBX_TCP_HEADER_DATA + ZBX_TCP_HEADER_VERSION

    @classmethod
    def pack_msg(cls, msg):
        res = bytearray()
        res += cls.header
        res += cls.pack_fmt.pack(len(msg))
        res += msg
        return bytes(res)

    @classmethod
    def encode_msg(cls, data):
        req = {"request": ZBX_PROTO_VALUE_SENDER_DATA,
               "data": data,
               "clock": int(time.time()),
               "ns": 0}
        req = json.dumps(req, ensure_ascii=False).encode()
        msg = cls.pack_msg(req)
        return msg

    @staticmethod
    def decode_msg(msg):
        if msg == b"ZBX_NOTSUPPORTED":
            raise ZBXNotSupported()
        data = json.loads(msg.decode())
        return data
