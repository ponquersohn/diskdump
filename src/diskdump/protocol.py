import struct

MSG_INIT         = 0x01
MSG_BATCH_HASHES = 0x02
MSG_BLOCK        = 0x03
MSG_DONE         = 0x04
MSG_BATCH_NEEDED = 0x10
MSG_RESULT       = 0x11
MSG_ERROR        = 0xFF


def send_msg(stream, msg_type: int, payload: bytes = b'') -> None:
    header = struct.pack('!IB', len(payload) + 1, msg_type)
    stream.write(header)
    if payload:
        stream.write(payload)
    stream.flush()


def recv_msg(stream) -> tuple:
    header = _read_exact(stream, 5)
    length, msg_type = struct.unpack('!IB', header)
    payload = _read_exact(stream, length - 1) if length > 1 else b''
    return msg_type, payload


async def async_send_msg(stream, msg_type: int, payload: bytes = b'') -> None:
    header = struct.pack('!IB', len(payload) + 1, msg_type)
    stream.write(header)
    if payload:
        stream.write(payload)
    await stream.drain()


async def async_recv_msg(stream) -> tuple:
    header = await _async_read_exact(stream, 5)
    length, msg_type = struct.unpack('!IB', header)
    payload = await _async_read_exact(stream, length - 1) if length > 1 else b''
    return msg_type, payload


def _read_exact(stream, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise EOFError(f'Expected {n} bytes, got {len(buf)}')
        buf.extend(chunk)
    return bytes(buf)


async def _async_read_exact(stream, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = await stream.read(n - len(buf))
        if not chunk:
            raise EOFError(f'Expected {n} bytes, got {len(buf)}')
        buf.extend(chunk)
    return bytes(buf)
