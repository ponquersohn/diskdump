import io
import struct

from diskdump.protocol import (
    MSG_BATCH_HASHES, MSG_BATCH_NEEDED, MSG_BLOCK, MSG_DONE, MSG_ERROR,
    MSG_INIT, MSG_RESULT, recv_msg, send_msg,
)


def test_send_recv_roundtrip():
    buf = io.BytesIO()
    send_msg(buf, MSG_INIT, b'hello')
    buf.seek(0)
    msg_type, payload = recv_msg(buf)
    assert msg_type == MSG_INIT
    assert payload == b'hello'


def test_send_recv_empty_payload():
    buf = io.BytesIO()
    send_msg(buf, MSG_DONE)
    buf.seek(0)
    msg_type, payload = recv_msg(buf)
    assert msg_type == MSG_DONE
    assert payload == b''


def test_send_recv_large_payload():
    data = b'x' * 1_000_000
    buf = io.BytesIO()
    send_msg(buf, MSG_BLOCK, data)
    buf.seek(0)
    msg_type, payload = recv_msg(buf)
    assert msg_type == MSG_BLOCK
    assert payload == data


def test_multiple_messages():
    buf = io.BytesIO()
    send_msg(buf, MSG_INIT, b'first')
    send_msg(buf, MSG_BATCH_HASHES, b'second')
    send_msg(buf, MSG_DONE)
    buf.seek(0)

    t1, p1 = recv_msg(buf)
    t2, p2 = recv_msg(buf)
    t3, p3 = recv_msg(buf)

    assert t1 == MSG_INIT and p1 == b'first'
    assert t2 == MSG_BATCH_HASHES and p2 == b'second'
    assert t3 == MSG_DONE and p3 == b''


def test_recv_eof_raises():
    buf = io.BytesIO(b'')
    try:
        recv_msg(buf)
        assert False, 'Should have raised'
    except EOFError:
        pass


def test_all_message_types():
    for msg_type in [MSG_INIT, MSG_BATCH_HASHES, MSG_BLOCK, MSG_DONE,
                     MSG_BATCH_NEEDED, MSG_RESULT, MSG_ERROR]:
        buf = io.BytesIO()
        send_msg(buf, msg_type, b'test')
        buf.seek(0)
        t, p = recv_msg(buf)
        assert t == msg_type
        assert p == b'test'


def test_message_wire_format():
    buf = io.BytesIO()
    send_msg(buf, MSG_INIT, b'AB')
    raw = buf.getvalue()
    length, msg_type = struct.unpack('!IB', raw[:5])
    assert length == 3
    assert msg_type == MSG_INIT
    assert raw[5:] == b'AB'
