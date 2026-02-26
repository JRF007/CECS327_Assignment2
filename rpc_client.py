#!/usr/bin/env python3
from __future__ import annotations
import json
import socket
import struct
import threading
from typing import Any, Dict, Tuple

LEN_STRUCT = struct.Struct("!I") # This prevents TCP message sticking
class TimeoutError(Exception):
    pass
class FramingError(Exception):
    pass
# makes sure to read exactly n bytes or fail
def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n: # Keep reading until buffer has exactly n bytes
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("socket closed")
        buf.extend(chunk)
    return bytes(buf)
# Length-prefixed framing requirement
def send_frame(sock: socket.socket, obj: Dict[str, Any]) -> None:
    payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    sock.sendall(LEN_STRUCT.pack(len(payload)) + payload)

def recv_frame(sock: socket.socket, *, max_len: int = 4 * 1024 * 1024) -> Dict[str, Any]:
    hdr = _recv_exact(sock, LEN_STRUCT.size)
    (length,) = LEN_STRUCT.unpack(hdr)
    if length < 0 or length > max_len:
        raise FramingError(f"invalid frame length: {length}")
    payload = _recv_exact(sock, length)
    obj = json.loads(payload.decode("utf-8"))
    if not isinstance(obj, dict):
        raise FramingError("frame JSON must be an object/dict")
    return obj

class RPCClient:
    def __init__(self, addr: Tuple[str, int], timeout: float = 1.0):
        self.addr = addr
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(addr)

        self._lock = threading.Lock()
        self._next_id = 1
        self._pending: Dict[int, Tuple[threading.Event, Dict[str, Any]]] = {}

        self._reader = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader.start()

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass

    def _reader_loop(self):
        while True:
            try:
                rep = recv_frame(self.sock) # Waits for a reply frame
            except Exception:
                with self._lock:
                    for rpc_id, (ev, box) in self._pending.items():
                        box["__error__"] = {"type": "ConnectionError", "message": "connection closed"}
                        ev.set()
                    self._pending.clear()
                return

            rpc_id = int(rep.get("rpcId", 0)) # Extract rpcId from reply
            with self._lock:
                if rpc_id in self._pending: # Find the correct waiting RPC call and wake it up
                    ev, box = self._pending[rpc_id]
                    box["reply"] = rep
                    ev.set()

    def _call(self, method: str, args: list) -> Any:
        with self._lock:
            rpc_id = self._next_id
            self._next_id += 1
            ev = threading.Event() # Event allows thread to wait
            box: Dict[str, Any] = {} # will store reply
            self._pending[rpc_id] = (ev, box)
            send_frame(self.sock, {"rpcId": rpc_id, "method": method, "args": args})

        if not ev.wait(self.timeout):
            with self._lock:
                self._pending.pop(rpc_id, None)
            raise TimeoutError(f"RPC {method} timed out after {self.timeout:.3f}s")

        with self._lock:
            self._pending.pop(rpc_id, None)

        if "__error__" in box:
            raise ConnectionError(box["__error__"]["message"])

        rep = box["reply"]
        err = rep.get("error")
        if err:
            raise RuntimeError(f"{err.get('type')}: {err.get('message')}")
        return rep.get("result")

    def getLots(self):
        return self._call("getLots", [])

    def getAvailability(self, lotId: str) -> int:
        return int(self._call("getAvailability", [lotId]))

    def reserve(self, lotId: str, plate: str) -> bool:
        return bool(self._call("reserve", [lotId, plate]))

    def cancel(self, lotId: str, plate: str) -> bool:
        return bool(self._call("cancel", [lotId, plate]))

    def subscribe(self, lotId: str) -> int:
        return int(self._call("subscribe", [lotId]))

    def unsubscribe(self, subId: int) -> bool:
        return bool(self._call("unsubscribe", [int(subId)]))
