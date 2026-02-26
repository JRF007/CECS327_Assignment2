#!/usr/bin/env python3
from __future__ import annotations
import argparse
import json
import queue
import socket
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

def log_json(**fields):
    print(json.dumps(fields, separators=(",", ":"), ensure_ascii=False), flush=True)

@dataclass
class Config:
    text_port: int = 5001
    rpc_port: int = 5003
    sensor_port: int = 5004
    event_port: int = 5005
    backlog: int = 128
    thread_pool_size: int = 32
    reservation_ttl_sec: int = 300
    subscriber_queue_max: int = 100
    lots: List[Tuple[str, int]] = field(default_factory=lambda: [("A", 120), ("B", 80)])

    @staticmethod
    def from_file(path: str) -> "Config":
        c = Config()
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        for k, v in data.items():
            if k == "lots":
                c.lots = [(x["id"], int(x["capacity"])) for x in v]
            elif hasattr(c, k):
                setattr(c, k, v)
        return c


LEN_STRUCT = struct.Struct("!I")  # 4-byte length prefix

class FramingError(Exception):
    pass

def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("socket closed")
        buf.extend(chunk)
    return bytes(buf)

def send_frame(sock: socket.socket, obj: Dict[str, Any]) -> None:
    payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8") # encode payload
    sock.sendall(LEN_STRUCT.pack(len(payload)) + payload) # send payload

def recv_frame(sock: socket.socket, *, max_len: int = 4 * 1024 * 1024) -> Dict[str, Any]:
    hdr = _recv_exact(sock, LEN_STRUCT.size)
    (length,) = LEN_STRUCT.unpack(hdr)
    if length < 0 or length > max_len:
        raise FramingError(f"invalid frame length: {length}")
    payload = _recv_exact(sock, length)
    try:
        obj = json.loads(payload.decode("utf-8"))
    except Exception as e:
        raise FramingError(f"invalid JSON payload: {e}") from e
    if not isinstance(obj, dict):
        raise FramingError("frame JSON must be an object/dict")
    return obj
# shared memory state:
@dataclass
class Lot:
    id: str
    capacity: int
    occupied: int = 0
    reservations: Dict[str, float] = field(default_factory=dict)  # plate -> expiry
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def free(self) -> int:
        return max(0, self.capacity - self.occupied)

class ParkingState:
    def __init__(self, lots: List[Tuple[str, int]], reservation_ttl_sec: int):
        self._lots: Dict[str, Lot] = {lid: Lot(lid, cap) for lid, cap in lots}
        self.reservation_ttl_sec = reservation_ttl_sec

    def _get_lot(self, lot_id: str) -> Lot:
        if lot_id not in self._lots:
            raise KeyError(f"unknown lot: {lot_id}")
        return self._lots[lot_id]

    def _reap_expired_locked(self, lot: Lot, now: Optional[float] = None) -> int:
        if now is None:
            now = time.time()
        expired = [p for p, exp in lot.reservations.items() if exp <= now]
        if not expired:
            return 0
        for p in expired:
            del lot.reservations[p]
        lot.occupied = max(0, lot.occupied - len(expired))
        return len(expired)

    def snapshot_lots(self) -> List[dict]:
        out = []
        for lot in self._lots.values():
            with lot.lock: # Prevents two threads reserving the last spot simultaneously, and guarantees no overbooking
                self._reap_expired_locked(lot)
                out.append({"id": lot.id, "capacity": lot.capacity, "occupied": lot.occupied, "free": lot.free()})
        return out

    def availability(self, lot_id: str) -> int:
        lot = self._get_lot(lot_id)
        with lot.lock:
            self._reap_expired_locked(lot)
            return lot.free()

    def reserve(self, lot_id: str, plate: str) -> str:
        lot = self._get_lot(lot_id)
        now = time.time()
        with lot.lock:
            self._reap_expired_locked(lot, now)
            if plate in lot.reservations and lot.reservations[plate] > now:
                return "EXISTS"
            if lot.occupied >= lot.capacity:
                return "FULL"
            lot.reservations[plate] = now + self.reservation_ttl_sec
            lot.occupied += 1
            return "OK"

    def cancel(self, lot_id: str, plate: str) -> str:
        lot = self._get_lot(lot_id)
        now = time.time()
        with lot.lock:
            self._reap_expired_locked(lot, now)
            if plate not in lot.reservations:
                return "NOT_FOUND"
            del lot.reservations[plate]
            lot.occupied = max(0, lot.occupied - 1)
            return "OK"
    # used by sensor path
    def apply_update(self, lot_id: str, delta: int) -> Tuple[int, int]:
        lot = self._get_lot(lot_id)
        with lot.lock:
            self._reap_expired_locked(lot)
            old_free = lot.free()
            lot.occupied += int(delta)
            # Clamp policy
            if lot.occupied < 0:
                lot.occupied = 0
            if lot.occupied > lot.capacity:
                lot.occupied = lot.capacity
            new_free = lot.free()
            return old_free, new_free
    # Called by background reaper thread, and removes expired reservations and publishes events.
    def reap_all_expired(self) -> List[Tuple[str, int, int]]:
        changes = []
        now = time.time()
        for lot in self._lots.values():
            with lot.lock:
                old_free = lot.free()
                freed = self._reap_expired_locked(lot, now)
                if freed:
                    new_free = lot.free()
                    if new_free != old_free:
                        changes.append((lot.id, old_free, new_free))
        return changes
# non-blocking event system
@dataclass
class Subscriber:
    sub_id: int
    lot_id: str
    event_sock: Optional[socket.socket] = None
    outq: "queue.Queue[str]" = field(default_factory=queue.Queue)
    dropped: int = 0

class PubSub:
    def __init__(self, queue_max: int):
        self.queue_max = queue_max
        self._subs: Dict[int, Subscriber] = {}
        self._lock = threading.Lock()
        self._next_id = 1
        self._wakeup = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._notifier_loop, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wakeup.set()

    def subscribe(self, lot_id: str) -> int:
        with self._lock:
            sid = self._next_id
            self._next_id += 1
            self._subs[sid] = Subscriber(sub_id=sid, lot_id=lot_id, outq=queue.Queue(maxsize=self.queue_max))
            return sid

    def unsubscribe(self, sub_id: int) -> bool:
        with self._lock:
            sub = self._subs.pop(sub_id, None)
        if not sub:
            return False
        try:
            if sub.event_sock:
                sub.event_sock.close()
        except Exception:
            pass
        return True

    def attach_event_socket(self, sub_id: int, sock: socket.socket) -> bool:
        with self._lock:
            sub = self._subs.get(sub_id)
            if not sub:
                return False
            try:
                if sub.event_sock:
                    sub.event_sock.close()
            except Exception:
                pass
            sub.event_sock = sock
            return True

    def publish(self, lot_id: str, free: int, ts: float) -> None:
        msg = f"EVENT {lot_id} {free} {ts}\n"
        woke = False
        with self._lock:
            for sub in self._subs.values():
                if sub.lot_id != lot_id or sub.event_sock is None:
                    continue
                try:
                    sub.outq.put_nowait(msg)
                    woke = True
                except queue.Full:
                    # Back-pressure policy: drop oldest
                    try:
                        _ = sub.outq.get_nowait()
                    except Exception:
                        pass
                    try:
                        sub.outq.put_nowait(msg)
                    except Exception:
                        pass
                    sub.dropped += 1
                    log_json(type="event_drop", subId=sub.sub_id, lotId=sub.lot_id, ts=ts, dropped_total=sub.dropped)
                    woke = True
        if woke:
            self._wakeup.set()
    # Dedicated thread that waits for wake-up signal, drains subscriber queues, calls sock.sendall()
    def _notifier_loop(self) -> None:
        while not self._stop.is_set():
            self._wakeup.wait(timeout=0.5)
            self._wakeup.clear()

            with self._lock:
                subs = list(self._subs.values())

            for sub in subs:
                sock = sub.event_sock
                if sock is None:
                    continue
                for _ in range(50):
                    try:
                        msg = sub.outq.get_nowait()
                    except queue.Empty:
                        break
                    try:
                        sock.sendall(msg.encode("utf-8"))
                    except Exception:
                        log_json(type="subscriber_disconnect", subId=sub.sub_id, lotId=sub.lot_id, ts=time.time())
                        self.unsubscribe(sub.sub_id)
                        break

def _make_listener(host: str, port: int, backlog: int) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(backlog)
    return s

def handle_text_client(sock: socket.socket, addr, state: ParkingState, pubsub: PubSub):
    with sock:
        f = sock.makefile("r", encoding="utf-8", newline="\n")
        w = sock.makefile("w", encoding="utf-8", newline="\n")
        while True:
            line = f.readline()
            if not line:
                return
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            cmd = parts[0].upper()

            try:
                if cmd == "PING":
                    w.write("PONG\n"); w.flush()
                elif cmd == "LOTS":
                    w.write(json.dumps(state.snapshot_lots(), ensure_ascii=False) + "\n"); w.flush()
                elif cmd == "AVAIL" and len(parts) == 2:
                    w.write(str(state.availability(parts[1])) + "\n"); w.flush()
                elif cmd == "RESERVE" and len(parts) == 3:
                    lot_id, plate = parts[1], parts[2]
                    r = state.reserve(lot_id, plate)
                    log_json(type="reserve", lotId=lot_id, plate=plate, ts=time.time(), result=r)
                    w.write(r + "\n"); w.flush()
                    if r == "OK":
                        pubsub.publish(lot_id, state.availability(lot_id), time.time())
                elif cmd == "CANCEL" and len(parts) == 3:
                    lot_id, plate = parts[1], parts[2]
                    r = state.cancel(lot_id, plate)
                    log_json(type="cancel", lotId=lot_id, plate=plate, ts=time.time(), result=r)
                    w.write(r + "\n"); w.flush()
                    if r == "OK":
                        pubsub.publish(lot_id, state.availability(lot_id), time.time())
                else:
                    w.write("ERR\n"); w.flush()
            except KeyError:
                w.write("ERR\n"); w.flush()
            except Exception as e:
                log_json(type="error", where="text_client", ts=time.time(), msg=str(e))
                try:
                    w.write("ERR\n"); w.flush()
                except Exception:
                    return

def handle_rpc_client(sock: socket.socket, state: ParkingState, pubsub: PubSub):
    with sock:
        while True:
            try:
                req = recv_frame(sock)
            except EOFError:
                return
            except FramingError as e:
                try:
                    send_frame(sock, {"rpcId": 0, "result": None, "error": {"type": "FramingError", "message": str(e)}})
                except Exception:
                    pass
                return

            rpc_id = int(req.get("rpcId", 0))
            method = req.get("method")
            args = req.get("args", [])
            if not isinstance(method, str) or not isinstance(args, list):
                send_frame(sock, {"rpcId": rpc_id, "result": None, "error": {"type": "BadRequest", "message": "method must be str and args must be list"}})
                continue

            try:
                if method == "getLots":
                    result = state.snapshot_lots()
                elif method == "getAvailability":
                    result = int(state.availability(str(args[0])))
                elif method == "reserve":
                    lot_id, plate = str(args[0]), str(args[1])
                    r = state.reserve(lot_id, plate)
                    result = (r == "OK")
                    log_json(type="reserve", lotId=lot_id, plate=plate, ts=time.time(), result=r)
                    if r == "OK":
                        pubsub.publish(lot_id, state.availability(lot_id), time.time())
                elif method == "cancel":
                    lot_id, plate = str(args[0]), str(args[1])
                    r = state.cancel(lot_id, plate)
                    result = (r == "OK")
                    log_json(type="cancel", lotId=lot_id, plate=plate, ts=time.time(), result=r)
                    if r == "OK":
                        pubsub.publish(lot_id, state.availability(lot_id), time.time())
                elif method == "subscribe":
                    lot_id = str(args[0])
                    sub_id = pubsub.subscribe(lot_id)
                    result = int(sub_id)
                    log_json(type="subscribe", lotId=lot_id, subId=sub_id, ts=time.time())
                elif method == "unsubscribe":
                    sub_id = int(args[0])
                    result = bool(pubsub.unsubscribe(sub_id))
                    log_json(type="unsubscribe", subId=sub_id, ts=time.time(), result=result)
                else:
                    raise RuntimeError(f"unknown method: {method}")

                send_frame(sock, {"rpcId": rpc_id, "result": result, "error": None})
            except Exception as e:
                send_frame(sock, {"rpcId": rpc_id, "result": None, "error": {"type": type(e).__name__, "message": str(e)}})

def handle_sensor_client(sock: socket.socket, addr, update_q: "queue.Queue[Tuple[str,int,float]]"):
    with sock:
        f = sock.makefile("r", encoding="utf-8", newline="\n")
        while True:
            line = f.readline()
            if not line:
                return
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) == 3 and parts[0].upper() == "UPDATE":
                lot_id = parts[1]
                try:
                    delta = int(parts[2])
                except ValueError:
                    continue
                update_q.put((lot_id, delta, time.time())) # Decouples network from state mutation
                log_json(type="sensor_update_enq", lotId=lot_id, delta=delta, ts=time.time())
            else:
                log_json(type="sensor_bad_line", ts=time.time(), line=line)

def handle_event_client(sock: socket.socket, addr, pubsub: PubSub):
    f = sock.makefile("r", encoding="utf-8", newline="\n")
    line = f.readline()
    if not line:
        sock.close()
        return
    parts = line.strip().split()
    if len(parts) != 2 or parts[0].upper() != "SUB":
        try:
            sock.sendall(b"ERR\n")
        except Exception:
            pass
        sock.close()
        return

    try:
        sub_id = int(parts[1])
    except ValueError:
        sock.close()
        return

    if not pubsub.attach_event_socket(sub_id, sock):
        try:
            sock.sendall(b"ERR\n")
        except Exception:
            pass
        sock.close()
        return
        
    log_json(type="event_attach", subId=sub_id, ts=time.time())
    # Keep connection open; notifier thread writes events.
    # Block until client disconnects.
    try:
        while True:
            if f.readline() == "":
                break
    except Exception:
        pass
    pubsub.unsubscribe(sub_id)

def update_worker_loop(state: ParkingState, pubsub: PubSub, update_q: "queue.Queue[Tuple[str,int,float]]"):
    while True:
        lot_id, delta, ts = update_q.get()
        try:
            old_free, new_free = state.apply_update(lot_id, delta)
            log_json(type="sensor_update_apply", lotId=lot_id, delta=delta, ts=ts, old_free=old_free, new_free=new_free)
            if new_free != old_free:
                pubsub.publish(lot_id, new_free, time.time())
        except Exception as e:
            log_json(type="error", where="update_worker", ts=time.time(), msg=str(e))

def reaper_loop(state: ParkingState, pubsub: PubSub, interval_sec: float):
    while True:
        time.sleep(interval_sec)
        changes = state.reap_all_expired()
        now = time.time()
        for lot_id, old_free, new_free in changes:
            log_json(type="reservation_expired", lotId=lot_id, ts=now, old_free=old_free, new_free=new_free)
            pubsub.publish(lot_id, new_free, now)

def run_server(config_path: str, host: str = "0.0.0.0"):
    cfg = Config.from_file(config_path)
    state = ParkingState(cfg.lots, reservation_ttl_sec=cfg.reservation_ttl_sec)
    pubsub = PubSub(queue_max=cfg.subscriber_queue_max)
    pubsub.start()
    update_q: "queue.Queue[Tuple[str,int,float]]" = queue.Queue()

    # Two update workers
    for _ in range(2):
        threading.Thread(target=update_worker_loop, args=(state, pubsub, update_q), daemon=True).start()
    threading.Thread(target=reaper_loop, args=(state, pubsub, 2.0), daemon=True).start()
    listeners = {
        "text": _make_listener(host, cfg.text_port, cfg.backlog),
        "rpc": _make_listener(host, cfg.rpc_port, cfg.backlog),
        "sensor": _make_listener(host, cfg.sensor_port, cfg.backlog),
        "event": _make_listener(host, cfg.event_port, cfg.backlog),
    }
    log_json(
        type="server_start", ts=time.time(), host=host,
        text_port=cfg.text_port, rpc_port=cfg.rpc_port,
        sensor_port=cfg.sensor_port, event_port=cfg.event_port,
        thread_pool_size=cfg.thread_pool_size, backlog=cfg.backlog
    )
    executor = ThreadPoolExecutor(max_workers=cfg.thread_pool_size)

    def accept_loop(kind: str, listener: socket.socket):
        while True:
            client, addr = listener.accept()
            if kind == "text":
                executor.submit(handle_text_client, client, addr, state, pubsub)
            elif kind == "rpc":
                executor.submit(handle_rpc_client, client, state, pubsub)
            elif kind == "sensor":
                executor.submit(handle_sensor_client, client, addr, update_q)
            elif kind == "event":
                executor.submit(handle_event_client, client, addr, pubsub)

    for kind, listener in listeners.items():
        threading.Thread(target=accept_loop, args=(kind, listener), daemon=True).start()

    while True:
        time.sleep(3600)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--host", default="0.0.0.0")
    args = ap.parse_args()
    run_server(args.config, host=args.host)
