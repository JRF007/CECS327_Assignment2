#!/usr/bin/env python3
"""
sensor_sim.py - Sensor simulator

Connects to sensor_port and sends:
  UPDATE <lotId> <delta>\n

Example:
  python3 sensor_sim.py --host 127.0.0.1 --port 5002 --lot A --lot B --rate 10
"""

from __future__ import annotations

import argparse
import random
import socket
import time

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5002)
    ap.add_argument("--lot", action="append", default=["A"])
    ap.add_argument("--rate", type=float, default=10.0, help="updates/sec per lot")
    ap.add_argument("--duration", type=float, default=0.0, help="seconds; 0=forever")
    args = ap.parse_args()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((args.host, args.port))

    period = 1.0 / max(args.rate, 0.001)
    start = time.time()

    try:
        while True:
            now = time.time()
            if args.duration and (now - start) >= args.duration:
                break
            for lot_id in args.lot:
                delta = random.choice([-1, +1])
                s.sendall(f"UPDATE {lot_id} {delta}\n".encode("utf-8"))
            time.sleep(period)
    finally:
        try:
            s.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()