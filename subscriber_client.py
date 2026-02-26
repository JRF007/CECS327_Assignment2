#!/usr/bin/env python3
from __future__ import annotations
import argparse
import socket
from rpc_client import RPCClient

def main():
    ap = argparse.ArgumentParser() # Creates CLI parser
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--rpc-port", type=int, default=5001)
    ap.add_argument("--event-port", type=int, default=5003)
    ap.add_argument("--lot", default="A")
    args = ap.parse_args()

    rpc = RPCClient((args.host, args.rpc_port), timeout=2.0) # Opens TCP connection to RPC
    sub_id = rpc.subscribe(args.lot)
    print(f"subscribed lot={args.lot} subId={sub_id}", flush=True)

    ev = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Opens second TCP connection, that is seperate from RPC
    ev.connect((args.host, args.event_port))
    ev.sendall(f"SUB {sub_id}\n".encode("utf-8"))

    f = ev.makefile("r", encoding="utf-8", newline="\n")
    try:
        for line in f:
            line = line.strip()
            if line:
                print(line, flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            ev.close()
        except Exception:
            pass
        try:
            rpc.unsubscribe(sub_id)
        except Exception:
            pass
        rpc.close()

if __name__ == "__main__":
    main()
