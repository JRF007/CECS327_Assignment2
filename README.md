# Smart Parking Server – Assignment 2
## Overview
This project implements a concurrent smart parking system for CSULB featuring:
- Multithreaded TCP server
- Minimal RPC layer over TCP
- Asynchronous sensor update path
- Publish/Subscribe (pub/sub) event delivery
- Back-pressure handling
- Reservation expiration (TTL)
- Structured logging
- Configurable ports and thread pool size
The system supports both synchronous RPC request/response interactions and asynchronous event notifications.

# Build & Setup (Virtual Environment Required)
## 1. Create Virtual Environment
python3 -m venv .venv

## 2. Activate
macOS / Linux:
source .venv/bin/activate
Windows:
.venv\Scripts\activate

## 3. Install Dependencies
pip install -r requirements.txt\
This project uses only the Python standard library.
requirements.txt is included as required by the assignment.

# Running the System
## Start the Server

python3 server.py --config config.json

The server reads all ports and settings from config.json.

Start a Subscriber (Pub/Sub Client)

python3 subscriber_client.py --rpc-port 5003 --event-port 5005 --lot A

Start Sensor Simulator

python3 sensor_sim.py --port 5004 --lot A --rate 10

Test Text Protocol

nc 127.0.0.1 5001

Commands:

PING
LOTS
AVAIL A
RESERVE A ABC123
CANCEL A ABC123

Configuration

All runtime configuration is stored in config.json.

Example:

{
"text_port": 5001,
"rpc_port": 5003,
"sensor_port": 5004,
"event_port": 5005,
"backlog": 128,
"thread_pool_size": 32,
"reservation_ttl_sec": 300,
"subscriber_queue_max": 100,
"lots": [
{"id": "A", "capacity": 120},
{"id": "B", "capacity": 80}
]
}

Configurable parameters:

Ports for text, RPC, sensor, and event channels

Thread pool size

Listen backlog

Reservation TTL

Subscriber queue size

Lot capacities

RPC Layer
Framing Specification (Prevents TCP Sticking)

Each RPC message is:

[ 4-byte big-endian unsigned length ][ JSON payload ]

Implemented using:

struct.Struct("!I")

! = network byte order (big-endian)

I = unsigned 32-bit integer

This ensures complete message boundaries over TCP.

Wire Format (Marshalling)

Request:

{
"rpcId": 1,
"method": "reserve",
"args": ["A", "ABC123"]
}

Reply:

{
"rpcId": 1,
"result": true,
"error": null
}

Error Reply:

{
"rpcId": 1,
"result": null,
"error": {"type": "BadRequest", "message": "..."}
}

Encoding:

JSON

UTF-8

Big-endian 4-byte length prefix

RPC API

getLots() -> List<Lot>

getAvailability(lotId) -> int

reserve(lotId, plate) -> bool

cancel(lotId, plate) -> bool

subscribe(lotId) -> subId

unsubscribe(subId) -> bool

Timeout Policy

The RPC client enforces a per-RPC timeout (default: 2 seconds).

If a reply is not received within the timeout:

TimeoutError: RPC <method> timed out after 2.000s

This prevents indefinite blocking if the server is slow or unreachable.

Thread Model

The server uses a bounded thread pool architecture.

Threads Used

1 accept loop per port (text, RPC, sensor, event)

Bounded ThreadPoolExecutor for connection handlers

2 update worker threads for sensor queue

1 reservation reaper thread (TTL expiration)

1 pub/sub notifier thread

Why Bounded Thread Pool?

Prevents unbounded thread creation

Provides natural back-pressure under load

Controls memory and CPU usage

Improves stability during high concurrency

Concurrency & Synchronization

Each parking lot has its own lock (per-lot locking)

Prevents overbooking under concurrent reservations

Reduces contention compared to a global lock

Guarantees:

No race conditions

No capacity violations

Thread-safe state updates

Asynchronous Sensor Path

Sensors connect to the dedicated sensor_port and send:

UPDATE <lotId> <delta>

Example:

UPDATE A 1
UPDATE A -1

Design:

Sensor handler enqueues updates

Update worker threads consume queue

State is updated under lock

Events are published if free count changes

This decouples network I/O from state mutation and prevents blocking RPC traffic.

Publish / Subscribe Design
Subscription Flow

Client calls subscribe(lotId) via RPC

Server returns subId

Client opens separate TCP connection to event_port

Client sends:

SUB <subId>

Server attaches socket to subscription

Server pushes EVENT messages asynchronously

Event Format

EVENT <lotId> <free> <timestamp>

Example:

EVENT A 119 1700000000.12345

Timestamp uses time.time() (epoch seconds).

Back-Pressure Policy (Required)

Each subscriber has a bounded outbound queue:

queue.Queue(maxsize=subscriber_queue_max)

Policy when queue is full:

Drop oldest event

Insert newest event

Log event_drop

Rationale:

Prevent slow subscribers from blocking the system

Protect RPC latency

Maintain system responsiveness under fan-out load

Reservation Expiration

Reservations expire after reservation_ttl_sec

A background reaper thread:

Removes expired reservations

Publishes updated EVENT notifications

Ensures consistent availability state even if clients disconnect.

Design Summary

Server model: Bounded thread pool

Synchronization: Per-lot locks

Framing: 4-byte big-endian length prefix

Marshalling: JSON over UTF-8

Timeout: Client-enforced per-RPC timeout

Async path: Update queue + worker threads

Pub/Sub: Separate TCP event channel

Back-pressure: Bounded queue, drop oldest

Logging: Structured JSON logs
