# Fault-Tolerant Ticketing System — Backend API & Payment Pipeline

A modular, high-concurrency **FastAPI** backend integrating **Redis** (`redis.asyncio`) for distributed seat locking, **RabbitMQ** (`pika`) for durable asynchronous payment event queues, and **PostgreSQL** (`ticketing_db` / `psycopg2`) for atomic financial transactions.

---

## 📌 Architecture & Components

1. **Redis Distributed Seat Locking**:
   - Atomic seat hold acquisition via `SET seat_lock:{show_id}:{seat_number} {user_id} EX 300 NX`.
   - Prevents race conditions and double bookings across concurrent users.
   - 5-minute TTL automatic expiration.

2. **RabbitMQ Event-Driven Payment Pipeline**:
   - `POST /seats/pay` receives payment confirmation payloads and enqueues `process_payment` events.
   - Durable RabbitMQ queue: `ticket_payments`.
   - Dead-Letter Exchange (`ticket_payments_dlx`) and Dead-Letter Queue (`ticket_payments_dlq`) for zero-loss error handling.

3. **Background Worker (`worker.py`)**:
   - Idempotently processes payment events from `ticket_payments`.
   - Executes atomic SQL transaction in PostgreSQL:
     - Pessimistic row locking on user wallet (`SELECT ... FOR UPDATE`).
     - Balance check (`balance >= amount`).
     - Balance deduction (`UPDATE wallets SET balance = balance - amount`).
     - Immutable audit record insertion in `wallet_transactions`.
     - Confirms reservation status in `bookings` and `show_seats`.
   - On success: Immediately deletes the Redis lock (`DEL seat_lock:{show_id}:{seat_number}`) and acknowledges message (`basic_ack`).
   - On failure / insufficient funds: Rolls back transaction, marks booking `FAILED`, deletes Redis lock, routes message to Dead-Letter Queue (`ticket_payments_dlq`), and acknowledges or rejects message.

---

## 🗂 Directory Layout

```
backend/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI application instance & lifespan manager
│   ├── config.py            # Environment configuration with pydantic-settings
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── seat.py          # Pydantic schemas for seat locking & inspection
│   │   └── payment.py       # Pydantic schemas for payment confirmation
│   ├── services/
│   │   ├── __init__.py
│   │   ├── cache.py         # Async Redis client and locking logic
│   │   └── queue.py         # RabbitMQ publisher service with DLX/DLQ declarations
│   └── routers/
│       ├── __init__.py
│       ├── seats.py         # POST /seats/lock and GET /seats/lock status routes
│       └── payments.py      # POST /seats/pay payment queueing route
├── worker.py                # Standalone background consumer worker executable
├── tests/
│   ├── __init__.py
│   ├── test_seats.py        # Async tests for seat locking & Redis operations
│   ├── test_payments.py     # Tests for POST /seats/pay endpoint
│   └── test_worker.py       # Tests for worker idempotency, ACID debit, & DLQ
├── requirements.txt         # Production and development dependencies
└── README.md
```

---

## 🚀 API Endpoints

### 1. Atomically Lock a Seat
- **Endpoint**: `POST /seats/lock`
- **Status Code**: `201 Created` on success, `409 Conflict` if already locked.

```json
{
  "show_id": "f0000001-0000-0000-0000-000000000001",
  "seat_number": "A1",
  "user_id": "a0000001-0000-0000-0000-000000000001"
}
```

---

### 2. Submit Payment Confirmation
- **Endpoint**: `POST /seats/pay`
- **Status Code**: `202 Accepted`

#### Request Body
```json
{
  "user_id": "a0000001-0000-0000-0000-000000000001",
  "show_id": "f0000001-0000-0000-0000-000000000001",
  "seat_number": "A1",
  "amount": 450.00,
  "idempotency_key": "PAY-BLR-20261012-001",
  "booking_id": "b0000001-0000-0000-0000-000000000001"
}
```

#### Response (`202 Accepted`)
```json
{
  "status": "QUEUED",
  "message": "Payment confirmation received and queued for atomic settlement.",
  "idempotency_key": "PAY-BLR-20261012-001",
  "user_id": "a0000001-0000-0000-0000-000000000001",
  "show_id": "f0000001-0000-0000-0000-000000000001",
  "seat_number": "A1",
  "amount": 450.0,
  "booking_id": "b0000001-0000-0000-0000-000000000001",
  "queue": "ticket_payments",
  "queued_at": "2026-10-12T15:00:00.000000Z"
}
```

---

### 3. Check Seat Lock Status
- **Endpoint**: `GET /seats/lock/{show_id}/{seat_number}`
- **Status Code**: `200 OK`

---

### 4. Health Check
- **Endpoint**: `GET /health`
- **Status Code**: `200 OK`

---

## ⚙️ Running the System

### 1. Install Dependencies
```bash
cd backend
pip install -r requirements.txt
```

### 2. Start FastAPI Server
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 3. Run Payment Consumer Worker
In a separate process:
```bash
python worker.py
```

---

## 🧪 Testing

Run all unit and integration tests:

```bash
pytest tests/ -v
```
