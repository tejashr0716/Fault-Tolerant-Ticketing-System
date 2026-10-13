import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["Authentication & Wallets"])


def get_db_connection():
    """Returns a direct psycopg2 connection to ticketing_db."""
    conn = psycopg2.connect(
        settings.postgres_dsn,
        cursor_factory=RealDictCursor,
    )
    return conn


class UserResponse(BaseModel):
    id: str
    name: str
    email: str
    phone: str
    balance: float
    role: str = "CUSTOMER"
    is_admin: bool = False


class LoginRequest(BaseModel):
    user_id: str


@router.get("/users", response_model=List[UserResponse])
def list_available_users():
    """Returns all seeded Bengaluru users with their real-time PostgreSQL wallet balances."""
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.id, u.name, u.email, u.phone, COALESCE(u.role, 'CUSTOMER') as role, COALESCE(w.balance, 0.00) as balance
                    FROM users u
                    LEFT JOIN wallets w ON u.id = w.user_id
                    ORDER BY (CASE WHEN u.role = 'ADMIN' THEN 0 ELSE 1 END), u.name ASC;
                    """
                )
                rows = cur.fetchall()
                return [
                    UserResponse(
                        id=str(r["id"]),
                        name=r["name"],
                        email=r["email"],
                        phone=r["phone"],
                        balance=float(r["balance"]),
                        role=r.get("role", "CUSTOMER"),
                        is_admin=bool(r.get("role") == "ADMIN" or "admin" in r["email"].lower()),
                    )
                    for r in rows
                ]
    except Exception as exc:
        logger.error("Failed to query users from PostgreSQL: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not retrieve users from database.",
        )


@router.get("/wallet/{user_id}")
def get_user_wallet(user_id: str):
    """Fetches live wallet balance from PostgreSQL for a specific user."""
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.id, u.name, COALESCE(w.balance, 0.00) as balance
                    FROM users u
                    LEFT JOIN wallets w ON u.id = w.user_id
                    WHERE u.id = %s;
                    """,
                    (user_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="User not found")
                return {
                    "user_id": str(row["id"]),
                    "name": row["name"],
                    "balance": float(row["balance"]),
                }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to query user wallet: %s", exc)
        raise HTTPException(status_code=500, detail="Database wallet query failed")


class RegisterRequest(BaseModel):
    name: str
    email: str
    phone: str
    initial_balance: Optional[float] = 5000.00



@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def register_user(req: RegisterRequest):
    """Registers a new user and provisions their ACID PostgreSQL wallet."""
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                # Check for existing email or phone
                cur.execute(
                    "SELECT id FROM users WHERE email = %s OR phone = %s;",
                    (req.email, req.phone),
                )
                if cur.fetchone():
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="An account with this email or phone number already exists.",
                    )

                # Insert user
                cur.execute(
                    """
                    INSERT INTO users (name, email, phone)
                    VALUES (%s, %s, %s)
                    RETURNING id, name, email, phone;
                    """,
                    (req.name, req.email, req.phone),
                )
                user = cur.fetchone()
                user_id = user["id"]

                # Provision ACID wallet
                deposit = max(0.0, float(req.initial_balance or 0.0))
                cur.execute(
                    """
                    INSERT INTO wallets (user_id, balance, currency)
                    VALUES (%s, %s, 'INR')
                    RETURNING balance;
                    """,
                    (user_id, deposit),
                )
                wallet = cur.fetchone()

                return UserResponse(
                    id=str(user["id"]),
                    name=user["name"],
                    email=user["email"],
                    phone=user["phone"],
                    balance=float(wallet["balance"]),
                )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("User registration failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Registration failed: {str(exc)}",
        )


class SendOtpRequest(BaseModel):
    phone_or_email: str


class VerifyOtpRequest(BaseModel):
    phone_or_email: str
    otp: str
    name: Optional[str] = None
    initial_deposit: Optional[float] = 5000.00


@router.post("/send-otp")
def send_otp(req: SendOtpRequest):
    """Sends OTP for BookMyShow mobile/email login."""
    val = req.phone_or_email.strip()
    if not val:
        raise HTTPException(status_code=400, detail="Please enter a valid mobile number or email.")

    digits = "".join(ch for ch in val if ch.isdigit())
    phone_suffix = digits[-10:] if len(digits) >= 10 else digits

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name FROM users 
                    WHERE phone = %s OR email = %s 
                       OR REPLACE(REPLACE(phone, ' ', ''), '-', '') LIKE %s;
                    """,
                    (val, val, f"%{phone_suffix}%" if phone_suffix else val),
                )
                user = cur.fetchone()
                return {
                    "status": "OTP_SENT",
                    "exists": bool(user),
                    "user_name": user["name"] if user else None,
                    "otp_preview": "8942",
                    "message": f"OTP sent successfully to {val}. (Demo OTP: 8942)",
                }
    except Exception as exc:
        logger.error("Failed to send OTP: %s", exc)
        raise HTTPException(status_code=500, detail="OTP generation service error")


@router.post("/verify-otp", response_model=UserResponse)
def verify_otp(req: VerifyOtpRequest):
    """Verifies OTP and signs in or registers new user with ACID wallet."""
    import uuid
    val = req.phone_or_email.strip()
    otp = req.otp.strip()

    if otp not in ["8942", "1234", "0000"] and len(otp) != 4:
        raise HTTPException(status_code=400, detail="Invalid OTP code. Please enter 8942 to continue.")

    digits = "".join(ch for ch in val if ch.isdigit())
    phone_suffix = digits[-10:] if len(digits) >= 10 else digits

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                # 1. Search if user exists
                cur.execute(
                    """
                    SELECT u.id, u.name, u.email, u.phone, COALESCE(u.role, 'CUSTOMER') as role, COALESCE(w.balance, 0.00) as balance
                    FROM users u
                    LEFT JOIN wallets w ON u.id = w.user_id
                    WHERE u.phone = %s OR u.email = %s 
                       OR REPLACE(REPLACE(u.phone, ' ', ''), '-', '') LIKE %s;
                    """,
                    (val, val, f"%{phone_suffix}%" if phone_suffix else val),
                )
                user = cur.fetchone()

                if user:
                    return UserResponse(
                        id=str(user["id"]),
                        name=user["name"],
                        email=user["email"],
                        phone=user["phone"],
                        balance=float(user["balance"]),
                        role=user.get("role", "CUSTOMER"),
                        is_admin=bool(user.get("role") == "ADMIN" or "admin" in user["email"].lower()),
                    )

                # 2. If new user, create user and wallet
                name = req.name.strip() if req.name else f"Moviegoer {val[-4:] if len(val) >= 4 else 'BLR'}"
                if "@" in val:
                    email = val
                    phone = f"+91 98860{uuid.uuid4().hex[:5]}"
                else:
                    phone = val if val.startswith("+91") else f"+91 {val}"
                    email = f"user_{uuid.uuid4().hex[:6]}@bengaluru.in"

                cur.execute(
                    """
                    INSERT INTO users (name, email, phone)
                    VALUES (%s, %s, %s)
                    RETURNING id, name, email, phone;
                    """,
                    (name, email, phone),
                )
                new_user = cur.fetchone()
                user_id = new_user["id"]

                deposit = max(500.0, float(req.initial_deposit or 5000.0))
                cur.execute(
                    """
                    INSERT INTO wallets (user_id, balance, currency)
                    VALUES (%s, %s, 'INR')
                    RETURNING balance;
                    """,
                    (user_id, deposit),
                )
                new_wallet = cur.fetchone()

                return UserResponse(
                    id=str(new_user["id"]),
                    name=new_user["name"],
                    email=new_user["email"],
                    phone=new_user["phone"],
                    balance=float(new_wallet["balance"]),
                )
    except Exception as exc:
        logger.error("OTP verification failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Authentication failed: {str(exc)}")


class GoogleAuthRequest(BaseModel):
    google_id: Optional[str] = "google-user-108"
    name: Optional[str] = "Rahul Sharma (Google Account)"
    email: Optional[str] = "rahul.sharma@gmail.com"
    initial_deposit: Optional[float] = 5000.00


@router.post("/google", response_model=UserResponse)
def google_auth(req: GoogleAuthRequest):
    """
    One-Tap Google Authentication endpoint for BookMyShow.
    Retrieves or registers a verified Google user with an automatic ₹5,000 ACID wallet balance.
    """
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.id, u.name, u.email, u.phone, COALESCE(w.balance, 0.00) as balance
                    FROM users u
                    LEFT JOIN wallets w ON u.id = w.user_id
                    WHERE u.email = %s;
                    """,
                    (req.email,),
                )
                existing = cur.fetchone()
                if existing:
                    return UserResponse(
                        id=str(existing["id"]),
                        name=existing["name"],
                        email=existing["email"],
                        phone=existing["phone"] or "+91 9886012345",
                        balance=float(existing["balance"]),
                    )

                phone = "+91 9886012345"
                cur.execute(
                    """
                    INSERT INTO users (name, email, phone)
                    VALUES (%s, %s, %s)
                    RETURNING id, name, email, phone;
                    """,
                    (req.name, req.email, phone),
                )
                user = cur.fetchone()
                user_id = user["id"]

                deposit = max(0.0, float(req.initial_deposit or 5000.00))
                cur.execute(
                    """
                    INSERT INTO wallets (user_id, balance, currency)
                    VALUES (%s, %s, 'INR')
                    RETURNING balance;
                    """,
                    (user_id, deposit),
                )
                wallet = cur.fetchone()

                return UserResponse(
                    id=str(user["id"]),
                    name=user["name"],
                    email=user["email"],
                    phone=user["phone"],
                    balance=float(wallet["balance"]),
                )
    except Exception as exc:
        logger.error("Google authentication failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google authentication failed: {str(exc)}",
        )


@router.get("/bookings/{user_id}")
def get_user_bookings(user_id: str):
    """Returns booking history with cinema, seats, and movie details for a user."""
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT b.id, b.booking_reference, b.seat_count, b.total_amount, b.status, b.confirmed_at,
                           m.title as movie_title, m.poster_url, t.name as theater_name,
                           COALESCE(STRING_AGG(ss.seat_number, ', '), 'Confirmed') as seats
                    FROM bookings b
                    JOIN shows sh ON b.show_id = sh.id
                    JOIN movies m ON sh.movie_id = m.id
                    JOIN screens sc ON sh.screen_id = sc.id
                    JOIN theaters t ON sc.theater_id = t.id
                    LEFT JOIN show_seats ss ON ss.booking_id = b.id
                    WHERE b.user_id = %s
                    GROUP BY b.id, b.booking_reference, b.seat_count, b.total_amount, b.status, b.confirmed_at,
                             m.title, m.poster_url, t.name
                    ORDER BY b.confirmed_at DESC NULLS LAST;
                    """,
                    (user_id,),
                )
                rows = cur.fetchall()
                return [
                    {
                        "booking_id": str(r["id"]),
                        "booking_reference": r["booking_reference"],
                        "movie_title": r["movie_title"],
                        "theater_name": r["theater_name"],
                        "seats": r["seats"],
                        "seat_count": r["seat_count"],
                        "total_amount": float(r["total_amount"]),
                        "status": r["status"],
                        "confirmed_at": r["confirmed_at"].strftime("%d %b %Y, %I:%M %p") if r["confirmed_at"] else "Just now",
                    }
                    for r in rows
                ]
    except Exception as exc:
        logger.error("Failed to fetch user bookings: %s", exc)
        raise HTTPException(status_code=500, detail="Could not load bookings.")


class TopupRequest(BaseModel):
    user_id: str
    amount: float


@router.post("/wallet/topup")
def topup_user_wallet(req: TopupRequest):
    """Adds funds to the user's ACID wallet in PostgreSQL and records an audit log."""
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, balance, version
                    FROM wallets
                    WHERE user_id = %s
                    FOR UPDATE;
                    """,
                    (req.user_id,),
                )
                w = cur.fetchone()
                if not w:
                    raise HTTPException(status_code=404, detail="Wallet not found for this user.")

                old_balance = float(w["balance"])
                new_balance = old_balance + float(req.amount)

                cur.execute(
                    """
                    UPDATE wallets
                    SET balance = %s,
                        version = version + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s;
                    """,
                    (new_balance, w["id"]),
                )

                import uuid
                ref_num = f"TOPUP-{uuid.uuid4().hex[:10].upper()}"
                cur.execute(
                    """
                    INSERT INTO wallet_transactions (
                        wallet_id, user_id, transaction_type, amount,
                        balance_before, balance_after, idempotency_key, reference_number, status, description
                    ) VALUES (
                        %s, %s, 'TOPUP', %s, %s, %s, %s, %s, 'SUCCESS', 'ACID Wallet manual reload'
                    );
                    """,
                    (w["id"], req.user_id, req.amount, old_balance, new_balance, f"IDEM-{ref_num}", ref_num),
                )


                return {
                    "status": "SUCCESS",
                    "user_id": req.user_id,
                    "added_amount": req.amount,
                    "previous_balance": old_balance,
                    "new_balance": new_balance,
                    "reference_number": ref_num,
                }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Wallet topup failed: %s", exc)
        raise HTTPException(status_code=500, detail="Wallet topup failed.")


class CancelBookingRequest(BaseModel):
    user_id: str
    booking_id: str


@router.post("/bookings/cancel")
def cancel_booking_and_refund(req: CancelBookingRequest):
    """Cancels a confirmed booking, frees seats in show_seats, and refunds amount to user's ACID wallet in PostgreSQL."""
    import uuid
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                # 1. Lock and fetch booking
                cur.execute(
                    """
                    SELECT id, show_id, user_id, total_amount, status
                    FROM bookings
                    WHERE id = %s AND user_id = %s
                    FOR UPDATE;
                    """,
                    (req.booking_id, req.user_id),
                )
                b = cur.fetchone()
                if not b:
                    raise HTTPException(status_code=404, detail="Booking not found or not owned by user.")
                if b["status"] != "CONFIRMED":
                    raise HTTPException(status_code=400, detail=f"Booking is already {b['status']}.")

                refund_amount = float(b["total_amount"])

                # 2. Mark booking CANCELLED
                cur.execute(
                    """
                    UPDATE bookings
                    SET status = 'CANCELLED'
                    WHERE id = %s;
                    """,
                    (b["id"],),
                )

                # 3. Free show_seats
                cur.execute(
                    """
                    UPDATE show_seats
                    SET status = 'AVAILABLE', booking_id = NULL, user_id = NULL
                    WHERE booking_id = %s;
                    """,
                    (b["id"],),
                )

                # 4. Lock user wallet and credit refund
                cur.execute(
                    """
                    SELECT id, balance
                    FROM wallets
                    WHERE user_id = %s
                    FOR UPDATE;
                    """,
                    (req.user_id,),
                )
                w = cur.fetchone()
                new_bal = 0.0
                if w:
                    old_bal = float(w["balance"])
                    new_bal = old_bal + refund_amount
                    cur.execute(
                        """
                        UPDATE wallets
                        SET balance = %s, version = version + 1, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                        """,
                        (new_bal, w["id"]),
                    )

                    ref_num = f"REF-{uuid.uuid4().hex[:10].upper()}"
                    cur.execute(
                        """
                        INSERT INTO wallet_transactions (
                            wallet_id, user_id, transaction_type, amount,
                            balance_before, balance_after, idempotency_key, reference_number, status, description
                        ) VALUES (
                            %s, %s, 'BOOKING_REFUND', %s, %s, %s, %s, %s, 'SUCCESS', 'Ticket cancellation refund'
                        );
                        """,
                        (w["id"], req.user_id, refund_amount, old_bal, new_bal, f"IDEM-{ref_num}", ref_num),
                    )

                # Broadcast live event to all connected WebSocket clients
                try:
                    import urllib.request, json
                    req_payload = json.dumps({
                        "type": "SEAT_RELEASED",
                        "show_id": str(b["show_id"]),
                        "booking_id": req.booking_id
                    }).encode("utf-8")
                    req_b = urllib.request.Request(
                        "http://localhost:8000/api/live/broadcast",
                        data=req_payload,
                        headers={"Content-Type": "application/json"}
                    )
                    urllib.request.urlopen(req_b, timeout=1.0)
                except Exception:
                    pass

                return {
                    "status": "CANCELLED_AND_REFUNDED",
                    "booking_id": req.booking_id,
                    "refunded_amount": refund_amount,
                    "new_wallet_balance": new_bal,
                    "message": f"Ticket cancelled successfully. ₹{refund_amount:.2f} refunded to your ACID wallet.",
                }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Booking cancellation failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Cancellation error: {str(exc)}")



