# Fault-Tolerant-Ticketing-System
Ever had a booking fail at the last second? This FastAPI backend fixes that. It uses Redis to lock a seat for exactly 5 minutes so no one else can steal it, and a safe MySQL wallet for instant payments. It guarantees lightning-fast checkouts and zero double-bookings, even under heavy traffic.
