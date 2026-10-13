import asyncio
import json
import logging
import re
import uuid
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor

from app.config import settings
from app.services.cache import get_redis_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["Catalog & Concurrency Simulation"])


def get_db_connection():
    """Returns a direct psycopg2 connection to ticketing_db."""
    return psycopg2.connect(
        settings.postgres_dsn,
        cursor_factory=RealDictCursor,
    )


# Seeded Bengaluru Data for high-fidelity BMS showcase
BENGALURU_THEATERS = [
    {
        "id": "c0000001-0000-0000-0000-000000000001",
        "name": "PVR: The Forum Mall, Koramangala",
        "location": "Hosur Main Road, Koramangala, Bengaluru",
        "screens": ["Audi 1 (IMAX Laser)", "Audi 2 (4DX)", "Audi 3 (Gold Class)", "Audi 4 (Dolby Atmos)"],
        "amenities": ["M-Ticket", "F&B", "Parking", "Wheelchair Accessible", "Dolby Atmos 7.1"],
    },
    {
        "id": "c0000001-0000-0000-0000-000000000002",
        "name": "INOX Megaplex: Forum South Bengaluru",
        "location": "Konanakunte Cross, Kanakapura Road, Bengaluru",
        "screens": ["Audi 1 (IMAX with Laser)", "Audi 2 (Insignia Luxe)", "Audi 3 (MX4D)"],
        "amenities": ["M-Ticket", "Gourmet Dining", "Recliner Seats", "Free Wi-Fi"],
    },
    {
        "id": "c0000001-0000-0000-0000-000000000003",
        "name": "Cinepolis: Orion Mall, Rajajinagar",
        "location": "Dr. Rajkumar Road, Brigade Gateway, Rajajinagar, Bengaluru",
        "screens": ["Audi 1 (Macro XE)", "Audi 2 (VIP Club Class)", "Audi 3 (4DX)"],
        "amenities": ["M-Ticket", "VIP Service", "Dolby Atmos", "Food Court Access"],
    },
    {
        "id": "c0000001-0000-0000-0000-000000000004",
        "name": "PVR Superplex: Vega City Mall, Bannerghatta Road",
        "location": "Dollars Colony, Bannerghatta Main Road, BTM Layout, Bengaluru",
        "screens": ["Audi 1 (IMAX 4K Laser)", "Audi 2 (P[XL] Large Format)", "Audi 3 (Playhouse)"],
        "amenities": ["M-Ticket", "Valet Parking", "Dolby Atmos", "Recliners"],
    },
]

MOVIES = [
    {
        "id": "e0000001-0000-0000-0000-000000000001",
        "title": "Kantara: Chapter 1",
        "poster": "https://image.tmdb.org/t/p/w500/kXElm7wt2kAXEVwJqW4cFhP43nW.jpg",
        "banner": "https://image.tmdb.org/t/p/w1280/kXElm7wt2kAXEVwJqW4cFhP43nW.jpg",
        "rating": "9.4",
        "votes": "182.4K",
        "certification": "UA 16+",
        "duration": "2h 48m",
        "languages": ["Kannada", "Hindi", "Telugu", "Tamil", "Malayalam"],
        "genres": ["Action", "Thriller", "Mythology"],
        "formats": ["IMAX 2D", "4DX", "Dolby Atmos", "2D"],
        "synopsis": "Set in the mystical woodlands of Coastal Karnataka, the legend of Guliga and Panjurli Daiva returns to explore the origins of the divine bond between man, forest, and folklore.",
        "director": "Rishab Shetty",
        "music": "B. Ajaneesh Loknath",
        "trailer_url": "https://www.youtube.com/embed/gL6X296sZt4",
        "cast": [
            {"name": "Rishab Shetty", "role": "Kaadubettu Shiva"},
            {"name": "Rukmini Vasanth", "role": "Princess Leela"},
            {"name": "Achyuth Kumar", "role": "Devendra Suttoor"},
            {"name": "Prakash Raj", "role": "Tribal Chieftain"}
        ],
        "reviews": [
            {"user": "Girish M. (Indiranagar)", "rating": 10, "comment": "Mind-blowing theatrical experience in IMAX 4K! The climax goosebumps are unreal."},
            {"user": "Sneha R. (Koramangala)", "rating": 9, "comment": "Ajaneesh Loknath's background score elevates every folklore sequence."}
        ]
    },
    {
        "id": "e0000001-0000-0000-0000-000000000002",
        "title": "Kalki 2898 AD - Part 2",
        "poster": "https://image.tmdb.org/t/p/w500/rstcAnBeCkxNQjNp3YXrF6IP1tW.jpg",
        "banner": "https://image.tmdb.org/t/p/w1280/rstcAnBeCkxNQjNp3YXrF6IP1tW.jpg",
        "rating": "9.1",
        "votes": "245.8K",
        "certification": "UA",
        "duration": "3h 00m",
        "languages": ["Telugu", "Hindi", "Tamil", "Kannada", "Malayalam"],
        "genres": ["Sci-Fi", "Action", "Mythology"],
        "formats": ["IMAX 3D", "4DX 3D", "3D", "2D"],
        "synopsis": "The epic battle between Supreme Yaskin and the rebel forces of Shambala escalates as Bhairava and Ashwatthama discover the prophesied avatar's true destiny.",
        "director": "Nag Ashwin",
        "music": "Santhosh Narayanan",
        "trailer_url": "https://www.youtube.com/embed/kQDd1AhGIHk",
        "cast": [
            {"name": "Prabhas", "role": "Bhairava / Karna"},
            {"name": "Amitabh Bachchan", "role": "Ashwatthama"},
            {"name": "Kamal Haasan", "role": "Supreme Yaskin"},
            {"name": "Deepika Padukone", "role": "SUM-80"}
        ],
        "reviews": [
            {"user": "Arun K. (Whitefield)", "rating": 10, "comment": "Visual effects and mythological blending are pure Hollywood level!"}
        ]
    },
    {
        "id": "e0000001-0000-0000-0000-000000000003",
        "title": "Interstellar: 12th Anniversary IMAX",
        "poster": "https://image.tmdb.org/t/p/w500/gEU2QniE6E77NI6lCU6MxlNBvIx.jpg",
        "banner": "https://image.tmdb.org/t/p/w1280/xJHokMbljvjADYdit5fK5VQsXEG.jpg",
        "rating": "9.6",
        "votes": "312.9K",
        "certification": "UA",
        "duration": "2h 49m",
        "languages": ["English", "Hindi"],
        "genres": ["Sci-Fi", "Adventure", "Drama"],
        "formats": ["IMAX 70mm", "IMAX with Laser", "2D"],
        "synopsis": "A team of explorers travels through a wormhole in space in an attempt to ensure humanity's survival as Earth faces catastrophic blight.",
        "director": "Christopher Nolan",
        "music": "Hans Zimmer",
        "trailer_url": "https://www.youtube.com/embed/zSWdZVtXT7E",
        "cast": [
            {"name": "Matthew McConaughey", "role": "Cooper"},
            {"name": "Anne Hathaway", "role": "Dr. Brand"},
            {"name": "Jessica Chastain", "role": "Murph"},
            {"name": "Michael Caine", "role": "Professor Brand"}
        ],
        "reviews": [
            {"user": "Vivek S. (Jayanagar)", "rating": 10, "comment": "Watching the docking scene in IMAX 70mm is life-changing."}
        ]
    },
    {
        "id": "e0000001-0000-0000-0000-000000000004",
        "title": "Toxic: A Fairy Tale for Grown-ups",
        "poster": "https://image.tmdb.org/t/p/w500/tZ8eSBXbza96vzCYeVTG9RBJ78E.jpg",
        "banner": "https://image.tmdb.org/t/p/w1280/tZ8eSBXbza96vzCYeVTG9RBJ78E.jpg",
        "rating": "9.2",
        "votes": "94.6K",
        "certification": "A",
        "duration": "2h 35m",
        "languages": ["Kannada", "Hindi", "English"],
        "genres": ["Action", "Crime", "Drama"],
        "formats": ["IMAX 2D", "Dolby Atmos", "2D"],
        "synopsis": "A dark, gritty narrative centered around the illicit underworld of Bengaluru and the rise of an enigmatic anti-hero navigating loyalty and betrayal.",
        "director": "Geethu Mohandas",
        "music": "Charan Raj",
        "trailer_url": "https://www.youtube.com/embed/U2QpwbUeJ6U",
        "cast": [
            {"name": "Yash", "role": "Raya"},
            {"name": "Kiara Advani", "role": "Nisha"},
            {"name": "Nayanthara", "role": "Geetha"}
        ],
        "reviews": [
            {"user": "Praveen B. (Rajajinagar)", "rating": 9, "comment": "Yash in a completely unseen dark avatar. Exceptional cinematography."}
        ]
    },
    {
        "id": "e0000001-0000-0000-0000-000000000005",
        "title": "Devara: Part 1",
        "poster": "https://image.tmdb.org/t/p/w500/lQfuaXjANoTsdx5iS0gCXlK9D2L.jpg",
        "banner": "https://image.tmdb.org/t/p/w1280/lQfuaXjANoTsdx5iS0gCXlK9D2L.jpg",
        "rating": "9.0",
        "votes": "142.1K",
        "certification": "UA",
        "duration": "2h 58m",
        "languages": ["Telugu", "Kannada", "Hindi", "Tamil"],
        "genres": ["Action", "Drama", "Thriller"],
        "formats": ["4DX 3D", "IMAX 2D", "2D"],
        "synopsis": "A fearless protector from the coastal lands fights tooth and nail against treacherous sea pirates and smuggling syndicates to protect his community's dignity.",
        "director": "Koratala Siva",
        "music": "Anirudh Ravichander",
        "trailer_url": "https://www.youtube.com/embed/7Vp8d2Jt3Yg",
        "cast": [
            {"name": "NTR Jr.", "role": "Devara / Vara"},
            {"name": "Janhvi Kapoor", "role": "Thangam"},
            {"name": "Saif Ali Khan", "role": "Bhaira"}
        ],
        "reviews": [
            {"user": "Karthik R. (Malleshwaram)", "rating": 9, "comment": "Anirudh's high-voltage background score makes every single action scene roar!"}
        ]
    },
    {
        "id": "e0000001-0000-0000-0000-000000000006",
        "title": "Pushpa 2: The Rule",
        "poster": "https://image.tmdb.org/t/p/w500/1T21FblunT0y8fz7YaW8JMYgUKm.jpg",
        "banner": "https://image.tmdb.org/t/p/w1280/1T21FblunT0y8fz7YaW8JMYgUKm.jpg",
        "rating": "9.5",
        "votes": "389.2K",
        "certification": "UA 16+",
        "duration": "3h 10m",
        "languages": ["Telugu", "Hindi", "Kannada", "Tamil", "Malayalam"],
        "genres": ["Action", "Crime", "Thriller"],
        "formats": ["IMAX 2D", "Dolby Atmos", "2D"],
        "synopsis": "Pushpa Raj establishes undisputed dominion over the red sandalwood trade, clashing fiercely with SP Bhanwar Singh Shekhawat in a war of egos and bullets.",
        "director": "Sukumar",
        "music": "Devi Sri Prasad",
        "trailer_url": "https://www.youtube.com/embed/g3JUbgJBVis",
        "cast": [
            {"name": "Allu Arjun", "role": "Pushpa Raj"},
            {"name": "Rashmika Mandanna", "role": "Srivalli"},
            {"name": "Fahadh Faasil", "role": "SP Bhanwar Singh Shekhawat"}
        ],
        "reviews": [
            {"user": "Tejas M. (HSR Layout)", "rating": 10, "comment": "Mass elevation at its absolute peak! Allu Arjun delivers an award-winning performance."}
        ]
    },
    {
        "id": "e0000001-0000-0000-0000-000000000007",
        "title": "KGF: Chapter 3 - Rise of the Empire",
        "poster": "https://image.tmdb.org/t/p/w500/khNVygolU0TxLIDWff5tQlAhZ23.jpg",
        "banner": "https://image.tmdb.org/t/p/w1280/khNVygolU0TxLIDWff5tQlAhZ23.jpg",
        "rating": "9.7",
        "votes": "512.4K",
        "certification": "UA 16+",
        "duration": "2h 55m",
        "languages": ["Kannada", "Hindi", "Telugu", "Tamil"],
        "genres": ["Action", "Period Drama", "Thriller"],
        "formats": ["IMAX 4K Laser", "Dolby Atmos", "2D"],
        "synopsis": "The untold story of Rocky Bhai's secret international cartel across Italy, the Americas, and the Indian Ocean before the naval standoff.",
        "director": "Prashanth Neel",
        "music": "Ravi Basrur",
        "trailer_url": "https://www.youtube.com/embed/JKa05nyUmuQ",
        "cast": [
            {"name": "Yash", "role": "Raja Krishnappa Bairya (Rocky)"},
            {"name": "Sanjay Dutt", "role": "Adheera"},
            {"name": "Raveena Tandon", "role": "Ramika Sen"}
        ],
        "reviews": [
            {"user": "Sharath S. (Basavanagudi)", "rating": 10, "comment": "Prashanth Neel and Yash recreate lightning. Loud, relentless, and thrilling."}
        ]
    },
    {
        "id": "e0000001-0000-0000-0000-000000000008",
        "title": "Gladiator II",
        "poster": "https://image.tmdb.org/t/p/w500/2cxhvwyEwRlysAmRH4iodkvo0z5.jpg",
        "banner": "https://image.tmdb.org/t/p/w1280/euYIwmwkmz95mnXvufEmbL6ovhZ.jpg",
        "rating": "8.9",
        "votes": "120.5K",
        "certification": "A",
        "duration": "2h 30m",
        "languages": ["English", "Hindi"],
        "genres": ["Action", "Historical", "Drama"],
        "formats": ["IMAX 2D", "4DX", "2D"],
        "synopsis": "Years after witnessing the death of Maximus, Lucius enters the Colosseum after his home is conquered by the tyrannical Emperors of Rome.",
        "director": "Ridley Scott",
        "music": "Harry Gregson-Williams",
        "trailer_url": "https://www.youtube.com/embed/4rgYUipGJNo",
        "cast": [
            {"name": "Paul Mescal", "role": "Lucius"},
            {"name": "Pedro Pascal", "role": "Marcus Acacius"},
            {"name": "Denzel Washington", "role": "Macrinus"}
        ],
        "reviews": [
            {"user": "Naveen N. (Koramangala)", "rating": 9, "comment": "Colosseum naval battle scene is mind-blowing in IMAX!"}
        ]
    },

]

FEATURED_BANNERS = [
    {
        "id": "b1",
        "badge": "BENGALURU PREMIERE • IMAX 4K",
        "title": "Kantara: Chapter 1",
        "subtitle": "UA 16+ • Action / Thriller / Mythology • 2h 48m",
        "cta": "🎟️ Book Tickets Now",
        "movie_id": "e0000001-0000-0000-0000-000000000001",
        "image": "https://image.tmdb.org/t/p/w1280/kXElm7wt2kAXEVwJqW4cFhP43nW.jpg",
    },
    {
        "id": "b2",
        "badge": "HIGH OCTANE 4DX • BLOCKBUSTER",
        "title": "Devara: Part 1",
        "subtitle": "NTR Jr., Janhvi Kapoor & Saif Ali Khan • In Cinemas Worldwide",
        "cta": "🎟️ Book 4DX 3D",
        "movie_id": "e0000001-0000-0000-0000-000000000005",
        "image": "https://image.tmdb.org/t/p/w1280/lQfuaXjANoTsdx5iS0gCXlK9D2L.jpg",
    },
    {
        "id": "b3",
        "badge": "LIVE CONCERT • PALACE GROUNDS",
        "title": "Arijit Singh Live in Bengaluru",
        "subtitle": "Exclusive Live Symphony at Bengaluru Palace Grounds • Limited Passes",
        "cta": "🎤 Book Concert Passes",
        "movie_id": "e0000001-0000-0000-0000-000000000001",
        "image": "https://images.unsplash.com/photo-1470225620780-dba8ba36b745?w=1200&auto=format&fit=crop&q=80",
    },
    {
        "id": "b4",
        "badge": "12TH ANNIVERSARY • 70MM RE-RELEASE",
        "title": "Interstellar: The IMAX Experience",
        "subtitle": "Christopher Nolan's Masterpiece returns to Orion & Vega City IMAX",
        "cta": "🚀 Book IMAX Experience",
        "movie_id": "e0000001-0000-0000-0000-000000000003",
        "image": "https://image.tmdb.org/t/p/w1280/xJHokMbljvjADYdit5fK5VQsXEG.jpg",
    },
]


class AdminCreateTheaterRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=200)
    location: str = Field(..., min_length=3, max_length=255)
    area: Optional[str] = None
    pincode: Optional[str] = "560001"
    brand: Optional[str] = None
    screens: Optional[List[str]] = None
    amenities: Optional[List[str]] = None


@router.get("/theaters")
async def get_theaters():
    """Returns Bengaluru multiplex theaters list from database with fallback."""
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, location, total_screens, metadata
                    FROM theaters
                    WHERE is_active = true
                    ORDER BY created_at DESC;
                    """
                )
                rows = cur.fetchall()
                if rows:
                    db_theaters = []
                    for r in rows:
                        meta = r.get("metadata") or {}
                        if isinstance(meta, str):
                            meta = json.loads(meta)
                        screens = meta.get("screens")
                        if not screens:
                            screens = [f"Audi {i+1} (Dolby Atmos)" for i in range(r.get("total_screens", 3))]
                        amenities = meta.get("amenities") or ["M-Ticket", "F&B", "Dolby Atmos", "Parking"]
                        db_theaters.append({
                            "id": str(r["id"]),
                            "name": r["name"],
                            "location": r["location"],
                            "screens": screens,
                            "amenities": amenities,
                        })
                    return {"theaters": db_theaters}
    except Exception as exc:
        logger.warning("Could not load theaters from DB, falling back to in-memory: %s", exc)
    return {"theaters": BENGALURU_THEATERS}


@router.post("/admin/theaters")
async def create_admin_theater(request: AdminCreateTheaterRequest):
    """
    Admin endpoint to add a new cinema in Bengaluru.
    Validates Bengaluru pincode, persists to PostgreSQL, and broadcasts real-time WS event.
    """
    pincode = (request.pincode or "560001").strip()
    if not re.match(r"^560\d{3}$", pincode):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pincode must be a valid 6-digit Bengaluru PIN code starting with 560 (e.g. 560001, 560066).",
        )

    name = request.name.strip()
    location = request.location.strip()
    area = (request.area or "Bengaluru").strip()

    brand = request.brand
    if not brand:
        if "pvr" in name.lower():
            brand = "PVR"
        elif "inox" in name.lower():
            brand = "INOX"
        elif "cinepolis" in name.lower():
            brand = "Cinepolis"
        elif "miraj" in name.lower():
            brand = "Miraj"
        else:
            brand = "Multiplex"

    screens = request.screens or [
        "Audi 1 (IMAX Laser)",
        "Audi 2 (4DX 3D)",
        "Audi 3 (Dolby Atmos)",
    ]
    amenities = request.amenities or [
        "M-Ticket",
        "F&B Available",
        "Dolby Atmos",
        "Parking",
        "Wheelchair Accessible",
    ]

    new_id = str(uuid.uuid4())
    total_screens = max(1, len(screens))
    metadata_obj = {"screens": screens, "amenities": amenities}

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO theaters (id, name, brand, location, area, city, pincode, total_screens, metadata, is_active)
                    VALUES (%s, %s, %s, %s, %s, 'Bengaluru', %s, %s, %s, true);
                    """,
                    (
                        new_id,
                        name,
                        brand,
                        location,
                        area,
                        pincode,
                        total_screens,
                        json.dumps(metadata_obj),
                    ),
                )
    except Exception as exc:
        logger.error("Failed to insert theater into PostgreSQL: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error while adding cinema: {str(exc)}",
        )

    new_theater = {
        "id": new_id,
        "name": name,
        "location": location,
        "screens": screens,
        "amenities": amenities,
    }
    BENGALURU_THEATERS.insert(0, new_theater)

    try:
        from app.services.live_server import live_manager
        await live_manager.broadcast({
            "type": "THEATER_ADDED",
            "theater": new_theater,
        })
    except Exception as e:
        logger.warning("Could not broadcast THEATER_ADDED: %s", e)

    return {
        "status": "success",
        "message": f"Cinema '{name}' added successfully to Bengaluru catalog",
        "theater": new_theater,
    }


@router.delete("/admin/theaters/{theater_id}")
async def delete_admin_theater(theater_id: str):
    """
    Admin endpoint to remove/deactivate a cinema.
    Soft-deletes in PostgreSQL, updates in-memory catalog, and broadcasts live WS event.
    """
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE theaters SET is_active = false, updated_at = NOW() WHERE id = %s;",
                    (theater_id,),
                )
    except Exception as exc:
        logger.error("Failed to soft-delete theater in PostgreSQL: %s", exc)

    global BENGALURU_THEATERS
    BENGALURU_THEATERS = [t for t in BENGALURU_THEATERS if str(t.get("id")) != theater_id]

    try:
        from app.services.live_server import live_manager
        await live_manager.broadcast({
            "type": "THEATER_REMOVED",
            "theater_id": theater_id,
        })
    except Exception as e:
        logger.warning("Could not broadcast THEATER_REMOVED: %s", e)

    return {
        "status": "success",
        "message": f"Cinema {theater_id} successfully removed from catalog",
        "theater_id": theater_id,
    }


@router.post("/admin/reset-seats")
async def reset_all_seats_admin():
    """
    Admin endpoint to reset all seat locks in Redis and release holds in PostgreSQL.
    """
    redis = await get_redis_client()
    keys = await redis.keys("seat_lock:*")
    cleared_locks = 0
    if keys:
        cleared_locks = await redis.delete(*keys)

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE show_seats SET status = 'AVAILABLE', locked_by = NULL, lock_expires_at = NULL WHERE status != 'BOOKED';"
                )
    except Exception as exc:
        logger.warning("Failed to reset show_seats in DB: %s", exc)

    try:
        from app.services.live_server import live_manager
        await live_manager.broadcast({
            "type": "SEATS_RESET",
            "cleared_locks": cleared_locks,
        })
    except Exception as e:
        logger.warning("Could not broadcast SEATS_RESET: %s", e)

    return {
        "status": "success",
        "message": f"Successfully cleared {cleared_locks} seat locks across Redis engine",
        "cleared_locks": cleared_locks,
    }


@router.get("/admin/stats")
async def get_admin_system_stats():
    """
    Admin endpoint returning real-time health metrics of the Bengaluru ticketing engine.
    """
    redis = await get_redis_client()
    keys = await redis.keys("seat_lock:*")
    from app.services.live_server import live_manager
    return {
        "theaters_count": len(BENGALURU_THEATERS),
        "movies_count": len(MOVIES),
        "active_locks": len(keys),
        "live_clients": len(live_manager.active_connections),
    }



@router.get("/movies")
async def get_movies():
    """Returns currently running movies in Bengaluru."""
    return {"movies": MOVIES}


@router.get("/movies/{movie_id}")
async def get_movie_by_id(movie_id: str):
    """Returns detailed information, cast, synopsis, and reviews for a movie."""
    movie = next((m for m in MOVIES if m["id"] == movie_id), None)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")
    return {"movie": movie}


@router.get("/featured-banners")
async def get_featured_banners():
    """Returns rotating featured promotional banner ads."""
    return {"banners": FEATURED_BANNERS}



LIVE_EVENTS = [
    {
        "id": "ev1",
        "title": "Arijit Singh - Live Symphony",
        "category": "Music Concerts",
        "venue": "Palace Grounds, Jayamahal, Bengaluru",
        "date": "Sat, 24 Oct",
        "price": 1999,
        "image": "https://images.unsplash.com/photo-1470225620780-dba8ba36b745?w=500&auto=format&fit=crop&q=80",
    },
    {
        "id": "ev2",
        "title": "Zakir Khan - Live Comedy Special",
        "category": "Standup Comedy",
        "venue": "Chowdiah Memorial Hall, Malleshwaram",
        "date": "Sun, 25 Oct",
        "price": 799,
        "image": "https://images.unsplash.com/photo-1585699324551-f6c309eedeca?w=500&auto=format&fit=crop&q=80",
    },
    {
        "id": "ev3",
        "title": "Sunburn Arena Bengaluru 2026",
        "category": "EDM Festival",
        "venue": "Bhartiya City, Thanisandra Main Road",
        "date": "Fri, 30 Oct",
        "price": 1499,
        "image": "https://images.unsplash.com/photo-1514525253161-7a46d19cd819?w=500&auto=format&fit=crop&q=80",
    },
    {
        "id": "ev4",
        "title": "Bangalore Comic Con 2026",
        "category": "Pop Culture & Comics",
        "venue": "KTPO Trade Centre, Whitefield",
        "date": "Sat, 7 Nov",
        "price": 899,
        "image": "https://images.unsplash.com/photo-1563089145-599997674d42?w=500&auto=format&fit=crop&q=80",
    },
]


@router.get("/events")
async def get_live_events():
    """Returns top live events in Bengaluru."""
    return {"events": LIVE_EVENTS}




@router.get("/shows/{show_id}/seats")
async def get_show_seats(show_id: str):
    """Fetches real-time persistent seat layout combining PostgreSQL and Redis lock statuses."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from app.config import settings

    conn = psycopg2.connect(settings.postgres_dsn, cursor_factory=RealDictCursor)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ss.seat_number, 
                       COALESCE(s.tier, CASE WHEN ss.seat_number ~ '^[AB]' THEN 'RECLINER' WHEN ss.seat_number ~ '^[CDEF]' THEN 'PRIME' ELSE 'CLASSIC' END) as tier,
                       ss.price, ss.status, ss.user_id as booked_by
                FROM show_seats ss
                LEFT JOIN seats s ON ss.seat_id = s.id
                WHERE ss.show_id = %s
                ORDER BY ss.seat_number ASC;
                """,
                (show_id,),
            )
            rows = cur.fetchall()

    redis = await get_redis_client()
    seats_list = []
    for r in rows:
        seat_no = r["seat_number"]
        db_status = r["status"]
        final_status = db_status
        locked_by = None
        ttl = 0

        if db_status == "AVAILABLE":
            # Check Redis live lock
            lock_val = await redis.get(f"seat_lock:{show_id}:{seat_no}")
            if lock_val:
                final_status = "LOCKED"
                locked_by = lock_val
                ttl = await redis.ttl(f"seat_lock:{show_id}:{seat_no}")

        seats_list.append({
            "seat_number": seat_no,
            "tier": r["tier"],
            "price": float(r["price"]),
            "status": final_status,
            "locked_by": locked_by,
            "ttl_seconds": max(0, ttl) if final_status == "LOCKED" else 0,
        })

    return {"show_id": show_id, "seats": seats_list}



class AttackSimulationRequest(BaseModel):
    show_id: str = Field(default="f0000001-0000-0000-0000-000000000001")
    seat_number: str = Field(default="A1")
    concurrency_count: int = Field(default=5, ge=2, le=20)


@router.post("/simulate-attack")
async def simulate_concurrency_attack(request: AttackSimulationRequest):
    """
    Simulates a high-concurrency race condition attack where N users simultaneously
    attempt to lock the exact same seat at the exact same millisecond.
    Demonstrates Redis atomic SET EX NX lock and 409 Conflict rejection.
    """
    show_id = request.show_id
    seat_no = request.seat_number
    count = request.concurrency_count

    # Generate distinct simulated user IDs
    users = [f"user-sim-{i+1}-{uuid.uuid4().hex[:6]}" for i in range(count)]

    # Fire all N requests concurrently using asyncio.gather
    async def try_lock(user_id: str):
        redis = await get_redis_client()
        key = f"seat_lock:{show_id}:{seat_no}"
        t0 = asyncio.get_event_loop().time()
        # Atomic lock primitive
        acquired = await redis.set(key, user_id, ex=300, nx=True)
        latency_ms = round((asyncio.get_event_loop().time() - t0) * 1000, 2)

        if acquired:
            return {
                "user_id": user_id,
                "status": "ACQUIRED",
                "http_code": 201,
                "message": f"Successfully locked {seat_no} for 300s",
                "latency_ms": latency_ms,
            }
        else:
            return {
                "user_id": user_id,
                "status": "CONFLICT_409",
                "http_code": 409,
                "message": f"Rejected with 409 Conflict: Seat {seat_no} already locked",
                "latency_ms": latency_ms,
            }

    results = await asyncio.gather(*(try_lock(u) for u in users))

    successes = [r for r in results if r["status"] == "ACQUIRED"]
    conflicts = [r for r in results if r["status"] == "CONFLICT_409"]

    return {
        "summary": {
            "total_concurrent_requests": count,
            "locks_acquired": len(successes),
            "locks_rejected_409": len(conflicts),
            "seat_number": seat_no,
            "show_id": show_id,
            "overbooking_prevented": len(conflicts) == (count - len(successes)),
        },
        "results": results,
    }


# -----------------------------------------------------------------------------
# AUTHENTIC BOOKMYSHOW CATEGORY DATASETS (Stream, Plays, Sports, Activities)
# -----------------------------------------------------------------------------
STREAM_CATALOG = [
    {
        "id": "str1",
        "title": "Dune: Part Two",
        "poster": "https://image.tmdb.org/t/p/w500/1pdfLvkbY9ohJlCjQH2CZjjYVvJ.jpg",
        "category": "BookMyShow Stream",
        "badge": "4K UHD • PREMIERE",
        "duration": "2h 46m",
        "languages": ["English", "Hindi"],
        "price": 149,
        "type": "Rent or Buy",
        "description": "Paul Atreides unites with Chani and the Fremen while seeking revenge against the conspirators who destroyed his family.",
    },
    {
        "id": "str2",
        "title": "Oppenheimer",
        "poster": "https://image.tmdb.org/t/p/w500/8Gxv8gSFCU0XGDykEGv7zR1n2ua.jpg",
        "category": "BookMyShow Stream",
        "badge": "IMAX ENHANCED",
        "duration": "3h 00m",
        "languages": ["English", "Hindi"],
        "price": 119,
        "type": "Rent in HD",
        "description": "The story of American scientist J. Robert Oppenheimer and his role in the development of the atomic bomb.",
    },
    {
        "id": "str3",
        "title": "Spider-Man: Across the Spider-Verse",
        "poster": "https://image.tmdb.org/t/p/w500/8Vt6mWEReuy4Of61Lnj5Xj704m8.jpg",
        "category": "BookMyShow Stream",
        "badge": "DOLBY VISION",
        "duration": "2h 20m",
        "languages": ["English", "Hindi", "Tamil", "Telugu"],
        "price": 99,
        "type": "Rent in HD",
        "description": "Miles Morales catapults across the Multiverse, encountering a team of Spider-People charged with protecting its existence.",
    },
    {
        "id": "str4",
        "title": "Furiosa: A Mad Max Saga",
        "poster": "https://image.tmdb.org/t/p/w500/vHBJBjkJKotI8AJXmrpNG6v2WRG.jpg",
        "category": "BookMyShow Stream",
        "badge": "TOP RATED",
        "duration": "2h 28m",
        "languages": ["English", "Hindi"],
        "price": 149,
        "type": "Rent or Buy",
        "description": "The origin story of renegade warrior Furiosa before her encounter and teamup with Mad Max in Fury Road.",
    },
]

PLAYS_CATALOG = [
    {
        "id": "ply1",
        "title": "Hayavadana by Girish Karnad",
        "poster": "https://images.unsplash.com/photo-1507676184212-d03ab07a01bf?w=500&auto=format&fit=crop&q=80",
        "category": "Theatre Play",
        "venue": "Ranga Shankara, 36/2 8th Cross Road, JP Nagar 2nd Phase",
        "date": "Fri, 18 Oct • 7:30 PM",
        "price": 300,
        "language": "Kannada",
        "description": "A landmark theatrical masterpiece exploring the themes of human identity, intellect, and physical completeness.",
    },
    {
        "id": "ply2",
        "title": "Hamlet: The Prince of Denmark",
        "poster": "https://images.unsplash.com/photo-1469488865564-c2de10f69f96?w=500&auto=format&fit=crop&q=80",
        "category": "Classic Drama",
        "venue": "Chowdiah Memorial Hall, Vyalikaval, Malleshwaram",
        "date": "Sat, 19 Oct • 6:30 PM",
        "price": 499,
        "language": "English",
        "description": "Shakespeare's tragedy of revenge, betrayal, and existential contemplation performed by Bengaluru's premier repertory.",
    },
    {
        "id": "ply3",
        "title": "Boiled Beans on Toast",
        "poster": "https://images.unsplash.com/photo-1514525253161-7a46d19cd819?w=500&auto=format&fit=crop&q=80",
        "category": "Contemporary Play",
        "venue": "Jagriti Theatre, Ramagondanahalli, Whitefield",
        "date": "Sun, 20 Oct • 5:00 PM & 8:00 PM",
        "price": 400,
        "language": "English / Kannada",
        "description": "A rich, humorous tapestry depicting the everyday lives of different individuals navigating modern Bengaluru.",
    },
]

SPORTS_CATALOG = [
    {
        "id": "spt1",
        "title": "Bengaluru FC vs Kerala Blasters FC",
        "poster": "https://images.unsplash.com/photo-1508098682722-e99c43a406b2?w=500&auto=format&fit=crop&q=80",
        "category": "Indian Super League (ISL)",
        "venue": "Sree Kanteerava Stadium, Nunegundlapalli, Bengaluru",
        "date": "Sat, 26 Oct • 7:30 PM",
        "price": 250,
        "status": "Fast Filling",
        "description": "The Southern Derby returns to the Fortress! Cheer for Sunil Chhetri and the Blues in high-voltage ISL action.",
    },
    {
        "id": "spt2",
        "title": "Bengaluru Midnight Marathon 2026",
        "poster": "https://images.unsplash.com/photo-1452626038306-9aae5e071dd3?w=500&auto=format&fit=crop&q=80",
        "category": "Running & Athletics",
        "venue": "KTPO Trade Centre, EPIP Zone, Whitefield",
        "date": "Sat, 12 Dec • 10:00 PM",
        "price": 1200,
        "status": "Registrations Open",
        "description": "Asia's only night marathon! 5K Hope Run, 10K, Half Marathon, and Full Marathon under the Bengaluru night sky.",
    },
    {
        "id": "spt3",
        "title": "Bengaluru Bulls vs Patna Pirates",
        "poster": "https://images.unsplash.com/photo-1517649763962-0c623266ddc0?w=500&auto=format&fit=crop&q=80",
        "category": "Pro Kabaddi League",
        "venue": "Kanteerava Indoor Stadium, Kasturba Road",
        "date": "Sun, 27 Oct • 8:00 PM",
        "price": 350,
        "status": "Available",
        "description": "High-intensity raid and tackle clash as the Bengaluru Bulls fight for a spot in the PKL playoffs.",
    },
]

ACTIVITIES_CATALOG = [
    {
        "id": "act1",
        "title": "Wonderla Amusement Park Bengaluru - All Access Pass",
        "poster": "https://images.unsplash.com/photo-1513889961551-628c1e5e2ee9?w=500&auto=format&fit=crop&q=80",
        "category": "Theme & Water Park",
        "venue": "28th km, Mysore Road, Bengaluru",
        "date": "Valid All Days • 11:00 AM - 6:00 PM",
        "price": 1299,
        "status": "Instant E-Ticket",
        "description": "Over 60 thrilling land and water rides including Recoil roller-coaster, Wave Pool, and High Thrill drops.",
    },
    {
        "id": "act2",
        "title": "Mystery Rooms: Real Escape Experience",
        "poster": "https://images.unsplash.com/photo-1518709268805-4e9042af9f23?w=500&auto=format&fit=crop&q=80",
        "category": "Escape Room Adventure",
        "venue": "1133, 100 Feet Road, HAL 2nd Stage, Indiranagar",
        "date": "Hourly Slots • 11:30 AM - 10:30 PM",
        "price": 900,
        "status": "Instant Booking",
        "description": "60 minutes to crack secret codes, uncover cryptic clues, and escape from thrilling locked mystery rooms.",
    },
    {
        "id": "act3",
        "title": "Smaaash VR Gaming & Twilight Bowling",
        "poster": "https://images.unsplash.com/photo-1538805060514-97d9cc17730c?w=500&auto=format&fit=crop&q=80",
        "category": "Gaming & Sports Zone",
        "venue": "1 MG-Lido Mall, Trinity Circle, MG Road",
        "date": "Open Everyday • 12:00 PM - 11:00 PM",
        "price": 650,
        "status": "Combo Deal",
        "description": "World-class 10-pin Twilight Bowling lanes, VR cricket simulators, and arcade racing machines.",
    },
]


@router.get("/catalog/category/{category_name}")
async def get_category_items(category_name: str):
    """Returns items for specific BookMyShow categories."""
    cat = category_name.strip().lower()
    if cat == "movies":
        return {"category": "Movies", "items": MOVIES}
    elif cat == "stream":
        return {"category": "Stream", "items": STREAM_CATALOG}
    elif cat == "events":
        return {"category": "Events", "items": LIVE_EVENTS}
    elif cat == "plays":
        return {"category": "Plays", "items": PLAYS_CATALOG}
    elif cat == "sports":
        return {"category": "Sports", "items": SPORTS_CATALOG}
    elif cat == "activities":
        return {"category": "Activities", "items": ACTIVITIES_CATALOG}
    else:
        return {"category": category_name, "items": MOVIES}


@router.get("/search")
async def live_search(q: str = ""):
    """Provides fast live autocomplete search across movies, cinemas, and entertainment."""
    query = q.strip().lower()
    if not query:
        return {"movies": [], "theaters": [], "events": []}

    matched_movies = [
        {"id": m["id"], "title": m["title"], "languages": m["languages"], "rating": m["rating"], "poster": m["poster"]}
        for m in MOVIES
        if query in m["title"].lower() or any(query in l.lower() for l in m["languages"]) or any(query in g.lower() for g in m["genres"])
    ]

    matched_theaters = [
        {"id": t["id"], "name": t["name"], "location": t["location"]}
        for t in BENGALURU_THEATERS
        if query in t["name"].lower() or query in t["location"].lower()
    ]

    all_entertainment = LIVE_EVENTS + PLAYS_CATALOG + SPORTS_CATALOG + ACTIVITIES_CATALOG
    matched_events = [
        {"id": e["id"], "title": e["title"], "venue": e.get("venue", ""), "category": e.get("category", "")}
        for e in all_entertainment
        if query in e["title"].lower() or query in e.get("category", "").lower() or query in e.get("venue", "").lower()
    ]

    return {
        "query": q,
        "movies": matched_movies[:5],
        "theaters": matched_theaters[:4],
        "events": matched_events[:4],
    }

