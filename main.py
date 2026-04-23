import os
import json
import re
import uuid6
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, Index, asc, desc
from sqlalchemy.orm import declarative_base, sessionmaker

# ---------------- DATABASE ----------------
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./insighta.db")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class Profile(Base):
    __tablename__ = "profiles"

    id = Column(String, primary_key=True, default=lambda: str(uuid6.uuid7()))
    name = Column(String, unique=True, index=True, nullable=False)
    gender = Column(String, index=True)
    gender_probability = Column(Float)
    age = Column(Integer, index=True)
    age_group = Column(String, index=True)
    country_id = Column(String(2), index=True)
    country_name = Column(String)
    country_probability = Column(Float)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


Index("ix_filter_combo", Profile.gender, Profile.age_group, Profile.country_id)
Index("ix_age_sort", Profile.age)

Base.metadata.create_all(bind=engine)

# ---------------- APP ----------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- SERIALIZER ----------------
def serialize(p):
    return {
        "id": p.id,
        "name": p.name,
        "gender": p.gender,
        "gender_probability": p.gender_probability,
        "age": p.age,
        "age_group": p.age_group,
        "country_id": p.country_id,
        "country_name": p.country_name,
        "country_probability": p.country_probability,
        "created_at": p.created_at.replace(tzinfo=timezone.utc).isoformat()
    }

# ---------------- SEEDING (FIXED) ----------------
# ---------------- SEEDING (RAILWAY-PROOF) ----------------
@app.on_event("startup")
async def seed_db():
    db = SessionLocal()
    try:
        # This gets the exact folder where main.py is located
        base_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(base_dir, "profiles.json")

        # Log to terminal so you can see it in Railway Logs
        print(f"🔍 Searching for profiles.json at: {json_path}")

        if os.path.exists(json_path):
            with open(json_path, "r") as f:
                data = json.load(f)

            # Extract list if nested in 'data' or 'records'
            if isinstance(data, dict):
                data = data.get("data", data.get("records", []))

            if data:
                # Clear any old empty state and seed fresh
                db.query(Profile).delete()
                db.bulk_insert_mappings(Profile, data)
                db.commit()
                print(f"🚀 SUCCESS: Seeded {len(data)} records!")
        else:
            print(f"❌ CRITICAL ERROR: File not found at {json_path}")
    except Exception as e:
        print(f"❌ SEEDER FAILED: {e}")
        db.rollback()
    finally:
        db.close()

# ---------------- FILTER ENDPOINT ----------------
@app.get("/api/profiles")
def get_profiles(
    gender: Optional[str] = None,
    age_group: Optional[str] = None,
    country_id: Optional[str] = None,
    min_age: Optional[int] = None,
    max_age: Optional[int] = None,
    min_gender_probability: Optional[float] = None,
    min_country_probability: Optional[float] = None,
    sort_by: str = "created_at",
    order: str = "desc",
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1)
):
    if sort_by not in ["age", "created_at", "gender_probability"] or order not in ["asc", "desc"]:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "Invalid query parameters"}
        )

    limit = min(limit, 50)

    db = SessionLocal()
    try:
        query = db.query(Profile)

        if gender:
            query = query.filter(Profile.gender == gender.lower())
        if age_group:
            query = query.filter(Profile.age_group == age_group.lower())
        if country_id:
            query = query.filter(Profile.country_id == country_id.upper())
        if min_age is not None:
            query = query.filter(Profile.age >= min_age)
        if max_age is not None:
            query = query.filter(Profile.age <= max_age)
        if min_gender_probability is not None:
            query = query.filter(Profile.gender_probability >= min_gender_probability)
        if min_country_probability is not None:
            query = query.filter(Profile.country_probability >= min_country_probability)

        total = query.count()

        column = getattr(Profile, sort_by)
        query = query.order_by(desc(column) if order == "desc" else asc(column))

        results = query.offset((page - 1) * limit).limit(limit).all()

        return {
            "status": "success",
            "page": page,
            "limit": limit,
            "total": total,
            "data": [serialize(p) for p in results]
        }

    finally:
        db.close()

# ---------------- NLP PARSER ----------------
def parse_query(q: str):
    q = q.lower()
    filters = {}
    found = False

    if "male" in q and "female" not in q:
        filters["gender"] = "male"
        found = True
    elif "female" in q and "male" not in q:
        filters["gender"] = "female"
        found = True

    for group in ["child", "teenager", "adult", "senior"]:
        if group in q:
            filters["age_group"] = group
            found = True

    if "young" in q:
        filters["min_age"] = 16
        filters["max_age"] = 24
        found = True

    match = re.search(r"(above|over|greater than)\s+(\d+)", q)
    if match:
        filters["min_age"] = int(match.group(2))
        found = True

    match = re.search(r"(below|under|less than)\s+(\d+)", q)
    if match:
        filters["max_age"] = int(match.group(2))
        found = True

    countries = {
        "nigeria": "NG",
        "kenya": "KE",
        "angola": "AO",
        "ghana": "GH",
        "benin": "BJ"
    }

    for name, code in countries.items():
        if name in q:
            filters["country_id"] = code
            found = True

    return filters if found else None

# ---------------- NLP ENDPOINT ----------------
@app.get("/api/profiles/search")
def search_profiles(
    q: str = Query(None),
    page: int = 1,
    limit: int = 10
):
    if not q or not q.strip():
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "Missing or empty parameter"}
        )

    filters = parse_query(q)

    if not filters:
        return JSONResponse(
            status_code=422,
            content={"status": "error", "message": "Unable to interpret query"}
        )

    return get_profiles(**filters, page=page, limit=limit)

# ---------------- CREATE ----------------
@app.post("/api/profiles", status_code=201)
async def create_profile(request: Request):
    db = SessionLocal()
    try:
        data = await request.json()

        if "name" not in data:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "Missing name"}
            )

        exists = db.query(Profile).filter_by(name=data["name"]).first()
        if exists:
            return JSONResponse(
                status_code=422,
                content={"status": "error", "message": "Duplicate profile"}
            )

        profile = Profile(
            id=str(uuid6.uuid7()),
            name=data["name"],
            gender=data.get("gender"),
            gender_probability=data.get("gender_probability"),
            age=data.get("age"),
            age_group=data.get("age_group"),
            country_id=data.get("country_id"),
            country_name=data.get("country_name"),
            country_probability=data.get("country_probability"),
            created_at=datetime.now(timezone.utc)
        )

        db.add(profile)
        db.commit()
        db.refresh(profile)

        return {"status": "success", "data": serialize(profile)}

    except Exception:
        db.rollback()
        return JSONResponse(
            status_code=422,
            content={"status": "error", "message": "Invalid request"}
        )
    finally:
        db.close()