import os
import json
import re
import uuid6
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, Index, desc, asc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# --- DATABASE CONFIG ---
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./insighta.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
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


Index('ix_query_optimization', Profile.gender, Profile.age, Profile.country_id)
Base.metadata.create_all(bind=engine)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- NLP PARSER ---
def parse_natural_query(q: str):
    q = q.lower().strip()
    filters = {}
    found_any = False

    if re.search(r'\bmales?\b', q):
        filters['gender'] = 'male'
        found_any = True
    elif re.search(r'\bfemales?\b', q):
        filters['gender'] = 'female'
        found_any = True

    for group in ['child', 'teenager', 'adult', 'senior']:
        if group in q:
            filters['age_group'] = group
            found_any = True

    if 'young' in q:
        filters['min_age'], filters['max_age'] = 16, 24
        found_any = True

    above = re.search(r'(?:above|older than|over|greater than)\s+(\d+)', q)
    if above:
        filters['min_age'] = int(above.group(1)) + 1
        found_any = True

    under = re.search(r'(?:under|younger than|below|less than)\s+(\d+)', q)
    if under:
        filters['max_age'] = int(under.group(1)) - 1
        found_any = True

    countries = {"nigeria": "NG", "kenya": "KE", "angola": "AO", "ghana": "GH", "benin": "BJ"}
    for name, cid in countries.items():
        if name in q:
            filters['country_id'] = cid
            found_any = True
            break

    return filters if found_any else None


# --- UPDATED PATH-SAFE SEEDER ---
@app.on_event("startup")
async def seed_db():
    db = SessionLocal()
    try:
        # This part ensures Railway finds the file regardless of the workdir
        current_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(current_dir, "profiles.json")

        if db.query(Profile).count() == 0:
            if os.path.exists(json_path):
                with open(json_path, "r") as f:
                    data = json.load(f)

                if isinstance(data, dict):
                    data = data.get("data", data.get("records", []))

                if data:
                    db.bulk_insert_mappings(Profile, data)
                    db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


# --- BOT SEEDING SUPPORT ---
@app.post("/api/profiles", status_code=201)
async def create_profile(request: Request):
    db = SessionLocal()
    try:
        data = await request.json()
        valid_fields = {k: v for k, v in data.items() if hasattr(Profile, k)}
        if "id" not in valid_fields:
            valid_fields["id"] = str(uuid6.uuid7())
        new_profile = Profile(**valid_fields)
        db.add(new_profile)
        db.commit()
        db.refresh(new_profile)
        return {"status": "success", "data": new_profile}
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=422, content={"status": "error", "message": str(e)})
    finally:
        db.close()


# --- ENDPOINTS ---
@app.get("/api/profiles")
async def get_profiles(
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
        limit: int = Query(10, ge=1, le=50)
):
    if sort_by not in ["age", "created_at", "gender_probability"]:
        return JSONResponse(status_code=400, content={"status": "error", "message": "Invalid query parameters"})

    db = SessionLocal()
    query = db.query(Profile)

    if gender: query = query.filter(Profile.gender == gender.lower())
    if age_group: query = query.filter(Profile.age_group == age_group.lower())
    if country_id: query = query.filter(Profile.country_id == country_id.upper())
    if min_age is not None: query = query.filter(Profile.age >= min_age)
    if max_age is not None: query = query.filter(Profile.age <= max_age)
    if min_gender_probability: query = query.filter(Profile.gender_probability >= min_gender_probability)
    if min_country_probability: query = query.filter(Profile.country_probability >= min_country_probability)

    col = getattr(Profile, sort_by)
    query = query.order_by(desc(col) if order == "desc" else asc(col))

    total = query.count()
    results = query.offset((page - 1) * limit).limit(limit).all()
    db.close()

    return {
        "status": "success", "page": page, "limit": limit, "total": total, "data": results
    }


@app.get("/api/profiles/search")
async def search_profiles(q: str = Query(None), page: int = 1, limit: int = 10):
    if not q or not q.strip():
        return JSONResponse(status_code=400, content={"status": "error", "message": "Missing or empty parameter"})

    filters = parse_natural_query(q)
    if filters is None:
        return JSONResponse(status_code=200, content={"status": "error", "message": "Unable to interpret query"})

    return await get_profiles(**filters, page=page, limit=limit)