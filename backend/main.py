import os
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import create_engine, text
from db.database import engine, get_db
from datetime import date, datetime
from ingestion.gdelt import run_gdelt_ingestion, debug_gdelt_raw
from ingestion.weather import run_weather_ingestion
from ingestion.stocks import run_stock_ingestion
from scoring.composite import run_scoring
from db.models import Base, CompositeScore, RawSignal, NormalisedScore
from sqlalchemy import select, delete, and_
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# Sync engine for fallback/initialization
SYNC_DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://badday:badday@localhost:5432/badday"
).replace("postgresql+asyncpg://", "postgresql://")

sync_engine = create_engine(SYNC_DATABASE_URL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Global Bad Day Index API starting...")
    # Run Base.metadata.create_all using the sync engine as a fallback
    try:
        Base.metadata.create_all(bind=sync_engine)
        print("Database tables created or already exist.")
    except Exception as e:
        print(f"Database initialization failed: {e}")
    yield

app = FastAPI(
    title="Global Bad Day Index API",
    version="0.1.0",
    lifespan=lifespan
)

# Mount frontend as static files
app.mount("/static", StaticFiles(directory="../frontend"), name="static")

@app.get("/")
async def read_index():
    return FileResponse("../frontend/index.html")

@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}

@app.get("/db-check")
async def db_check(db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(text("SELECT 1"))
        return {"db": "ok"}
    except Exception as e:
        return {"db": "error", "detail": str(e)}

@app.post("/ingest/gdelt")
async def ingest_gdelt(db: AsyncSession = Depends(get_db)):
    num_countries = await run_gdelt_ingestion(db)
    return {"status": "ok", "countries_processed": num_countries}

@app.get("/debug/gdelt")
async def debug_gdelt():
    return await debug_gdelt_raw()

@app.post("/ingest/weather")
async def ingest_weather(db: AsyncSession = Depends(get_db)):
    num_countries = await run_weather_ingestion(db)
    return {"status": "ok", "countries_processed": num_countries}

@app.post("/ingest/stocks")
async def ingest_stocks(db: AsyncSession = Depends(get_db)):
    """
    Triggers stock market ingestion. 
    Note: This will take several minutes due to Alpha Vantage rate limiting (5 req/min).
    """
    num_processed, skipped = await run_stock_ingestion(db)
    return {
        "status": "ok", 
        "countries_processed": num_processed, 
        "skipped": skipped
    }

@app.post("/score")
async def score_data(target_date: str = None, db: AsyncSession = Depends(get_db)):
    """
    Triggers the composite scoring engine for a specific date.
    Date format: YYYY-MM-DD. Defaults to today.
    """
    if target_date:
        try:
            parsed_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        except ValueError:
            return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD"}
    else:
        parsed_date = date.today()
    
    num_countries, convergence_events = await run_scoring(parsed_date, db)
    return {
        "status": "ok", 
        "date": str(parsed_date), 
        "countries_scored": num_countries, 
        "convergence_events": convergence_events
    }

@app.get("/scores/today")
async def get_today_scores(db: AsyncSession = Depends(get_db)):
    """
    Returns composite scores for today.
    """
    today = date.today()
    stmt = select(CompositeScore).where(CompositeScore.date == today)
    result = await db.execute(stmt)
    scores = result.scalars().all()
    
    return [
        {
            "country_code": s.country_code,
            "bad_day_score": s.bad_day_score,
            "convergence_flag": s.convergence_flag,
            "signal_count": s.signal_count
        }
        for s in scores
    ]

@app.delete("/scores/clear")
async def clear_scores(target_date: str = None, db: AsyncSession = Depends(get_db)):
    """
    Deletes all records from RawSignal, NormalisedScore, and CompositeScore for a specific date.
    Date format: YYYY-MM-DD. Defaults to today.
    DEV/ADMIN ONLY - Remove before production.
    """
    if target_date:
        try:
            parsed_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        except ValueError:
            return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD"}
    else:
        parsed_date = date.today()

    # Define range for RawSignal (timestamp)
    start_dt = datetime.combine(parsed_date, datetime.min.time())
    end_dt = datetime.combine(parsed_date, datetime.max.time())

    # Delete RawSignals
    await db.execute(delete(RawSignal).where(
        and_(RawSignal.timestamp >= start_dt, RawSignal.timestamp <= end_dt)
    ))

    # Delete NormalisedScores
    await db.execute(delete(NormalisedScore).where(NormalisedScore.date == parsed_date))

    # Delete CompositeScores
    await db.execute(delete(CompositeScore).where(CompositeScore.date == parsed_date))

    await db.commit()
    return {"status": "ok", "message": f"Cleared data for {parsed_date}"}
