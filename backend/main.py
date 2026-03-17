import os
import pandas as pd
from fastapi import FastAPI, Depends, HTTPException
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import create_engine, text, select, delete, and_, desc
from db.database import engine, get_db
from datetime import date, datetime
from ingestion.gdelt import run_gdelt_ingestion
from ingestion.weather import run_weather_ingestion
from ingestion.stocks import run_stock_ingestion
from scoring.composite import run_scoring
from db.models import Base, RawSignal, NormalisedScore, CompositeScore, Headline
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

# Load country names for global headlines
COUNTRY_NAMES = {}
try:
    centroids_path = os.path.join(os.path.dirname(__file__), "data", "country_centroids.csv")
    if os.path.exists(centroids_path):
        df_countries = pd.read_csv(centroids_path)
        COUNTRY_NAMES = dict(zip(df_countries['country_code'], df_countries['country_name']))
except Exception as e:
    print(f"Error loading country names: {e}")

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

@app.get("/headlines/global")
async def get_global_headlines(db: AsyncSession = Depends(get_db)):
    """
    Returns the top 20 most negative headlines globally today.
    """
    today = date.today()
    
    # Query headlines and join with composite_scores to get bad_day_score
    # We use an outer join in case some headline countries don't have a composite score today yet
    stmt = (
        select(Headline, CompositeScore.bad_day_score)
        .outerjoin(
            CompositeScore, 
            and_(
                Headline.country_code == CompositeScore.country_code,
                Headline.date == CompositeScore.date
            )
        )
        .where(Headline.date == today)
        .order_by(Headline.tone.asc())
        .limit(20)
    )
    
    result = await db.execute(stmt)
    rows = result.all()
    
    if not rows:
        return {"message": "No headlines yet — trigger /ingest/gdelt first", "headlines": []}
        
    return {
        "status": "ok",
        "headlines": [
            {
                "country_code": h.country_code,
                "country_name": COUNTRY_NAMES.get(h.country_code, h.country_code),
                "url": h.url,
                "source_name": h.source_name or "Unknown",
                "tone": h.tone,
                "bad_day_score": bad_day_score or 0.0
            }
            for h, bad_day_score in rows
        ]
    }

@app.get("/headlines/{country_code}")
async def get_headlines(country_code: str, db: AsyncSession = Depends(get_db)):
    """
    Returns today's top negative headlines for a country.
    """
    today = date.today()
    stmt = select(Headline).where(
        and_(Headline.country_code == country_code.upper(), Headline.date == today)
    ).order_by(Headline.tone.asc()).limit(5)
    
    result = await db.execute(stmt)
    headlines = result.scalars().all()
    
    return [
        {"url": h.url, "source_name": h.source_name, "tone": h.tone}
        for h in headlines
    ]

@app.get("/sources/{country_code}")
async def get_sources(country_code: str, db: AsyncSession = Depends(get_db)):
    """
    Returns a structured breakdown of signals and headlines for a country.
    """
    today = date.today()
    country_code = country_code.upper()
    
    # Get composite score
    composite_stmt = select(CompositeScore).where(
        and_(CompositeScore.country_code == country_code, CompositeScore.date == today)
    )
    composite_result = await db.execute(composite_stmt)
    composite = composite_result.scalar_one_or_none()
    
    if not composite:
        return {"status": "error", "message": "No data found for this country today"}
    
    # Get normalized scores
    scores_stmt = select(NormalisedScore).where(
        and_(NormalisedScore.country_code == country_code, NormalisedScore.date == today)
    )
    scores_result = await db.execute(scores_stmt)
    scores = scores_result.scalars().all()
    
    # Get headlines
    headlines_stmt = select(Headline).where(
        and_(Headline.country_code == country_code, Headline.date == today)
    ).order_by(Headline.tone.asc()).limit(5)
    headlines_result = await db.execute(headlines_stmt)
    headlines = headlines_result.scalars().all()
    
    # Build signals list
    signals = []
    source_info = {
        "gdelt_sentiment": {"label": "News sentiment", "source_name": "GDELT Project", "source_url": "https://gdeltproject.org"},
        "stock_change": {"label": "Stock market", "source_name": "Yahoo Finance", "source_url": "https://finance.yahoo.com"},
        "weather_anomaly": {"label": "Weather", "source_name": "Open-Meteo", "source_url": "https://open-meteo.com"}
    }
    
    for s in scores:
        info = source_info.get(s.signal_type, {"label": s.signal_type, "source_name": "Unknown", "source_url": "#"})
        signals.append({
            "type": s.signal_type,
            "score": s.score,
            "label": info["label"],
            "source_name": info["source_name"],
            "source_url": info["source_url"]
        })
        
    return {
        "country_code": country_code,
        "bad_day_score": composite.bad_day_score,
        "signal_count": composite.signal_count,
        "signals": signals,
        "headlines": [
            {"url": h.url, "source_name": h.source_name, "tone": h.tone}
            for h in headlines
        ]
    }

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
