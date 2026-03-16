import os
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import create_engine, text
from db.database import engine, get_db
from db.models import Base
from ingestion.gdelt import run_gdelt_ingestion, debug_gdelt_raw

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
