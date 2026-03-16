import os
import logging
import time
import pandas as pd
import httpx
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from db.models import RawSignal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_weather_anomalies() -> dict[str, float]:
    """
    Fetches weather data from Open-Meteo and calculates temperature anomalies 
    relative to a 35-day rolling baseline for each country.
    """
    logger.info("Starting weather anomaly fetch...")
    
    # Load centroids
    data_path = os.path.join(os.path.dirname(__file__), "..", "data", "country_centroids.csv")
    df_countries = pd.read_csv(data_path)
    
    anomalies = {}
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        for _, row in df_countries.iterrows():
            country_code = row['country_code']
            lat = row['latitude']
            lon = row['longitude']
            
            try:
                url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max,temperature_2m_min&timezone=UTC&past_days=35&forecast_days=1"
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
                
                daily = data.get("daily", {})
                max_temps = daily.get("temperature_2m_max", [])
                min_temps = daily.get("temperature_2m_min", [])
                
                if len(max_temps) < 36:
                    logger.warning(f"Insufficient data for {country_code}")
                    continue
                
                # Calculate daily means (avg of max and min)
                daily_means = [(mx + mn) / 2 for mx, mn in zip(max_temps, min_temps)]
                
                # Today is the last value
                today_mean = daily_means[-1]
                
                # Baseline is the previous 35 days
                baseline_days = daily_means[:-1]
                baseline_mean = sum(baseline_days) / len(baseline_days)
                
                # Anomaly: absolute deviation from baseline
                anomaly = abs(today_mean - baseline_mean)
                
                # Normalise: max meaningful anomaly is 15°C
                score = min(anomaly / 15.0, 1.0)
                
                anomalies[country_code] = score
                logger.info(f"Processed {country_code}: anomaly={anomaly:.2f}°C, score={score:.2f}")
                
            except Exception as e:
                logger.error(f"Failed to fetch weather for {country_code}: {e}")
            
            # Be polite to the free API
            await asyncio.sleep(0.1)
            
    return anomalies

async def save_weather_scores(scores: dict[str, float], db: AsyncSession):
    """
    Saves the scores as RawSignal records in the database.
    """
    timestamp = datetime.utcnow()
    signals = []
    
    for country_code, score in scores.items():
        signal = RawSignal(
            country_code=country_code,
            timestamp=timestamp,
            signal_type="weather_anomaly",
            raw_value=score
        )
        signals.append(signal)
    
    if signals:
        db.add_all(signals)
        await db.commit()
        logger.info(f"Saved {len(signals)} weather anomaly signals to database.")

async def run_weather_ingestion(db: AsyncSession):
    """
    Orchestrates the weather ingestion process.
    """
    try:
        logger.info("Starting weather anomaly ingestion...")
        scores = await fetch_weather_anomalies()
        await save_weather_scores(scores, db)
        logger.info("Weather ingestion completed successfully.")
        return len(scores)
    except Exception as e:
        logger.error(f"Weather ingestion failed: {e}", exc_info=True)
        return 0

# Necessary for the await asyncio.sleep
import asyncio
