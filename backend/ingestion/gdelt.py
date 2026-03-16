import io
import zipfile
import logging
import pandas as pd
import httpx
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from db.models import RawSignal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GDELT_LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

def extract_country_code(location_str):
    """
    Extracts the 2-letter country code from GDELT location field.
    Example: 1#Kabul#AF#AF#... -> AF
    """
    if not isinstance(location_str, str) or not location_str:
        return None
    try:
        # Locations are semicolon separated
        locations = location_str.split(';')
        for loc in locations:
            fields = loc.split('#')
            if len(fields) >= 3:
                country_code = fields[2]
                if len(country_code) == 2:
                    return country_code
    except Exception:
        pass
    return None

async def fetch_latest_gdelt_sentiment() -> dict[str, float]:
    """
    Fetches the latest GKG file from GDELT, parses sentiment, 
    and returns a normalized badness score per country.
    """
    logger.info("Fetching latest GDELT update information...")
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(GDELT_LAST_UPDATE_URL)
        resp.raise_for_status()
        
        # Find the .gkg.csv.zip URL
        lines = resp.text.strip().split('\n')
        gkg_url = None
        for line in lines:
            if '.gkg.csv.zip' in line:
                gkg_url = line.split()[-1]
                break
        
        if not gkg_url:
            raise ValueError("Could not find GKG URL in GDELT lastupdate.txt")
        
        logger.info(f"Downloading GKG file from {gkg_url}...")
        gkg_resp = await client.get(gkg_url)
        gkg_resp.raise_for_status()
        
    # Unzip in memory
    with zipfile.ZipFile(io.BytesIO(gkg_resp.content)) as z:
        csv_filename = z.namelist()[0]
        with z.open(csv_filename) as f:
            # GDELT GKG V2 columns
            # Column 9: Locations (structured)
            # Column 15: V2Tone (sentiment)
            # Using usecols to save memory
            df = pd.read_csv(
                f, 
                sep='\t', 
                header=None, 
                usecols=[9, 15],
                names=['Locations', 'V2Tone'],
                encoding='utf-8',
                on_bad_lines='skip'
            )

    logger.info(f"Parsed {len(df)} rows from GKG file.")

    # Process sentiment
    def parse_tone(tone_str):
        try:
            return float(tone_str.split(',')[0])
        except (ValueError, AttributeError):
            return None

    df['tone'] = df['V2Tone'].apply(parse_tone)
    df['country_code'] = df['Locations'].apply(extract_country_code)

    # Drop rows with missing data
    df = df.dropna(subset=['tone', 'country_code'])
    
    # Group by country and calculate mean tone
    country_tones = df.groupby('country_code')['tone'].mean()

    # Normalize tone to 0-1 badness score
    # score = (tone * -1 + 100) / 200, clamped to 0.0–1.0
    def normalize_tone(tone):
        score = (tone * -1 + 100) / 200
        return max(0.0, min(1.0, score))

    badness_scores = country_tones.apply(normalize_tone).to_dict()
    
    return badness_scores

async def save_gdelt_scores(scores: dict[str, float], db: AsyncSession):
    """
    Saves the scores as RawSignal records in the database.
    """
    timestamp = datetime.utcnow()
    signals = []
    
    for country_code, score in scores.items():
        signal = RawSignal(
            country_code=country_code,
            timestamp=timestamp,
            signal_type="gdelt_sentiment",
            raw_value=score
        )
        signals.append(signal)
    
    if signals:
        db.add_all(signals)
        await db.commit()
        logger.info(f"Saved {len(signals)} GDELT sentiment signals to database.")

async def run_gdelt_ingestion(db: AsyncSession):
    """
    Orchestrates the GDELT ingestion process.
    """
    try:
        logger.info("Starting GDELT news sentiment ingestion...")
        scores = await fetch_latest_gdelt_sentiment()
        
        logger.info(f"Found news sentiment for {len(scores)} countries.")
        
        await save_gdelt_scores(scores, db)
        logger.info("GDELT ingestion completed successfully.")
        return len(scores)
    except Exception as e:
        logger.error(f"GDELT ingestion failed: {e}", exc_info=True)
        return 0

async def debug_gdelt_raw() -> dict:
    """
    Debug function to inspect raw GDELT data.
    """
    logger.info("Debugging raw GDELT data...")
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(GDELT_LAST_UPDATE_URL)
        resp.raise_for_status()
        
        lines = resp.text.strip().split('\n')
        gkg_url = None
        for line in lines:
            if '.gkg.csv.zip' in line:
                gkg_url = line.split()[-1]
                break
        
        if not gkg_url:
            raise ValueError("Could not find GKG URL")
        
        gkg_resp = await client.get(gkg_url)
        gkg_resp.raise_for_status()
        
    with zipfile.ZipFile(io.BytesIO(gkg_resp.content)) as z:
        csv_filename = z.namelist()[0]
        with z.open(csv_filename) as f:
            # Load only first 1000 rows for debugging to be fast
            df_debug = pd.read_csv(
                f, 
                sep='\t', 
                header=None, 
                nrows=1000,
                encoding='utf-8',
                on_bad_lines='skip'
            )

    return {
        "total_rows_parsed": len(df_debug),
        "column_count": len(df_debug.columns),
        "col_3_sample": df_debug[3].dropna().head(3).tolist() if 3 in df_debug.columns else None,
        "col_9_sample": df_debug[9].dropna().head(3).tolist() if 9 in df_debug.columns else None,
        "col_15_sample": df_debug[15].dropna().head(3).tolist() if 15 in df_debug.columns else None,
        "col_7_sample": df_debug[7].dropna().head(3).tolist() if 7 in df_debug.columns else None
    }
