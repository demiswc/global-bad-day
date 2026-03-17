import io
import json
import os
import zipfile
import logging
import pandas as pd
import httpx
from datetime import datetime, date
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete, and_
from db.models import RawSignal, Headline

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GDELT_LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# Load FIPS to ISO mapping
FIPS_TO_ISO = {}
try:
    mapping_path = os.path.join(os.path.dirname(__file__), "..", "data", "fips_to_iso.json")
    with open(mapping_path, "r") as f:
        FIPS_TO_ISO = json.load(f)
    logger.info(f"Loaded {len(FIPS_TO_ISO)} FIPS->ISO mappings")
except Exception as e:
    logger.error(f"Failed to load FIPS to ISO mapping: {e}")

HEADLINE_BLOCKLIST = {
    "tvguide.co.uk", "tvguide.com", "imdb.com", "rottentomatoes.com",
    "metacritic.com", "entertainment.ie", "digitalspy.com",
    "radiotimes.com", "whats-on-tv.co.uk"
}

def extract_country_codes(locations_str: str) -> list[str]:
    """
    Extracts 2-letter country codes from GDELT location field.
    Example: 1#Iran#IR#IR#32#53#IR;...
    """
    if not isinstance(locations_str, str) or not locations_str.strip():
        return []
    codes = []
    for location in locations_str.split(';'):
        # Split on #
        parts = location.strip().split('#')
        if len(parts) >= 3:
            code = parts[2].strip()
            # Only take clean 2-letter codes
            if len(code) == 2 and code.isalpha():
                codes.append(code.upper())
    return list(set(codes)) # Unique codes per row

async def fetch_latest_gdelt_sentiment() -> tuple[dict[str, float], list[dict]]:
    """
    Fetches the latest GKG file from GDELT, parses sentiment, 
    and returns a normalized badness score per country AND a list of negative headlines.
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
            # Column 3: SourceCommonName
            # Column 4: DocumentIdentifier (URL)
            # Column 9: Locations (structured)
            # Column 15: V2Tone (sentiment)
            df = pd.read_csv(
                f, 
                sep='\t', 
                header=None, 
                usecols=[3, 4, 9, 15],
                names=['SourceCommonName', 'DocumentIdentifier', 'Locations', 'V2Tone'],
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
    df['fips_list'] = df['Locations'].apply(extract_country_codes)
    
    # Explode the list of FIPS codes so each has its own row with the same tone
    df_exploded = df.explode('fips_list')

    # Map FIPS to ISO
    def map_to_iso(fips):
        if pd.isna(fips):
            return None
        return FIPS_TO_ISO.get(fips)

    df_exploded['country_code'] = df_exploded['fips_list'].apply(map_to_iso)

    # Drop rows with missing data or no ISO mapping
    df_clean = df_exploded.dropna(subset=['tone', 'country_code'])
    
    # Process headlines (from negative sentiment only)
    # Keep only rows where tone < -2.0 and URL starts with http
    headlines_df = df_clean[
        (df_clean['tone'] < -2.0) & 
        (df_clean['DocumentIdentifier'].str.startswith('http', na=False))
    ].copy()

    # Limit to 500 headline records to keep database lean
    headlines_df = headlines_df.head(500)
    
    headlines_list = []
    for _, row in headlines_df.iterrows():
        source = str(row['SourceCommonName']).lower()
        if source in HEADLINE_BLOCKLIST:
            continue
            
        headlines_list.append({
            "country_code": row['country_code'],
            "url": row['DocumentIdentifier'],
            "source_name": row['SourceCommonName'],
            "tone": row['tone']
        })

    # Group by ISO country code and calculate mean tone for badness scores
    country_tones = df_clean.groupby('country_code')['tone'].mean()

    # Normalize tone to 0-1 badness score
    def normalize_tone(tone):
        score = (tone * -1 + 100) / 200
        return max(0.0, min(1.0, score))

    badness_scores = country_tones.apply(normalize_tone).to_dict()
    
    return badness_scores, headlines_list

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

async def save_headlines(headlines: list[dict], db: AsyncSession):
    """
    Saves the extracted headlines to the database.
    """
    if not headlines:
        return

    today = date.today()
    timestamp = datetime.utcnow()
    
    # Affected countries
    countries = list(set([h['country_code'] for h in headlines]))
    
    # Delete today's existing headlines for these countries to avoid duplicates
    await db.execute(
        delete(Headline).where(
            and_(
                Headline.date == today,
                Headline.country_code.in_(countries)
            )
        )
    )
    
    new_headlines = [
        Headline(
            country_code=h['country_code'],
            date=today,
            url=h['url'],
            source_name=h['source_name'],
            tone=h['tone'],
            timestamp=timestamp
        )
        for h in headlines
    ]
    
    db.add_all(new_headlines)
    await db.commit()
    logger.info(f"Saved {len(new_headlines)} headlines to database.")

async def run_gdelt_ingestion(db: AsyncSession):
    """
    Orchestrates the GDELT ingestion process.
    """
    try:
        logger.info("Starting GDELT news sentiment ingestion...")
        scores, headlines = await fetch_latest_gdelt_sentiment()
        
        logger.info(f"Found news sentiment for {len(scores)} countries and {len(headlines)} headlines.")
        
        await save_gdelt_scores(scores, db)
        await save_headlines(headlines, db)
        
        logger.info("GDELT ingestion completed successfully.")
        return len(scores)
    except Exception as e:
        logger.error(f"GDELT ingestion failed: {e}", exc_info=True)
        return 0

