import logging
import yfinance as yf
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from db.models import RawSignal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COUNTRY_INDICES = {
    "US": "^GSPC",
    "GB": "^FTSE",
    "DE": "^GDAXI",
    "FR": "^FCHI",
    "JP": "^N225",
    "CN": "000001.SS",
    "IN": "^BSESN",
    "BR": "^BVSP",
    "AU": "^AXJO",
    "ZA": "J203.JO",
    "MX": "^MXX",
    "AR": "^MERV",
    "SA": "^TASI.SR",
    "ID": "^JKSE",
    "MY": "^KLSE",
    "TH": "^SET.BK",
    "PH": "PSEi.PS",
    "VN": "^VNINDEX",
    "KR": "^KS11",
    "TW": "^TWII",
    "HK": "^HSI",
    "SG": "^STI",
    "PK": "^KSE100",
    "BD": "^DSEX",
    "LK": "^CSEALL",
    "NG": "^NGSEINDX",
    "KE": "^NSE20",
    "GH": "^GSE-CI",
    "EG": "^CASE",
    "MA": "^MASI",
    "TN": "^TUNINDEX",
    "NL": "^AEX",
    "ES": "^IBEX",
    "IT": "FTSEMIB.MI",
    "PT": "^PSI20",
    "BE": "^BFX",
    "SE": "^OMX",
    "NO": "^OSEAX",
    "DK": "^OMXC25",
    "FI": "^OMXH25",
    "AT": "^ATX",
    "CH": "^SSMI",
    "PL": "^WIG20",
    "CZ": "^PX",
    "HU": "^BUX",
    "RO": "^BET",
    "GR": "^ATG",
    "TR": "XU100.IS",
    "IL": "^TA125.TA",
    "QA": "^QSI",
    "KW": "^KWSE",
    "BH": "^BHSEASI",
    "OM": "^MSM30",
    "JO": "^AMGNRLX",
    "LB": "^BLOM",
    "NZ": "^NZ50",
    "CL": "^IPSA",
    "CO": "^COLCAP",
    "PE": "^SPBLPGPT",
    "VE": "^IBC",
    "UA": "^PFTS",
    "KZ": "^KASE",
}

async def fetch_stock_changes() -> tuple[dict[str, float], list[str]]:
    """
    Fetches stock index data from Yahoo Finance and calculates percentage change.
    Returns a tuple of (scores, skipped_symbols).
    """
    scores = {}
    skipped = []
    
    for country_code, symbol in COUNTRY_INDICES.items():
        logger.info(f"Fetching stock data for {country_code} ({symbol})...")
        try:
            ticker = yf.Ticker(symbol)
            # Fetch last 5 trading days to ensure we have at least 2
            hist = ticker.history(period="5d")
            
            if hist.empty or len(hist) < 2:
                logger.warning(f"Insufficient data for {symbol}")
                skipped.append(symbol)
                continue

            # Get the two most recent closing prices
            # hist index is chronological, so -1 is today, -2 is yesterday
            today_close = hist['Close'].iloc[-1]
            yesterday_close = hist['Close'].iloc[-2]
            
            pct_change = (today_close - yesterday_close) / yesterday_close * 100
            
            # Normalise: -5% drop -> 1.0 (bad), +5% gain -> 0.0 (good)
            # score = (-pct_change + 5) / 10
            score = (-pct_change + 5) / 10
            score = max(0.0, min(1.0, score))
            
            scores[country_code] = score
            logger.info(f"Processed {country_code}: change={pct_change:.2f}%, score={score:.2f}")

        except Exception as e:
            logger.error(f"Failed to fetch stocks for {symbol}: {e}")
            skipped.append(symbol)
            
    return scores, skipped

async def save_stock_scores(scores: dict[str, float], db: AsyncSession):
    """
    Saves the scores as RawSignal records in the database.
    """
    timestamp = datetime.utcnow()
    signals = []
    
    for country_code, score in scores.items():
        signal = RawSignal(
            country_code=country_code,
            timestamp=timestamp,
            signal_type="stock_change",
            raw_value=score
        )
        signals.append(signal)
    
    if signals:
        db.add_all(signals)
        await db.commit()
        logger.info(f"Saved {len(signals)} stock change signals to database.")

async def run_stock_ingestion(db: AsyncSession):
    """
    Orchestrates the stock ingestion process.
    """
    logger.info("Starting Yahoo Finance stock market ingestion...")
    
    try:
        scores, skipped = await fetch_stock_changes()
        await save_stock_scores(scores, db)
        logger.info("Stock ingestion completed successfully.")
        return len(scores), skipped
    except Exception as e:
        logger.error(f"Stock ingestion failed: {e}", exc_info=True)
        return 0, list(COUNTRY_INDICES.values())
