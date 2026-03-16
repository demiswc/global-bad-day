import logging
from datetime import date, datetime, time
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from db.models import RawSignal, NormalisedScore, CompositeScore

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SIGNAL_WEIGHTS = {
    "gdelt_sentiment": 0.40,
    "weather_anomaly": 0.25,
    "stock_change":    0.35,
}

CONVERGENCE_THRESHOLD = 0.65

async def compute_daily_scores(target_date: date, db: AsyncSession) -> list[dict]:
    """
    Computes daily composite scores for all countries by aggregating raw signals.
    """
    logger.info(f"Computing daily scores for {target_date}...")

    # Define the range for the target date
    start_dt = datetime.combine(target_date, time.min)
    end_dt = datetime.combine(target_date, time.max)

    # 1. Query raw_signals for the target date
    stmt = select(
        RawSignal.country_code,
        RawSignal.signal_type,
        func.avg(RawSignal.raw_value).label("avg_value")
    ).where(
        and_(
            RawSignal.timestamp >= start_dt,
            RawSignal.timestamp <= end_dt
        )
    ).group_by(
        RawSignal.country_code,
        RawSignal.signal_type
    )

    result = await db.execute(stmt)
    rows = result.all()

    if not rows:
        logger.info(f"No raw signals found for {target_date}.")
        return []

    # Organize data by country
    country_data = {}
    
    for row in rows:
        country_code = row.country_code
        signal_type = row.signal_type
        avg_score = float(row.avg_value)

        if country_code not in country_data:
            country_data[country_code] = {}
        
        country_data[country_code][signal_type] = avg_score

        # 2. Upsert into normalised_scores
        # Using a simple approach: delete and insert (manual upsert)
        stmt_del = select(NormalisedScore).where(
            and_(
                NormalisedScore.country_code == country_code,
                NormalisedScore.date == target_date,
                NormalisedScore.signal_type == signal_type
            )
        )
        existing_norm = await db.execute(stmt_del)
        norm_obj = existing_norm.scalar_one_or_none()

        if norm_obj:
            norm_obj.score = avg_score
        else:
            new_norm = NormalisedScore(
                country_code=country_code,
                date=target_date,
                signal_type=signal_type,
                score=avg_score
            )
            db.add(new_norm)

    # 3. Calculate composite scores
    results_list = []
    convergence_count = 0

    for country_code, signals in country_data.items():
        total_weight = 0.0
        weighted_sum = 0.0
        all_above_threshold = True
        signal_count = len(signals)

        for signal_type, score in signals.items():
            weight = SIGNAL_WEIGHTS.get(signal_type, 0.0)
            weighted_sum += score * weight
            total_weight += weight
            
            if score < CONVERGENCE_THRESHOLD:
                all_above_threshold = False

        if total_weight > 0:
            bad_day_score = weighted_sum / total_weight
        else:
            bad_day_score = 0.0

        convergence_flag = all_above_threshold and signal_count > 0
        if convergence_flag:
            convergence_count += 1

        # 4. Upsert into composite_scores
        stmt_composite = select(CompositeScore).where(
            and_(
                CompositeScore.country_code == country_code,
                CompositeScore.date == target_date
            )
        )
        existing_composite = await db.execute(stmt_composite)
        comp_obj = existing_composite.scalar_one_or_none()

        if comp_obj:
            comp_obj.bad_day_score = bad_day_score
            comp_obj.convergence_flag = convergence_flag
            comp_obj.signal_count = signal_count
        else:
            new_composite = CompositeScore(
                country_code=country_code,
                date=target_date,
                bad_day_score=bad_day_score,
                convergence_flag=convergence_flag,
                signal_count=signal_count
            )
            db.add(new_composite)

        results_list.append({
            "country_code": country_code,
            "bad_day_score": bad_day_score,
            "convergence_flag": convergence_flag,
            "signal_count": signal_count
        })

    await db.commit()
    return results_list

async def run_scoring(target_date: date, db: AsyncSession):
    """
    Orchestrates the scoring process.
    """
    try:
        results = await compute_daily_scores(target_date, db)
        convergence_events = sum(1 for r in results if r["convergence_flag"])
        logger.info(f"Scored {len(results)} countries for {target_date}, {convergence_events} convergence events")
        return len(results), convergence_events
    except Exception as e:
        logger.error(f"Scoring failed for {target_date}: {e}", exc_info=True)
        return 0, 0
