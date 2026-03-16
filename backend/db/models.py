from sqlalchemy import Column, BigInteger, String, DateTime, Float, Date, Boolean, Integer
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class RawSignal(Base):
    __tablename__ = "raw_signals"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    country_code = Column(String(3), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    signal_type = Column(String(32), nullable=False)  # gdelt_sentiment, stock_change, gdp_delta, weather_anomaly
    raw_value = Column(Float, nullable=False)

class NormalisedScore(Base):
    __tablename__ = "normalised_scores"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    country_code = Column(String(3), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    signal_type = Column(String(32), nullable=False)
    score = Column(Float, nullable=False)  # normalised 0.0 to 1.0

class CompositeScore(Base):
    __tablename__ = "composite_scores"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    country_code = Column(String(3), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    bad_day_score = Column(Float, nullable=False)  # weighted composite 0.0 to 1.0
    convergence_flag = Column(Boolean, nullable=False, default=False)
    signal_count = Column(Integer, nullable=False, default=0)

class Headline(Base):
    __tablename__ = "headlines"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    country_code = Column(String(3), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    url = Column(String(2048), nullable=False)
    source_name = Column(String(256), nullable=True)
    tone = Column(Float, nullable=True)
    timestamp = Column(DateTime(timezone=True), nullable=False)
