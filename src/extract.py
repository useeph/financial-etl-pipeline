"""Extract S&P 500 daily OHLCV data from yfinance to local parquet.

Output layout mimics the S3 partitioning we'll use in Phase 3:
  data/raw/date=YYYY-MM-DD/prices.parquet
"""
from __future__ import annotations

import io
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

DATA_DIR = Path("data/raw")
SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def get_sp500_tickers() -> list[str]:
    """Scrape the current S&P 500 ticker list from Wikipedia."""
    log.info("Fetching S&P 500 ticker list from Wikipedia")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    }
    response = requests.get(SP500_WIKI_URL, headers=headers, timeout=30)
    response.raise_for_status()

    tables = pd.read_html(io.StringIO(response.text))
    df = tables[0]  # First table is the constituents list
    # yfinance uses '-' instead of '.' (BRK.B -> BRK-B)
    tickers = df["Symbol"].str.replace(".", "-", regex=False).tolist()
    log.info(f"Found {len(tickers)} tickers")
    return tickers


def extract_prices(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Download daily OHLCV data for all tickers in one batch call."""
    log.info(f"Downloading {len(tickers)} tickers from {start} to {end}")

    raw = yf.download(
        tickers=tickers,
        start=start,
        end=end,
        group_by="ticker",
        auto_adjust=False,
        progress=True,
        threads=True,
    )

    # Reshape wide multi-index format -> long format
    frames = []
    for ticker in tickers:
        if ticker not in raw.columns.get_level_values(0):
            log.warning(f"No data returned for {ticker}, skipping")
            continue
        df = raw[ticker].copy()
        df["ticker"] = ticker
        df = df.reset_index()
        df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
        frames.append(df)

    if not frames:
        raise RuntimeError("No data extracted for any ticker")

    result = pd.concat(frames, ignore_index=True)
    result = result.dropna(subset=["close"])
    result["extracted_at"] = datetime.now(timezone.utc)

    log.info(f"Extracted {len(result):,} rows across {result['ticker'].nunique()} tickers")
    return result


def save_partitioned(df: pd.DataFrame, run_date: str) -> Path:
    """Save as parquet under date=YYYY-MM-DD/ partition (S3-style)."""
    out_dir = DATA_DIR / f"date={run_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "prices.parquet"
    df.to_parquet(out_path, index=False, compression="snappy")

    size_mb = out_path.stat().st_size / 1024 / 1024
    log.info(f"Wrote {out_path} ({size_mb:.2f} MB)")
    return out_path


def main(years_back: int = 5) -> None:
    end = datetime.today().strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=years_back * 365)).strftime("%Y-%m-%d")

    tickers = get_sp500_tickers()
    df = extract_prices(tickers, start, end)
    save_partitioned(df, run_date=end)

    log.info("Extract complete")


if __name__ == "__main__":
    main()