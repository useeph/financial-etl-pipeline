"""Smoke test: 5 tickers, 1 month, verify everything works."""
from extract import extract_prices, save_partitioned
from datetime import datetime, timedelta

end = datetime.today().strftime("%Y-%m-%d")
start = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
df = extract_prices(["AAPL", "MSFT", "GOOGL", "NVDA", "TSLA"], start, end)
print(df.head(10))
print(f"\nTotal rows: {len(df)}")
print(f"Columns: {list(df.columns)}")
save_partitioned(df, run_date=f"test_{end}")