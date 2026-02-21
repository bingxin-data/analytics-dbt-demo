import os
from datetime import datetime, timezone
import time

import requests
import snowflake.connector


# Edit this list to your own tickers
TICKERS = ["CRWV", "USAR", "VOO", "ROBO", "LEU", "NVDA", "NFLX"]


def fetch_history_for_symbol(symbol: str):
    """
    Fetch last ~100 daily OHLC data for a symbol using Alpha Vantage (compact).
    We will upsert these into Snowflake. Older data stays in our table.
    """
    api_key = os.environ["ALPHAVANTAGE_API_KEY"]

    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY&symbol={symbol}"
        f"&outputsize=compact&apikey={api_key}"
    )

    resp = requests.get(url, timeout=20)

    if resp.status_code == 429:
        print(f"  ⚠️  Got 429 (Too Many Requests) from Alpha Vantage for {symbol}, skipping.")
        return []

    if not resp.ok:
        print(f"  ⚠️  Failed to fetch {symbol}: HTTP {resp.status_code}")
        return []

    data = resp.json()

    if "Time Series (Daily)" not in data:
        print(f"  ⚠️  Unexpected response for {symbol}: {list(data.keys())}")
        print(f"  Message: {data.get('Information') or data.get('Note') or data}")
        return []

    ts = data["Time Series (Daily)"]

    records = []
    for date_str, vals in ts.items():
        as_of_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        o = float(vals["1. open"])
        h = float(vals["2. high"])
        l = float(vals["3. low"])
        c = float(vals["4. close"])
        v = int(vals["5. volume"])

        records.append(
            (
                as_of_date,
                symbol,
                o,
                h,
                l,
                c,
                v,
            )
        )

    # sort by date ascending
    records.sort(key=lambda r: r[0])
    return records





def insert_history_into_snowflake(records):
    """
    Upsert rows into DBT_DEMO.RAW.STOCK_PRICES.
    Uses fully qualified names so we don't depend on current schema.
    """
    if not records:
        return

    account = os.environ["SNOWFLAKE_ACCOUNT"]
    user = os.environ["SNOWFLAKE_USER"]
    password = os.environ["SNOWFLAKE_PASSWORD"]
    role = os.environ["SNOWFLAKE_ROLE"]
    warehouse = os.environ["SNOWFLAKE_WH"]
    database = os.environ["SNOWFLAKE_DB"]
    schema = os.environ.get("SNOWFLAKE_RAW_SCHEMA", "RAW")

    ctx = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )
    try:
        cs = ctx.cursor()

        # Make sure schema exist
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")

        # Fully qualified table name
        fq_table = f"{database}.{schema}.STOCK_PRICES"

        # Create table if not exists
        cs.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {fq_table} (
              AS_OF_DATE   DATE,
              SYMBOL       STRING,
              OPEN_PRICE   FLOAT,
              HIGH_PRICE   FLOAT,
              LOW_PRICE    FLOAT,
              CLOSE_PRICE  FLOAT,
              VOLUME       NUMBER,
              FETCHED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
              CONSTRAINT PK_STOCK_PRICES PRIMARY KEY (AS_OF_DATE, SYMBOL)
            )
            """
        )

        delete_sql = f"""
            DELETE FROM {fq_table}
            WHERE AS_OF_DATE = %s AND SYMBOL = %s
        """

        insert_sql = f"""
            INSERT INTO {fq_table} (
              AS_OF_DATE,
              SYMBOL,
              OPEN_PRICE,
              HIGH_PRICE,
              LOW_PRICE,
              CLOSE_PRICE,
              VOLUME
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for rec in records:
            as_of_date, symbol, o, h, l, c, v = rec
            cs.execute(delete_sql, (as_of_date, symbol))
            cs.execute(insert_sql, rec)

        ctx.commit()
    finally:
        cs.close()
        ctx.close()



def main():
    total = 0
    for symbol in TICKERS:
        print(f"Fetching last ~100 days for {symbol} ...")
        records = fetch_history_for_symbol(symbol)
        print(f"  Got {len(records)} rows, upserting into Snowflake...")
        insert_history_into_snowflake(records)
        total += len(records)

        # respect Alpha Vantage rate limit (5 calls/min)
        time.sleep(15)

    print(f"Done. Upserted {total} stock price rows into Snowflake.")



if __name__ == "__main__":
    main()
