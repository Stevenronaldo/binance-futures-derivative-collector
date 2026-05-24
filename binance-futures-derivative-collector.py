import requests
import io
import os
import json
import boto3
import numpy as np
import pandas as pd

URL = 'https://fapi.binance.com'

ENDPOINTS ={
    'open_interest' : '/futures/data/openInterestHist',
    'global_long_short' : '/futures/data/globalLongShortAccountRatio',
    'top_trader_accounts' : '/futures/data/topLongShortAccountRatio',
    'top_trader_positions' : '/futures/data/topLongShortPositionRatio',
    'taker_volume' : '/futures/data/takerlongshortRatio'
}

# Short prefixes used when combining columns into one DataFrame
PREFIXES = {
    "open_interest":        "oi",
    "global_long_short":    "gls",
    "top_trader_accounts":  "tta",
    "top_trader_positions": "ttp",
    "taker_volume":         "tv",
}

# Read from Lambda environment variables
S3_BUCKET = os.environ["S3_BUCKET"]
SYMBOLS   = [s.strip() for s in os.environ["SYMBOLS"].split(",") if s.strip()]
PERIOD    = os.environ.get("PERIOD", "1h")
LIMIT     = int(os.environ.get("LIMIT", "72"))

# boto3 client created once at module level — reused across warm invocations
s3 = boto3.client("s3")

def fetch_derivative(path, params):
    """
    Fetch one derivative from Binance Futures API.
    Returns a DataFrame indexed by UTC timestamp.
    """
    response = requests.get(path, params = params)
    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc = True) #make sure datetime is UTC
        df = df.set_index('timestamp')
        df = df.drop(columns = ['symbol'], errors = 'ignore')
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors = "coerce")
        return df

def fetch_all_derivative(URL, symbol, period, limit, first_run = True):
    """
    Fetch all derivative listed in ENDPOINTS for one symbol and merge into one wide DataFrame.
    Columns are prefixed by derivative (e.g. 'oi_sumOpenInterest') to avoid collisions.
    Returns empty DataFrame if every metric fails.
    """
    params = {'symbol' : symbol, 'period' : period, 'limit' : 500 if first_run else limit}
    frames  = []
    for metric in ENDPOINTS:
        try:
            df = fetch_derivative(URL + ENDPOINTS[metric], params)
            if df.empty:
                print(f"  [{symbol}] {metric}: no data")
                continue
            df.columns = [f"{PREFIXES[metric]}_{c}" for c in df.columns]
            frames.append(df)
        except requests.RequestException as e:
            print(f"  [{symbol}] {metric}: FAILED ({e})")

    if not frames:
        return pd.DataFrame()

    # Outer-join on timestamp index — keeps all timestamps from any metric
    combined = pd.concat(frames, axis=1).sort_index()
    return combined

def upsert_to_s3(bucket, key, new_df):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing = pd.read_parquet(io.BytesIO(obj["Body"].read()))

        combined = pd.concat([existing, new_df])
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()

    except s3.exceptions.NoSuchKey:
        combined = new_df  # First write — no existing file

    buf = io.BytesIO()
    combined.to_parquet(buf, engine="pyarrow")
    buf.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    return len(combined)

# ─── Lambda entry point ──────────────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Triggered by EventBridge (daily schedule).
    Loops over SYMBOLS, fetches 5 metrics each, writes one parquet per symbol.
    """
    print(f"Start: symbols={SYMBOLS} period={PERIOD} limit={LIMIT}")
    summary = {}

    with requests.Session() as session:
        for symbol in SYMBOLS:
            try:
              # --- CHECK IF FILE EXISTS ---
                key = f"binance-futures-derivative/{symbol}-derivative-{PERIOD}.parquet"
                try:
                    s3.head_object(Bucket=S3_BUCKET, Key= key)
                    first_run = False
                except s3.exceptions.ClientError:
                    print('file did not exist running Limit 500')
                    first_run = True

                df = fetch_all_derivative(URL, symbol, PERIOD, LIMIT, first_run = first_run)
                if df.empty:
                    print(f"[{symbol}] no data — skipping")
                    summary[symbol] = {"new_rows": 0, "total_rows": 0}
                    continue

                total = upsert_to_s3(S3_BUCKET, key, df)

                print(f"[{symbol}] wrote {len(df)} new rows → {total} total in {key}")
                summary[symbol] = {"new_rows": len(df), "total_rows": total}

            except Exception as e:
                # One symbol failing shouldn't kill other symbols
                print(f"[{symbol}] FAILED: {e}")
                summary[symbol] = {"error": str(e)}

    print(f"Done: {summary}")
    return {
        "statusCode": 200,
        "body": json.dumps({"summary": summary}),
    }