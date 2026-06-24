# 📊 Binance Futures Derivatives Collector

An AWS Lambda function that automatically fetches derivatives data from the Binance Futures API and stores it as Parquet files in S3. Built for building time-series datasets for trading analysis and model development.

---

## Overview

This function runs on a scheduled trigger (EventBridge) and collects 5 derivatives metrics per symbol from the Binance Futures API. On the first run it pulls a full history (up to 500 rows); on subsequent runs it fetches only the latest N rows and upserts them — deduplicating by timestamp so no rows are ever duplicated.

**Collected metrics per symbol:**

| Metric | Binance Endpoint | Column Prefix |
| :---- | :---- | :---- |
| Open Interest | `/futures/data/openInterestHist` | `oi_` |
| Global Long/Short Account Ratio | `/futures/data/globalLongShortAccountRatio` | `gls_` |
| Top Trader Account Ratio | `/futures/data/topLongShortAccountRatio` | `tta_` |
| Top Trader Position Ratio | `/futures/data/topLongShortPositionRatio` | `ttp_` |
| Taker Buy/Sell Volume | `/futures/data/takerlongshortRatio` | `tv_` |

---

## Architecture

        EventBridge (cron schedule)

        │

          AWS Lambda Function

        │

        ├── Binance Futures API  ──→  fetch 5 metrics per symbol

        │

        └── Amazon S3

              └── binance-futures/
              
                    └── symbol={SYMBOL}/

                            └── {SYMBOL}-derivative-{PERIOD}.parquet

**First run:** fetches up to 500 rows (full history) per symbol. (binance API limit)  
**Subsequent runs:** fetches `LIMIT` rows, upserts, deduplicates by timestamp.

---

## Environment Variables

Configure these in your Lambda function's environment settings:

| Variable | Required | Default | Description |
| :---- | :---- | :---- | :---- |
| `S3_BUCKET` | ✅ | — | S3 bucket name where Parquet files are stored |
| `SYMBOLS` | ✅ | — | Comma-separated list of trading pairs (e.g. `BTCUSDT,ETHUSDT`) |
| `PERIOD` | ❌ | `1h` | Candle/aggregation period (`5m`, `15m`, `1h`, `4h`, `1d`) |
| `LIMIT` | ❌ | `72` | Number of rows fetched per incremental run |

**Example:**

S3\_BUCKET	\= my-trading-data-bucket

SYMBOLS	\= BTCUSDT,ETHUSDT,SOLUSDT

PERIOD	\= 1h

LIMIT	\= 72

---

## S3 Output

One Parquet file per symbol is written to:

s3://{S3\_BUCKET}/binance-futures/symbol={SYMBOL}/{SYMBOL}-derivative-{PERIOD}.parquet

The file is an indexed DataFrame with a UTC `timestamp` index and wide-format columns:

timestamp (index)

├── oi\_sumOpenInterest

├── oi\_sumOpenInterestValue

├── gls\_longAccount

├── gls\_shortAccount

├── gls\_longShortRatio

├── tta\_longAccount

├── tta\_shortAccount

├── tta\_longShortRatio

├── ttp\_longAccount

├── ttp\_shortAccount

├── ttp\_longShortRatio

├── tv\_buySellRatio

├── tv\_buyVol

└── tv\_sellVol

---

## IAM Permissions Required

The Lambda execution role needs the following S3 permissions on your bucket:

        {

          "Effect": "Allow",
        
          "Action": \[

            "s3:GetObject",
        
            "s3:PutObject"

          \],

          "Resource": "arn:aws:s3:::your-bucket-name/binance-futures/\*"
        
        },
            {
              "Effect": "Allow",
              "Action": "s3:ListBucket",
              "Resource": "arn:aws:s3:::your-bucket-name"
            }
---

## Dependencies

requests

pandas

pyarrow

boto3

`boto3` is pre-installed in the Lambda runtime. For `requests`, `pandas`, and `pyarrow`, package them into a Lambda layer or include them in your deployment ZIP.

---

## Deployment

### Lambda Settings

| Setting | Recommended |
| :---- | :---- |
| Runtime | Python 3.12 |
| Memory | 256 MB (depend on number of symbols) |
| Timeout | 60 seconds |
| Architecture | any |

### EventBridge Schedule

**Schedule design rationale:**

| Setting | Value | Reason |
| :---- | :---- | :---- |
| `PERIOD` | `1h` | 1-hour candles |
| `LIMIT` | `72` | 72 hours \= 3 days of data per run |
| Run frequency | **Once daily** | 24 new candles/day; 72 limit gives a 3-day error buffer |
| Binance max | `500` rows | First-run only; well within the daily limit |

Running once daily is sufficient for `1h` candles — only 24 new rows are produced per day. The `LIMIT=72` buffer means the pipeline can miss up to **3 consecutive days** before any data gap occurs. This minimises Lambda invocations and cost while keeping the dataset complete.

---

## Error Handling

- **Per-symbol isolation:** if one symbol fails (API error, bad data), the others continue processing. Failures are captured in the return summary.  
- **Missing file (first run):** `s3.head_object` raises `ClientError` → triggers `first_run=True` → fetches 500 rows.  
- **API non-200 response:** `fetch_derivative` returns `None`; the metric is skipped without crashing the full run.  
- **Return body** always includes a per-symbol summary:

{

  "summary": {

    "BTCUSDT": {"new\_rows": 72, "total\_rows": 1440},

    "ETHUSDT": {"new\_rows": 72, "total\_rows": 1440}

  }

}

---

## Notes

- All timestamps are stored in **UTC**.  
- Columns are **outer-joined** across metrics — if one metric returns fewer rows than others, missing cells will be `NaN`.  
- The upsert logic uses `keep="last"` deduplication, so re-running the function for the same time window is safe.  
- Binance rate limits apply — avoid running too many symbols simultaneously. For large symbol lists, consider batching across multiple Lambda invocations.

