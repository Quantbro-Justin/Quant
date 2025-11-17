"""
Gemini BTC/USD Historical Dataset (Full History via CryptoDataDownload)
-----------------------------------------------------------------------
Fetches Gemini BTC/USD 1-hour and 1-day OHLCV data from CryptoDataDownload.com,
cleans the dataset for quantitative MACD strategy (Version 1).

Author : OpenAI Quant Assistant
Date   : 2025-11-17
Reference : Quantpedia "Bitcoin MACD Multi-Timeframe Strategy"
"""

import pandas as pd

# --------------------------------------------------------
# 📌 URLs of public datasets
# --------------------------------------------------------
sources = {
    "1h": "https://www.cryptodatadownload.com/cdd/Gemini_BTCUSD_1h.csv",
    "1d": "https://www.cryptodatadownload.com/cdd/Gemini_BTCUSD_d.csv"
}

# --------------------------------------------------------
# 🧩 Unified cleaning function
# --------------------------------------------------------
def clean_cryptodatadownload(url: str, tf: str) -> pd.DataFrame:
    print(f"📥 Downloading Gemini BTC/USD {tf} data ...")
    df = pd.read_csv(url, skiprows=1)   # skip the top comment line

    # Normalize column names (remove spaces & lowercase)
    df.columns = [c.strip().replace(" ", "").lower() for c in df.columns]

    # Rename to standard names
    rename_dict = {
        "date": "Time",
        "unix": "Unix",
        "symbol": "Symbol",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volumebtc": "Volume",
        "volumeusd": "VolumeUSD"
    }
    df.rename(columns=rename_dict, inplace=True)

    # Parse datetime column
    if "Time" in df.columns:
        df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")
    elif "date" in df.columns:
        df["Time"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    else:
        raise KeyError("No recognizable Time/date column found.")

    # Drop missing timestamps
    df = df.dropna(subset=["Time"]).sort_values("Time")

    # Safely convert numeric columns
    possible_cols = ["Open", "High", "Low", "Close", "Volume", "VolumeUSD"]
    existing_cols = [c for c in possible_cols if c in df.columns]
    df[existing_cols] = df[existing_cols].apply(pd.to_numeric, errors="coerce")

    # Filter by Quantpedia period (2018-12 → 2025-11)
    df = df[(df["Time"] >= "2018-12-01") & (df["Time"] <= "2025-11-01")]

    # Keep only essential columns
    keep_cols = ["Time", "Open", "High", "Low", "Close"]
    if "Volume" in df.columns:
        keep_cols.append("Volume")
    df = df[keep_cols]

    # Final clean output
    print(f"✅ {tf}: {len(df):,} bars from {df['Time'].min()} to {df['Time'].max()}")
    return df.reset_index(drop=True)

# --------------------------------------------------------
# 🚀 Main execution
# --------------------------------------------------------
def main():
    for tf, url in sources.items():
        try:
            df = clean_cryptodatadownload(url, tf)
            outfile = f"./Gemini_BTCUSD_{tf}_clean.csv"
            df.to_csv(outfile, index=False)
            print(f"💾 Saved → {outfile}\n")
        except Exception as e:
            print(f"⚠️ Failed for {tf}: {e}\n")

if __name__ == "__main__":
    main()