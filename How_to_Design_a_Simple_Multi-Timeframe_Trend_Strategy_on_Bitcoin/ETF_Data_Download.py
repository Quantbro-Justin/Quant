# =======================================================
# TuShare Pro - 获取沪深300ETF(510300.SH) 30分钟历史行情
# 接口: fund_min (适用于 Pro会员 5000积分及以上)
# =======================================================
import tushare as ts
import pandas as pd
import time

# ====== 1️⃣ 设置 TuShare Token ======
TOKEN = "6eeadde0c615452a0b1015218259e8175877a7c04d11c3c95eb22b57"
ts.set_token(TOKEN)
pro = ts.pro_api()

# ====== 2️⃣ 获取函数 ======
def get_etf_min(symbol="510300.SH",
                start="2012-05-28 09:30:00",
                end="2025-11-17 15:00:00",
                freq="30min",
                fname="HS300ETF_fund_min_30m.csv",
                retry=5,
                delay=2):
    """
    用 fund_min 接口获取ETF分钟级行情（会员可全量）
    :param symbol: ETF代码
    :param start: 起始时间 (格式: YYYY-MM-DD HH:MM:SS)
    :param end:   结束时间 (格式: YYYY-MM-DD HH:MM:SS)
    :param freq:  频率 (1min,5min,15min,30min,60min)
    """
    for i in range(retry):
        try:
            print(f"🚀 正在下载 {symbol} {freq} 数据 ({start} → {end}) ...")

            df = pro.fund_min(
                ts_code=symbol,
                start_time=start,
                end_time=end,
                freq=freq
            )

            if df is None or df.empty:
                print("⚠️ 返回空数据，请检查权限、时间或接口版本。")
                time.sleep(delay)
                continue

            # 格式清洗
            df.rename(columns={
                "ts_code": "code",
                "trade_time": "datetime",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "vol": "volume",
                "amount": "amount"
            }, inplace=True, errors='ignore')

            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.sort_values("datetime").reset_index(drop=True)

            # 保存
            df.to_csv(fname, index=False, encoding="utf-8-sig")
            print(f"✅ 下载成功并保存为 {fname} （{len(df)} 行）")
            return df

        except Exception as e:
            print(f"⚠️ 第 {i+1} 次尝试失败: {e}")
            time.sleep(delay)

    print("❌ 多次重试仍失败，请检查网络或权限。")
    return pd.DataFrame()

# ====== 3️⃣ 主流程 ======
if __name__ == "__main__":
    symbol = "510300.SH"
    start_time = "2020-01-01 09:30:00"
    end_time = "2025-11-17 15:00:00"

    df_30min = get_etf_min(symbol, start=start_time, end=end_time, freq="30min")

    if not df_30min.empty:
        print("\n📊 数据样例：")
        print(df_30min.head())
        print(df_30min.tail())