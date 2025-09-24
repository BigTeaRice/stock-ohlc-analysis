# ------------------------------
# 導入所需套件
# ------------------------------
import pandas as pd
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime
import os
import traceback
import pytz
import time

# ------------------------------
# 全域參數設定
# ------------------------------
TICKER = "0700.HK"  # 港股正确代码（0700.HK）
START_DATE = "2004-06-16"  # 腾讯上市日期
END_DATE = datetime.today().strftime("%Y-%m-%d")
CACHE_DIR = "data"
CACHE_FILE = os.path.join(CACHE_DIR, f"{TICKER.replace('.', '-')}.csv")
HONG_KONG_TZ = pytz.timezone('Asia/Hong_Kong')
MAX_RETRIES = 3
RETRY_DELAY = 5

# ------------------------------
# 函式定義：數據獲取與緩存（修復列名問題）
# ------------------------------
def fetch_and_cache_data(ticker, start_date, end_date, cache_dir, cache_file, tz):
    try:
        os.makedirs(cache_dir, exist_ok=True)

        # 讀取緩存（若存在）
        if os.path.exists(cache_file):
            print(f"📂 嘗試讀取緩存：{cache_file}")
            df = pd.read_csv(cache_file, parse_dates=["Date"], encoding='utf-8')
            if df.empty:
                raise ValueError("緩存數據為空")
            # 檢查緩存列名是否包含必要字段
            required_cols = ["Open", "High", "Low", "Close"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"緩存缺少必要列：{missing_cols}")
            # 驗證時間範圍
            min_date = df["Date"].min().strftime('%Y-%m-%d')
            max_date = df["Date"].max().strftime('%Y-%m-%d')
            if min_date >= start_date and max_date <= end_date:
                print(f"✅ 緩存有效（{min_date} 至 {max_date}）")
                return df
            else:
                print("❌ 緩存時間範圍不匹配，重新下載")

        # 下載數據（yfinance 默認返回大寫列名：Open/High/Low/Close）
        for attempt in range(MAX_RETRIES):
            try:
                print(f"⏳ 下載數據（第 {attempt+1}/{MAX_RETRIES} 次）")
                df = yf.download(
                    tickers=ticker,
                    start=start_date,
                    end=end_date,
                    progress=False,
                    auto_adjust=True,
                    actions=False
                )
                if df.empty:
                    raise ValueError("Yahoo Finance 無數據")
                
                # 🔧 修復：將多層索引列名轉換為普通列名（關鍵！）
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.droplevel(1)  # 去除第二層索引（股票代碼）
                
                # 檢查下載的列名
                required_cols = ["Open", "High", "Low", "Close"]
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    raise ValueError(f"下載數據缺少列：{missing_cols}")
                
                # 時區轉換
                df.index = df.index.tz_localize('UTC').tz_convert(tz)
                df = df.reset_index()
                
                # 保存緩存
                df.to_csv(cache_file, index=False, encoding='utf-8')
                print(f"💾 數據保存到緩存：{cache_file}")
                return df

            except Exception as e:
                print(f"❌ 下載失敗（第 {attempt+1} 次）：{str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    raise RuntimeError(f"下載失敗（超過 {MAX_RETRIES} 次）") from e

    except Exception as e:
        error_msg = (
            f"時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"函式：fetch_and_cache_data\n"
            f"錯誤：{str(e)}\n"
            f"堆疊：{traceback.format_exc()}"
        )
        with open("error.log", "w", encoding='utf-8') as f:
            f.write(error_msg)
        raise

# ------------------------------
# 函式定義：數據預處理（修復列名問題）
# ------------------------------
def preprocess_data(df):
    try:
        print("🔄 開始預處理數據...")
        
        # 🔧 修復：再次確保列名是普通索引（避免緩存讀取時的問題）
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        
        # 1. 去除列名前後空格（解決「Open 」或「 open」等問題）
        df.columns = df.columns.str.strip()
        print(f"✅ 列名處理完成：{df.columns.tolist()}")

        # 2. 檢查必要列是否存在（Open/High/Low/Close）
        required_cols = ["Open", "High", "Low", "Close"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"數據缺少必要列：{missing_cols}！實際列名：{df.columns.tolist()}")

        # 3. 轉換日期欄位並排序
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert(HONG_KONG_TZ)
        df = df.sort_values(by="Date").reset_index(drop=True)

        # 4. 處理缺失值
        initial_count = len(df)
        df = df.dropna(subset=required_cols)
        deleted_rows = initial_count - len(df)
        if deleted_rows > 0:
            print(f"⚠️ 刪除 {deleted_rows} 行缺失值數據")

        # 5. 驗證數據量
        if len(df) < 2:
            raise ValueError("預處理後數據不足（少於 2 行）")

        print("✅ 預處理完成！")
        return df

    except Exception as e:
        error_msg = (
            f"時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"函式：preprocess_data\n"
            f"錯誤：{str(e)}\n"
            f"堆疊：{traceback.format_exc()}"
        )
        with open("error.log", "a", encoding='utf-8') as f:
            f.write(error_msg)
        raise

# ------------------------------
# 函式定義：繪製圖表
# ------------------------------
def plot_ohlc_chart(df, ticker):
    try:
        print("📈 開始繪製圖表...")
        df["Date_Str"] = df["Date"].dt.strftime("%Y-%m-%d")

        fig = go.Figure(data=[go.Ohlc(
            x=df["Date_Str"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name=ticker,
            increasing_line_color='#ff0000',
            decreasing_line_color='#00ff00'
        )])

        fig.update_layout(
            title={"text": f"{ticker} 歷史K線圖", "x": 0.5},
            xaxis_title="日期",
            yaxis_title="價格 (HKD)",
            xaxis_rangeslider_visible=False,
            template="plotly_white"
        )

        output_path = "./ohlc_chart.html"
        fig.write_html(output_path, include_plotlyjs="cdn", auto_open=True)
        print(f"✅ 圖表生成：{output_path}")

        return output_path

    except Exception as e:
        error_msg = (
            f"時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"函式：plot_ohlc_chart\n"
            f"錯誤：{str(e)}\n"
            f"堆疊：{traceback.format_exc()}"
        )
        with open("error.log", "a", encoding='utf-8') as f:
            f.write(error_msg)
        raise

# ------------------------------
# 主程式入口
# ------------------------------
if __name__ == "__main__":
    try:
        print("🚀 開始運行程式...")
        # 1. 獲取/緩存數據
        df = fetch_and_cache_data(
            ticker=TICKER,
            start_date=START_DATE,
            end_date=END_DATE,
            cache_dir=CACHE_DIR,
            cache_file=CACHE_FILE,
            tz=HONG_KONG_TZ
        )
        # 打印下載數據的列名（調試用）
        print(f"📊 下載數據的列名：{df.columns.tolist()}")

        # 2. 預處理數據
        processed_df = preprocess_data(df)
        # 打印預處理後的列名（調試用）
        print(f"🔍 預處理後的列名：{processed_df.columns.tolist()}")

        # 3. 繪製圖表
        plot_ohlc_chart(processed_df, TICKER)

        print("🎉 程式執行成功！")

    except Exception as e:
        print(f"❌ 程式執行失敗：{str(e)}")
        exit(1)
