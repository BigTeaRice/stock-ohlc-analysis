# ------------------------------
# 全域參數設定（優先讀取環境變量）
# ------------------------------
import os
import argparse
import pandas as pd
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime
import time
import pytz
import traceback

# 1. 基礎配置（環境變量覆蓋默認值）
TICKER = os.getenv('TICKER', '0700.HK')                  # 港股代碼（默認騰訊）
START_DATE = os.getenv('START_DATE', '2004-06-16')       # 起始日期（騰訊上市日）
END_DATE = os.getenv('END_DATE', datetime.today().strftime("%Y-%m-%d"))  # 結束日期（今日）
CACHE_DIR = os.getenv('CACHE_DIR', 'stock_data')         # 數據緩存目錄（與工作流一致）
CACHE_FILE = os.path.join(CACHE_DIR, f"{TICKER.replace('.', '-')}.csv")  # 緩存文件路徑（0700-HK.csv）
HONG_KONG_TZ = pytz.timezone('Asia/Hong_Kong')           # 香港時區
MAX_RETRIES = 3                                          # 下載重試次數
RETRY_DELAY = 5                                          # 重試間隔（秒）


# ------------------------------
# 函式定義：數據獲取與緩存（強化日誌與驗證）
# ------------------------------
def fetch_and_cache_data(ticker, start_date, end_date, cache_dir, cache_file, tz):
    try:
        os.makedirs(cache_dir, exist_ok=True)
        print(f"[DEBUG] 緩存目錄：{cache_dir}")
        print(f"[DEBUG] 緩存文件：{cache_file}")

        # 1. 嘗試讀取緩存
        if os.path.exists(cache_file):
            print(f"[INFO] 發現緩存文件，嘗試加載...")
            df = pd.read_csv(cache_file, parse_dates=["Date"], encoding='utf-8')
            
            # 驗證緩存有效性
            if df.empty:
                raise ValueError("緩存數據為空")
            required_cols = ["Date", "Open", "High", "Low", "Close"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"緩存缺少必要列：{missing_cols}")
            
            # 驗證時間範圍（緩存需覆蓋請求的時間段）
            min_cache_date = df["Date"].min().strftime('%Y-%m-%d')
            max_cache_date = df["Date"].max().strftime('%Y-%m-%d')
            if not (min_cache_date <= start_date and max_cache_date >= end_date):
                raise ValueError(f"緩存時間範圍不足（緩存：{min_cache_date}~{max_cache_date}，請求：{start_date}~{end_date}）")
            
            print(f"[SUCCESS] 緩存有效（{min_cache_date} ~ {max_cache_date}）")
            return df

        # 2. 下載數據（yfinance）
        for attempt in range(MAX_RETRIES):
            try:
                print(f"[INFO] 下載數據（第{attempt+1}/{MAX_RETRIES}次）...")
                df = yf.download(
                    tickers=ticker,
                    start=start_date,
                    end=end_date,
                    progress=False,
                    auto_adjust=True,
                    actions=False
                )
                
                if df.empty:
                    raise ValueError("Yahoo Finance 返回空數據")
                
                # 修復列名與索引
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.droplevel(1)
                df = df.reset_index().rename(columns={'Datetime': 'Date'})
                df.columns = df.columns.str.capitalize()  # 統一列名（首字母大寫）
                
                # 驗證必要列
                required_cols = ["Date", "Open", "High", "Low", "Close"]
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    raise ValueError(f"下載數據缺少列：{missing_cols}")
                
                # 時區轉換（UTC → 香港時區）
                df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize('UTC').dt.tz_convert(tz)
                
                # 保存緩存
                df.to_csv(cache_file, index=False, encoding='utf-8')
                print(f"[SUCCESS] 數據保存到緩存：{cache_file}")
                return df

            except Exception as e:
                print(f"[ERROR] 下載失敗（第{attempt+1}次）：{str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    raise RuntimeError(f"下載失敗（超過{MAX_RETRIES}次）") from e

    except Exception as e:
        # 保存錯誤日誌
        error_msg = f"[ERROR] 時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        error_msg += f"[ERROR] 函式：fetch_and_cache_data\n"
        error_msg += f"[ERROR] 錯誤：{str(e)}\n"
        error_msg += f"[ERROR] 堆疊：{traceback.format_exc()}"
        with open("error.log", "w", encoding='utf-8') as f:
            f.write(error_msg)
        raise


# ------------------------------
# 函式定義：數據預處理（強化驗證）
# ------------------------------
def preprocess_data(df):
    try:
        print("[INFO] 開始預處理數據...")
        df.columns = df.columns.str.capitalize()  # 確保列名統一

        # 1. 驗證必要列
        required_cols = ["Date", "Open", "High", "Low", "Close"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"數據缺少必要列：{missing_cols}（實際列：{df.columns.tolist()}）")

        # 2. 轉換日期與排序
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values(by="Date").reset_index(drop=True)
        print(f"[DEBUG] 預處理後數據量：{len(df)} 行")
        print(f"[DEBUG] 數據時間範圍：{df['Date'].min().strftime('%Y-%m-%d')} ~ {df['Date'].max().strftime('%Y-%m-%d')}")

        # 3. 處理缺失值
        initial_count = len(df)
        df = df.dropna(subset=required_cols)
        deleted_rows = initial_count - len(df)
        if deleted_rows > 0:
            print(f"[WARNING] 刪除 {deleted_rows} 行缺失值數據")

        # 4. 驗證數據量
        if len(df) < 2:
            raise ValueError("預處理後數據不足（少於2行，無法繪圖）")
        print("[SUCCESS] 預處理完成！")
        return df

    except Exception as e:
        # 保存錯誤日誌
        error_msg = f"[ERROR] 時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        error_msg += f"[ERROR] 函式：preprocess_data\n"
        error_msg += f"[ERROR] 錯誤：{str(e)}\n"
        error_msg += f"[ERROR] 堆疊：{traceback.format_exc()}"
        with open("error.log", "a", encoding='utf-8') as f:
            f.write(error_msg)
        raise


# ------------------------------
# 函式定義：繪製K線圖（輸出到根目錄）
# ------------------------------
def plot_ohlc_chart(df, ticker):
    try:
        print("[INFO] 開始繪製K線圖...")
        df["Date_Str"] = df["Date"].dt.strftime("%Y-%m-%d")

        # 生成K線圖
        fig = go.Figure(data=[go.Ohlc(
            x=df["Date_Str"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name=ticker,
            increasing_line_color='#ff0000',  # 紅漲
            decreasing_line_color='#00ff00'   # 綠跌
        )])

        # 設置圖表布局
        fig.update_layout(
            title={"text": f"{ticker} 歷史K線圖（香港時區）", "x": 0.5},
            xaxis_title="日期",
            yaxis_title="價格（港元）",
            xaxis_rangeslider_visible=False,  # 隱藏滑塊
            template="plotly_white",
            width=1200,  # 圖表寬度
            height=600   # 圖表高度
        )

        # 輸出到根目錄（與工作流一致）
        output_path = "./ohlc_chart.html"
        fig.write_html(output_path, include_plotlyjs="cdn", auto_open=True)
        print(f"[SUCCESS] 圖表生成：{output_path}")
        return output_path

    except Exception as e:
        # 保存錯誤日誌
        error_msg = f"[ERROR] 時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        error_msg += f"[ERROR] 函式：plot_ohlc_chart\n"
        error_msg += f"[ERROR] 錯誤：{str(e)}\n"
        error_msg += f"[ERROR] 堆疊：{traceback.format_exc()}"
        with open("error.log", "a", encoding='utf-8') as f:
            f.write(error_msg)
        raise


# ------------------------------
# 主程式入口（解析命令行參數）
# ------------------------------
if __name__ == "__main__":
    try:
        # 1. 解析命令行參數（支持debug_mode）
        parser = argparse.ArgumentParser(description="生成騰訊股票K線圖")
        parser.add_argument("--debug_mode", action="store_true", help="啟用詳細調試日誌")
        args = parser.parse_args()

        # 2. 初始化調試模式
        if args.debug_mode:
            print("="*50)
            print("🐞 偵錯模式已啟用 - 輸出詳細日誌")
            print("="*50)
            print(f"[DEBUG] 環境變量：TICKER={TICKER}, CACHE_DIR={CACHE_DIR}")
            print(f"[DEBUG] 時間範圍：{START_DATE} ~ {END_DATE}")
            print("="*50)

        # 3. 執行流程
        print("🚀 開始運行程式...")
        df = fetch_and_cache_data(TICKER, START_DATE, END_DATE, CACHE_DIR, CACHE_FILE, HONG_KONG_TZ)
        processed_df = preprocess_data(df)
        plot_ohlc_chart(processed_df, TICKER)

        print("🎉 程式執行成功！")
        exit(0)

    except Exception as e:
        print(f"❌ 程式執行失敗：{str(e)}")
        exit(1)
