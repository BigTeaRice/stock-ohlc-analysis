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
# 全域參數設定（關鍵修正：港股代碼格式）
# ------------------------------
TICKER = "0700.HK"  # 修正：港股正確代碼（去掉前導零）
START_DATE = "2004-06-16"  # 腾訊上市日期
END_DATE = datetime.today().strftime("%Y-%m-%d")  # 當前日期
CACHE_DIR = "data"
CACHE_FILE = os.path.join(CACHE_DIR, f"{TICKER.replace('.', '-')}.csv")  # 緩存文件名：0700-HK.csv
HONG_KONG_TZ = pytz.timezone('Asia/Hong_Kong')
MAX_RETRIES = 3  # 最大重試次數
RETRY_DELAY = 5  # 重試間隔（秒）

# ------------------------------
# 函式定義：數據獲取與緩存（含重試、緩存驗證）
# ------------------------------
def fetch_and_cache_data(ticker, start_date, end_date, cache_dir, cache_file, tz):
    """
    下載並緩存股票數據，自動重試，驗證緩存有效性。
    """
    try:
        # 創建緩存目錄（若不存在）
        os.makedirs(cache_dir, exist_ok=True)

        # 讀取有效緩存（若存在）
        if os.path.exists(cache_file):
            print(f"嘗試讀取緩存數據：{cache_file}")
            try:
                df = pd.read_csv(cache_file, parse_dates=["Date"], encoding='utf-8')
                if df.empty:
                    raise ValueError("緩存數據為空，觸發重新下載")
                
                # 驗證緩存時間範圍是否匹配
                min_cache_date = df["Date"].min()
                max_cache_date = df["Date"].max()
                if (pd.to_datetime(min_cache_date) >= pd.to_datetime(start_date) and 
                    pd.to_datetime(max_cache_date) <= pd.to_datetime(end_date)):
                    print(f"緩存有效（時間範圍：{min_cache_date} 至 {max_cache_date}）")
                    return df
                else:
                    print("緩存時間範圍不匹配，觸發重新下載")
            except Exception as e:
                print(f"緩存讀取失敗（可能損壞）：{str(e)}，觸發重新下載")

        # 下載數據（含重試機制）
        for attempt in range(MAX_RETRIES):
            try:
                print(f"下載數據中...（第 {attempt+1}/{MAX_RETRIES} 次，{ticker}，{start_date} 至 {end_date}）")
                df = yf.download(
                    tickers=ticker,
                    start=start_date,
                    end=end_date,
                    progress=False,
                    auto_adjust=True,  # 自動調整分紅拆股
                    actions=False      # 不包含股息/拆股事件
                )

                if df.empty:
                    raise ValueError(f"Yahoo Finance 無數據（代碼/時間錯誤）")
                
                # 時區校正（UTC → 香港時間）
                df.index = df.index.tz_localize('UTC').tz_convert(tz)
                df = df.reset_index()  # 轉換索引為日期列
                
                # 保存緩存（UTF-8 編碼，避免亂碼）
                df.to_csv(cache_file, index=False, encoding='utf-8')
                print(f"數據保存到緩存：{cache_file}")
                return df

            except Exception as e:
                print(f"下載失敗（第 {attempt+1} 次）：{str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)  # 等待後重試
                else:
                    raise RuntimeError(f"下載失敗（超過 {MAX_RETRIES} 次嘗試）") from e

    except Exception as e:
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_msg = (
            f"時間：{error_time}
"
            f"函式：fetch_and_cache_data
"
            f"錯誤信息：{str(e)}
"
            f"堆疊跟踪：
{traceback.format_exc()}"
        )
        # 寫入錯誤日誌（確保字符串閉合）
        with open("error.log", "w", encoding='utf-8') as f:
            f.write(error_msg)  # 一次性寫入完整字符串，避免換行問題
        raise

# ------------------------------
# 函式定義：數據預處理（驗證、清洗、時區轉換）
# ------------------------------
def preprocess_data(df):
    """
    驗證數據完整性，清洗缺失值，確保時間序列正確。
    """
    try:
        # 檢查必要欄位
        required_columns = ["Date", "Open", "High", "Low", "Close"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"缺少必要欄位：{missing_columns}")

        # 轉換日期欄位（確保為 datetime 並應用香港時區）
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert(HONG_KONG_TZ)

        # 按日期升序排列
        df = df.sort_values(by="Date").reset_index(drop=True)

        # 處理缺失值（刪除關鍵欄位缺失的行）
        initial_count = len(df)
        df = df.dropna(subset=required_columns)
        deleted_rows = initial_count - len(df)
        if deleted_rows > 0:
            print(f"警告：刪除 {deleted_rows} 行含缺失值的數據")

        # 驗證數據量（至少 2 行才能繪圖）
        if len(df) < 2:
            raise ValueError("預處理後數據不足（少於 2 行）")

        return df

    except Exception as e:
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_msg = (
            f"時間：{error_time}
"
            f"函式：preprocess_data
"
            f"錯誤信息：{str(e)}
"
            f"堆疊跟踪：
{traceback.format_exc()}"
        )
        with open("error.log", "a", encoding='utf-8') as f:  # 追加模式（避免覆蓋）
            f.write(error_msg)
        raise

# ------------------------------
# 函式定義：繪製互動式OHLC圖表（港股風格）
# ------------------------------
def plot_ohlc_chart(df, ticker):
    """
    使用 Plotly 繪製互動式K線圖（紅漲綠跌，香港時間軸）。
    """
    try:
        # 轉換日期為字符串（避免 Plotly 自動添加時間）
        df["Date_Str"] = df["Date"].dt.strftime("%Y-%m-%d")

        # 創建K線圖
        fig = go.Figure(data=[go.Ohlc(
            x=df["Date_Str"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name=ticker,
            increasing_line_color='#ff0000',  # 紅漲
            decreasing_line_color='#00ff00',  # 綠跌
            line=dict(width=1)
        )])

        # 設置圖表佈局
        fig.update_layout(
            title={
                "text": f"{ticker} 歷史K線圖（香港時間）",
                "x": 0.5,
                "xanchor": "center",
                "font": {"size": 18}
            },
            xaxis_title="日期",
            yaxis_title="價格 (HKD)",
            xaxis_rangeslider_visible=False,  # 隱藏範圍滑塊
            template="plotly_white",
            hovermode="x unified",
            margin=dict(l=40, r=20, t=50, b=100)  # 調整邊距
        )

        # 設置X軸格式
        fig.update_xaxes(
            tickformat="%Y-%m-%d",
            nticks=10,  # 最多顯示10個刻度
            showgrid=True,
            gridcolor="#f0f0f0"
        )

        # 設置Y軸格式
        fig.update_yaxes(
            tickformat=".2f",
            showgrid=True,
            gridcolor="#f0f0f0"
        )

        # 輸出到HTML（自動打開）
        output_path = "./ohlc_chart.html"
        fig.write_html(output_path, include_plotlyjs="cdn", auto_open=True)
        print(f"圖表生成並打開：{output_path}")

        return output_path

    except Exception as e:
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_msg = (
            f"時間：{error_time}
"
            f"函式：plot_ohlc_chart
"
            f"錯誤信息：{str(e)}
"
            f"堆疊跟踪：
{traceback.format_exc()}"
        )
        with open("error.log", "a", encoding='utf-8') as f:
            f.write(error_msg)
        raise

# ------------------------------
# 主程式入口（完整流程控制）
# ------------------------------
if __name__ == "__main__":
    try:
        # 步驟1：獲取/緩存數據
        df = fetch_and_cache_data(
            ticker=TICKER,
            start_date=START_DATE,
            end_date=END_DATE,
            cache_dir=CACHE_DIR,
            cache_file=CACHE_FILE,
            tz=HONG_KONG_TZ
        )

        # 步驟2：預處理數據
        processed_df = preprocess_data(df)

        # 步驟3：繪製圖表
        plot_ohlc_chart(processed_df, TICKER)

    except Exception as e:
        print(f"主程式執行失敗：{str(e)}")
        exit(1)  # 非零退出碼表示異常
