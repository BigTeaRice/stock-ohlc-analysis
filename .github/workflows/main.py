# ------------------------------
# 導入所需套件（關鍵修正：確保時區和錯誤處理庫已導入）
# ------------------------------
import pandas as pd
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime
import os
import traceback  # 用於記錄完整錯誤堆疊
import pytz  # 用於時區轉換

# ------------------------------
# 全域參數設定（可根據需求調整）
# ------------------------------
TICKER = "0700.HK"  # 股票代碼（港股需帶.HK）
START_DATE = "2000-01-01"  # 數據起始日期
END_DATE = datetime.today().strftime("%Y-%m-%d")  # 自動獲取今日日期
CACHE_DIR = "data"  # 緩存目錄名稱
CACHE_FILE = os.path.join(CACHE_DIR, f"{TICKER.replace('.', '-')}.csv")  # 緩存文件路徑（避免特殊符號）
HONG_KONG_TZ = pytz.timezone('Asia/Hong_Kong')  # 香港時區

# ------------------------------
# 函式定義：數據獲取與緩存
# ------------------------------
def fetch_and_cache_data(ticker, start_date, end_date, cache_dir, cache_file, tz):
    """
    下載或讀取緩存的股票數據，並進行時區校正。
    返回處理後的 DataFrame。
    """
    try:
        # 創建緩存目錄（若不存在）
        os.makedirs(cache_dir, exist_ok=True)

        # 讀取緩存數據（若存在）
        if os.path.exists(cache_file):
            print(f"讀取緩存數據：{cache_file}")
            df = pd.read_csv(cache_file, parse_dates=["Date"])
            
            # 檢查緩存數據是否為空
            if df.empty:
                raise ValueError("緩存數據為空，請刪除舊緩存文件後重試！")
            
            # 驗證時區是否已校正（避免舊緩存未轉換時區）
            if "Timezone" not in df.columns or df["Timezone"].iloc[0] != tz.zone:
                print("檢測到舊緩存未轉換時區，重新下載數據...")
                return download_and_process_data(ticker, start_date, end_date, cache_dir, cache_file, tz)
            
            return df

        # 下載新數據（若緩存不存在）
        else:
            print(f"下載數據中...（{ticker}，{start_date} 至 {end_date}）")
            df = yf.download(
                tickers=ticker,
                start=start_date,
                end=end_date,
                progress=False,  # 隱藏進度條（適用於自動化環境）
                auto_adjust=True  # 自動復權（前復權）
            )

            # 檢查數據是否為空（如股票代碼錯誤或日期範圍無效）
            if df.empty:
                raise ValueError(f"下載失敗：{ticker} 在 {start_date} 至 {end_date} 無數據！")

            # 時區校正（關鍵步驟）
            df.index = df.index.tz_localize('UTC').tz_convert(tz)  # UTC → 香港時區
            df = df.reset_index()  # 將日期索引轉為欄位

            # 保存必要欄位（避免 yfinance 返回多餘欄位）
            required_columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
            df = df[required_columns]

            # 新增時區標記欄位（用於後續緩存驗證）
            df["Timezone"] = tz.zone

            # 保存到 CSV（日期格式轉為字符串，避免時區問題）
            df["Date"] = df["Date"].dt.strftime("%Y-%m-%d %H:%M:%S")
            df.to_csv(cache_file, index=False)
            print(f"數據已保存到緩存：{cache_file}")
            return df

    except Exception as e:
        print(f"數據獲取/緩存失敗：{str(e)}")
        with open("error.log", "w") as f:
            f.write(traceback.format_exc())  # 記錄完整堆棧軌跡
        raise  # 終止程序以避免錯誤數據繼續處理

# ------------------------------
# 函式定義：數據預處理
# ------------------------------
def preprocess_data(df):
    """
    驗證數據完整性，處理缺失值，確保數據按時間排序。
    """
    try:
        # 檢查必要欄位是否存在
        required_columns = ["Date", "Open", "High", "Low", "Close"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"數據缺少必要欄位：{missing_columns}")

        # 轉換日期欄位類型（確保為 datetime）
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert(HONG_KONG_TZ)

        # 按日期升序排列（確保時間序列正確）
        df = df.sort_values(by="Date").reset_index(drop=True)

        # 處理缺失值（刪除含有缺失值的行）
        initial_row_count = len(df)
        df = df.dropna(subset=required_columns)
        deleted_rows = initial_row_count - len(df)
        if deleted_rows > 0:
            print(f"警告：刪除 {deleted_rows} 行含缺失值的數據。")

        return df

    except Exception as e:
        print(f"數據預處理失敗：{str(e)}")
        with open("error.log", "a") as f:  # 追加模式（避免覆蓋之前的錯誤）
            f.write(traceback.format_exc())
        raise

# ------------------------------
# 函式定義：繪製OHLC圖表
# ------------------------------
def plot_ohlc_chart(df, ticker):
    """
    使用 Plotly 繪製OHLCK線圖，輸出為 HTML 文件。
    """
    try:
        fig = go.Figure(data=[go.Ohlc(
            x=df["Date"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name=ticker,
            increasing_line_color='red',  # 港股紅漲
            decreasing_line_color='green'# 港股綠跌
        )])

        # 設置圖表佈局
        fig.update_layout(
            title={
                "text": f"{ticker} 歷史K線圖（香港時間）",
                "x": 0.5,
                "xanchor": "center"
            },
            xaxis_title="日期",
            yaxis_title="價格 (HKD)",
            xaxis_rangeslider_visible=False,  # 隱藏範圍滑塊
            template="plotly_white",  # 主題樣式
            xaxis=dict(
                tickformat="%Y-%m-%d",  # 日期顯示格式（僅日期）
                rangeslider=dict(visible=False)  # 重複隱藏滑塊（確保兼容）
            ),
            hovermode="x unified"  # 懸浮提示顯示所有欄位
        )

        # 輸出到 HTML
        output_path = "./ohlc_chart.html"
        fig.write_html(output_path, include_plotlyjs="cdn")  # 使用CDN加速加載
        print(f"圖表已生成：{output_path}")
        return output_path

    except Exception as e:
        print(f"繪圖失敗：{str(e)}")
        with open("error.log", "a") as f:
            f.write(traceback.format_exc())
        raise

# ------------------------------
# 主程式入口
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
        # 錯誤已記錄在 error.log，此處可選擇退出碼（非零表示異常）
        exit(1)
