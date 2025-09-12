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
import time  # 重試機制用

# ------------------------------
# 全域參數設定（關鍵修正：港股代碼格式）
# ------------------------------
TICKER = "0700.HK"  # 修正：港股正確代碼（去掉前導零）
START_DATE = "2004-06-16"  # 腾訊上市日期（2004-06-16）
END_DATE = datetime.today().strftime("%Y-%m-%d")  # 自動取當前日期
CACHE_DIR = "data"  # 緩存目錄名
CACHE_FILE = os.path.join(CACHE_DIR, f"{TICKER.replace('.', '-')}.csv")  # 緩存文件名：0700-HK.csv
HONG_KONG_TZ = pytz.timezone('Asia/Hong_Kong')  # 香港時區
MAX_RETRIES = 3  # 數據下載最大重試次數
RETRY_DELAY = 5  # 重試間隔（秒）

# ------------------------------
# 函式定義：數據獲取與緩存（含重試機制、緩存驗證）
# ------------------------------
def fetch_and_cache_data(ticker, start_date, end_date, cache_dir, cache_file, tz):
    """
    下載並緩存股票數據，自動重試，處理時區和緩存有效性。
    """
    try:
        # 創建緩存目錄（若不存在）
        os.makedirs(cache_dir, exist_ok=True)

        # 讀取有效緩存（若存在且符合時間範圍）
        if os.path.exists(cache_file):
            print(f"嘗試讀取緩存數據：{cache_file}")
            try:
                # 讀取緩存並解析日期列
                df = pd.read_csv(cache_file, parse_dates=["Date"], encoding='utf-8')
                if df.empty:
                    raise ValueError("緩存數據為空，觸發重新下載")
                
                # 驗證緩存時間範圍是否匹配當前請求
                min_cache_date = df["Date"].min()
                max_cache_date = df["Date"].max()
                if (pd.to_datetime(min_cache_date) >= pd.to_datetime(start_date) and 
                    pd.to_datetime(max_cache_date) <= pd.to_datetime(end_date)):
                    print(f"緩存有效（時間範圍：{min_cache_date.strftime('%Y-%m-%d')} 至 {max_cache_date.strftime('%Y-%m-%d')})")
                    return df
                else:
                    print("緩存時間範圍不匹配，觸發重新下載")
            except Exception as e:
                print(f"緩存讀取失敗（可能損壞）：{str(e)}，觸發重新下載")

        # 下載數據（含重試機制）
        for attempt in range(MAX_RETRIES):
            try:
                print(f"下載數據中...（第 {attempt+1}/{MAX_RETRIES} 次嘗試，{ticker}，{start_date} 至 {end_date}）")
                # 使用 yfinance 下載數據
                df = yf.download(
                    tickers=ticker,
                    start=start_date,
                    end=end_date,
                    progress=False,  # 關閉進度條
                    auto_adjust=True,  # 自動調整分紅拆股影響
                    actions=False  # 不包含股息/拆股事件（避免干擾價格數據）
                )

                # 檢查數據是否為空
                if df.empty:
                    raise ValueError(f"Yahoo Finance 無回應數據（可能代碼錯誤或時間範圍無效）")
                
                # 時區校正（Yahoo 返回 UTC 時間，轉換為香港時間）
                df.index = df.index.tz_localize('UTC').tz_convert(tz)
                df = df.reset_index()  # 將時間索引轉為數據列（方便保存）
                
                # 保存緩存（UTF-8 編碼，避免亂碼）
                df.to_csv(cache_file, index=False, encoding='utf-8')
                print(f"數據成功保存到緩存：{cache_file}")
                return df

            except Exception as e:
                print(f"下載失敗（第 {attempt+1} 次）：{str(e)}")
                if attempt < MAX_RETRIES - 1:
                    print(f"等待 {RETRY_DELAY} 秒後重試...")
                    time.sleep(RETRY_DELAY)  # 等待後重試
                else:
                    # 超過最大重試次數，記錄錯誤並拋出異常
                    error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    error_msg = (
                        f"時間：{error_time}\n"
                        f"函式：fetch_and_cache_data\n"
                        f"錯誤信息：{str(e)}\n"
                        f"堆疊跟踪：\n{traceback.format_exc()}"
                    )
                    with open("error.log", "w", encoding='utf-8') as f:
                        f.write(error_msg)
                    raise RuntimeError(f"數據下載失敗（超過 {MAX_RETRIES} 次嘗試）") from e

    except Exception as e:
        # 捕獲未預期的異常，記錄錯誤日誌
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_msg = (
            f"時間：{error_time}\n"
            f"函式：fetch_and_cache_data\n"
            f"錯誤信息：{str(e)}\n"
            f"堆疊跟踪：\n{traceback.format_exc()}"
        )
        with open("error.log", "w", encoding='utf-8') as f:
            f.write(error_msg)
        raise

# ------------------------------
# 函式定義：數據預處理（驗證、清洗、時區轉換）
# ------------------------------
def preprocess_data(df):
    """
    驗證數據完整性，清洗缺失值，確保時間序列正確。
    """
    try:
        # 檢查必要欄位是否存在（yfinance 下載的默認欄位）
        required_columns = ["Date", "Open", "High", "Low", "Close"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"數據缺少必要欄位：{missing_columns}（可能下載失敗）")

        # 轉換日期欄位類型（確保為 datetime 並應用香港時區）
        # 注意：緩存中的 Date 列已存儲為字符串，需重新解析
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert(HONG_KONG_TZ)

        # 按日期升序排列（確保時間序列正確）
        df = df.sort_values(by="Date").reset_index(drop=True)

        # 處理缺失值（刪除關鍵欄位缺失的行）
        initial_row_count = len(df)
        df = df.dropna(subset=required_columns)
        deleted_rows = initial_row_count - len(df)
        if deleted_rows > 0:
            print(f"警告：刪除 {deleted_rows} 行含缺失值的數據（原因：網絡或數據源異常）")

        # 驗證數據量（至少需要 2 行才能繪圖）
        if len(df) < 2:
            raise ValueError("預處理後數據不足（少於 2 行），無法繪製圖表")

        return df

    except Exception as e:
        # 記錄錯誤日誌（追加模式，避免覆蓋之前的錯誤）
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_msg = (
            f"時間：{error_time}\n"
            f"函式：preprocess_data\n"
            f"錯誤信息：{str(e)}\n"
            f"堆疊跟踪：\n{traceback.format_exc()}"
        )
        with open("error.log", "a", encoding='utf-8') as f:
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
        # 轉換日期為字符串（僅顯示日期部分，避免 Plotly 自動添加時間）
        df["Date_Str"] = df["Date"].dt.strftime("%Y-%m-%d")

        # 創建K線圖（Ohlc 類型）
        fig = go.Figure(data=[go.Ohlc(
            x=df["Date_Str"],  # 使用字符串避免時間軸自動偏移
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name=ticker,
            increasing_line_color='#ff0000',  # 港股紅漲（十六進制代碼）
            decreasing_line_color='#00ff00',  # 港股綠跌
            line=dict(width=1)  # 線寬微調
        )])

        # 設置圖表佈局（港股風格）
        fig.update_layout(
            title={
                "text": f"{ticker} 歷史K線圖（香港時間）",
                "x": 0.5,  # 水平居中
                "xanchor": "center",  # 居中錨點
                "font": {"size": 18}  # 標題字體大小
            },
            xaxis_title="日期",
            yaxis_title="價格 (HKD)",
            xaxis_rangeslider_visible=False,  # 隱藏範圍滑塊（更清晰）
            template="plotly_white",  # 簡潔主題
            hovermode="x unified",  # 懸浮提示顯示所有欄位
            margin=dict(l=40, r=20, t=50, b=100)  # 調整邊距避免標籤被截斷
        )

        # 設置X軸日期格式（自動適配範圍，最多顯示10個刻度）
        fig.update_xaxes(
            tickformat="%Y-%m-%d",  # 日期顯示格式
            nticks=10,  # 最多顯示10個刻度
            showgrid=True,  # 顯示網格
            gridcolor="#f0f0f0"  # 網格顏色
        )

        # 設置Y軸格式（保留2位小數）
        fig.update_yaxes(
            tickformat=".2f",  # 保留2位小數
            showgrid=True,  # 顯示網格
            gridcolor="#f0f0f0"  # 網格顏色
        )

        # 輸出到HTML（自動打開瀏覽器）
        output_path = "./ohlc_chart.html"
        fig.write_html(output_path, include_plotlyjs="cdn", auto_open=True)  # 使用CDN加速
        print(f"圖表成功生成並打開：{output_path}")

        return output_path

    except Exception as e:
        # 記錄錯誤日誌
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_msg = (
            f"時間：{error_time}\n"
            f"函式：plot_ohlc_chart\n"
            f"錯誤信息：{str(e)}\n"
            f"堆疊跟踪：\n{traceback.format_exc()}"
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

        # 步驟2：預處理數據（驗證、清洗、時區轉換）
        processed_df = preprocess_data(df)

        # 步驟3：繪製互動式K線圖
        plot_ohlc_chart(processed_df, TICKER)

    except Exception as e:
        # 主程式異常處理
        print(f"主程式執行失敗：{str(e)}")
        exit(1)  # 非零退出碼表示異常（便於腳本集成）
