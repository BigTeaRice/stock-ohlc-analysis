# ------------------------------
# 導入所需套件
# ------------------------------
import pandas as pd
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime
import os  # 用於檢查數據是否已快取

# ------------------------------
# 設定參數（可根據需求調整）
# ------------------------------
ticker = "0700.HK"       # 騰訊控股股票代碼
start_date = "1990-01-01"  # 若 yfinance 下載失敗，可指定更合理的起始日期
end_date = datetime.today().strftime("%Y-%m-%d")  # 當天日期
cache_dir = "data"       # 數據快取目錄
cache_file = os.path.join(cache_dir, f"{ticker}.csv")  # 快取檔案路徑

# ------------------------------
# 步驟 1：檢查數據是否已快取（避免重複下載）
# ------------------------------
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)  # 創建快取目錄（若不存在）

if os.path.exists(cache_file):
    print(f"讀取快取數據：{cache_file}")
    df = pd.read_csv(cache_file, parse_dates=["Date"])  # 從快取讀取（日期轉為 datetime）
else:
    # 步驟 2：下載數據（若快取不存在）
    print(f"下載數據中...（{ticker}，{start_date} 至 {end_date}）")
    try:
        # 下載歷史數據（包含 Open, High, Low, Close, Volume 等欄位）
        df = yf.download(ticker, start=start_date, end=end_date)
        df = df.reset_index()  # 將日期從索引轉為欄位
        df.to_csv(cache_file, index=False)  # 保存到快取
        print(f"數據已保存到：{cache_file}")
    except Exception as e:
        raise RuntimeError(f"數據下載失敗：{str(e)}")

# ------------------------------
# 步驟 3：數據預處理（確保格式正確）
# ------------------------------
# 檢查必要欄位是否存在（避免欄位名稱錯誤）
required_columns = ["Date", "Open", "High", "Low", "Close"]
if not all(col in df.columns for col in required_columns):
    missing = [col for col in required_columns if col not in df.columns]
    raise ValueError(f"數據缺少必要欄位：{missing}")

# 刪除空值（例如停牌日期無交易數據）
df = df.dropna(subset=required_columns)

# 確保日期欄位為 datetime 類型（避免繪圖時時間軸混亂）
df["Date"] = pd.to_datetime(df["Date"])

# ------------------------------
# 步驟 4：繪製互動式 OHLC 圖表
# ------------------------------
fig = go.Figure(
    data=go.Ohlc(
        x=df["Date"],
        open=df["Open"],
        high=df["High"],
        low=df["Low"],
        close=df["Close"],
        name=ticker,
        increasing_line_color="red",   # 上漲蠟燭顏色（港股慣用紅漲）
        decreasing_line_color="green"  # 下跌蠟燭顏色
    )
)

# 設定圖表佈局
fig.update_layout(
    title=f"{ticker} 歷史 OHLC 圖表（{start_date} 至 {end_date}）",
    xaxis_title="日期",
    yaxis_title="價格（港元）",
    xaxis_rangeslider_visible=True,  # 顯示時間軸滑桿（可縮放）
    hovermode="x unified",           # 懸停時顯示同一時間點所有數據
    template="plotly_white",         # 圖表風格
    legend_title="股票代號"
)

# 設定 x軸時間格式（自動適配範圍）
fig.update_xaxes(tickformat="%Y-%m-%d")

# ------------------------------
# 步驟 5：輸出圖表（HTML 文件，方便在 GitHub 展示）
# ------------------------------
output_path = "ohlc_chart.html"
fig.write_html(output_path)
print(f"圖表已保存到：{output_path}")

