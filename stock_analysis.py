# ------------------------------
# 導入所需套件（關鍵修正：確保 datetime 正確導入）
# ------------------------------
import pandas as pd
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime  # 必須保留此句，且無拼寫錯誤
import os

# ------------------------------
# 設定參數（使用 datetime 前已導入）
# ------------------------------
ticker = "0700.HK"
start_date = "2000-01-01"
end_date = datetime.today().strftime("%Y-%m-%d")  # 正確使用 datetime 類
cache_dir = "data"
cache_file = os.path.join(cache_dir, f"{ticker}.csv")

# ------------------------------
# 步驟 2：數據快取（添加錯誤處理）
# ------------------------------
try:
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir, exist_ok=True)  # 強制創建目錄
    if os.path.exists(cache_file):
        print(f"讀取快取數據：{cache_file}")
        df = pd.read_csv(cache_file, parse_dates=["Date"])
    else:
        print(f"下載數據中...（{ticker}，{start_date} 至 {end_date}）")
        # 下載數據時指定進度隱藏，並獲取完整的時間序列
        df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=True)
        if df.empty:
            raise ValueError("下載的數據為空，請檢查股票代碼或日期範圍！")
        df = df.reset_index()  # 將日期從索引轉為欄位
        # 保存時確保日期格式正確（避免不同系統的時區問題）
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df.to_csv(cache_file, index=False)
        print(f"數據已保存到：{cache_file}")
except Exception as e:
    print(f"數據下載/讀取失敗：{str(e)}")
    with open("error.log", "w") as f:
        f.write(str(e))
    raise  # 可選：若需要終止程序則保留，否則可替換為 sys.exit(1)

# ------------------------------
# 步驟 3：數據預處理（添加欄位檢查和排序）
# ------------------------------
required_columns = ["Date", "Open", "High", "Low", "Close"]
if not all(col in df.columns for col in required_columns):
    missing = [col for col in required_columns if col not in df.columns]
    raise ValueError(f"數據缺少必要欄位：{missing}")

# 確保數據按日期升序排列（避免繪圖時時間錯亂）
df = df.sort_values(by="Date").reset_index(drop=True)

# 檢查是否有缺失值（可選增強）
if df[required_columns].isnull().any().any():
    print("警告：數據中存在缺失值，可能影響繪圖效果！")

# ------------------------------
# 步驟 4：繪圖（輸出路徑修正）
# ------------------------------
# 創建OHLC圖表
fig = go.Figure(data=[go.Ohlc(
    x=df["Date"],
    open=df["Open"],
    high=df["High"],
    low=df["Low"],
    close=df["Close"],
    name=ticker,
    increasing_line_color='red',  # 自定義漲跌顏色（港股慣例紅漲綠跌）
    decreasing_line_color='green'
)])

# 設置圖表佈局
fig.update_layout(
    title=f"{ticker} 歷史K線圖",
    xaxis_title="日期",
    yaxis_title="價格 (HKD)",
    xaxis_rangeslider_visible=False,  # 隱藏範圍滑塊（適用於K線圖）
    template="plotly_white"  # 使用預設主題
)

# 輸出到HTML文件
output_path = "./ohlc_chart.html"
fig.write_html(output_path)
print(f"圖表已生成：{output_path}")






