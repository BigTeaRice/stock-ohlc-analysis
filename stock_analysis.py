# ------------------------------
# 步驟 1：參數設定
# ------------------------------
ticker = "0700.HK"       # 確認代碼正確
start_date = "2000-01-01"  # 縮短起始日期避免下載失敗
end_date = datetime.today().strftime("%Y-%m-%d")
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
        df = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if df.empty:
            raise ValueError("下載的數據為空，請檢查股票代碼或日期範圍！")
        df = df.reset_index()
        df.to_csv(cache_file, index=False)
        print(f"數據已保存到：{cache_file}")
except Exception as e:
    print(f"數據下載/讀取失敗：{str(e)}")
    with open("error.log", "w") as f:
        f.write(str(e))
    raise

# ------------------------------
# 步驟 3：數據預處理（添加欄位檢查）
# ------------------------------
required_columns = ["Date", "Open", "High", "Low", "Close"]
if not all(col in df.columns for col in required_columns):
    missing = [col for col in required_columns if col not in df.columns]
    raise ValueError(f"數據缺少必要欄位：{missing}")

# ------------------------------
# 步驟 4：繪圖（輸出路徑修正）
# ------------------------------
fig.write_html("./ohlc_chart.html")  # 輸出到根目錄
print("圖表已保存到：./ohlc_chart.html")


