# ------------------------------
# 導入所需套件
# ------------------------------
import pandas as pd
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime, timedelta
import os
import traceback
import time
import sys

# ------------------------------
# 全域參數設定
# ------------------------------
TICKER = "0700.HK"  # 騰訊港股代碼
START_DATE = "2004-06-16"  # 騰訊上市日期
CACHE_DIR = "stock_data"  # 緩存目錄
CACHE_FILE = os.path.join(CACHE_DIR, "0700_HK.csv")  # 緩存文件路徑
HTML_FILE = os.path.join(CACHE_DIR, "0700_HK_candlestick.html")  # HTML文件路徑
MAX_RETRIES = 5  # 最大重試次數
RETRY_DELAY = 3  # 重試間隔(秒)

# ------------------------------
# 函式：獲取並緩存股票數據
# ------------------------------
def fetch_and_cache_data():
    """下載股票數據並緩存到本地文件"""
    try:
        # 獲取當前日期（確保是工作日）
        end_date = datetime.now()
        # 如果是周末，使用上一個工作日
        if end_date.weekday() >= 5:  # 5=周六, 6=周日
            days_to_subtract = end_date.weekday() - 4
            end_date = end_date - timedelta(days=days_to_subtract)
        END_DATE = end_date.strftime("%Y-%m-%d")
        
        print(f"下載 {TICKER} 數據 ({START_DATE} 至 {END_DATE})...")
        
        # 創建緩存目錄
        os.makedirs(CACHE_DIR, exist_ok=True)
        
        # 從Yahoo Finance下載數據
        for attempt in range(MAX_RETRIES):
            try:
                df = yf.download(
                    TICKER, 
                    start=START_DATE, 
                    end=END_DATE,
                    progress=True,
                    auto_adjust=True,
                    threads=True
                )
                
                # 檢查數據是否有效
                if df.empty or len(df) < 10:
                    print(f"數據量不足或為空，嘗試 {attempt+1}/{MAX_RETRIES}")
                    time.sleep(RETRY_DELAY)
                    continue
                
                # 重置索引並重命名列
                df = df.reset_index()
                df.rename(columns={
                    'Date': 'Date',
                    'Open': 'Open',
                    'High': 'High',
                    'Low': 'Low',
                    'Close': 'Close',
                    'Volume': 'Volume'
                }, inplace=True)
                
                # 保存到緩存
                df.to_csv(CACHE_FILE, index=False, encoding='utf-8')
                print(f"數據已保存到 {CACHE_FILE}，共 {len(df)} 條記錄")
                return df
                
            except Exception as e:
                print(f"下載失敗 (嘗試 {attempt+1}/{MAX_RETRIES}): {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    print(f"{RETRY_DELAY}秒後重試...")
                    time.sleep(RETRY_DELAY)
                else:
                    raise RuntimeError(f"達到最大重試次數: {str(e)}")
    
    except Exception as e:
        error_msg = f"{datetime.now()}: 數據獲取失敗 - {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        # 嘗試加載歷史緩存數據
        if os.path.exists(CACHE_FILE):
            print("嘗試載入歷史緩存數據...")
            try:
                df = pd.read_csv(CACHE_FILE, parse_dates=['Date'])
                print(f"成功載入歷史緩存數據，共 {len(df)} 條記錄")
                return df
            except Exception as cache_error:
                print(f"載入緩存數據失敗: {str(cache_error)}")
        raise RuntimeError("無法獲取股票數據")

# ------------------------------
# 函式：數據預處理
# ------------------------------
def preprocess_data(df):
    """清洗和準備數據用於繪圖"""
    # 確保日期格式正確
    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        df['Date'] = pd.to_datetime(df['Date'])
    
    # 處理缺失值
    initial_count = len(df)
    df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)
    if len(df) < initial_count:
        print(f"移除 {initial_count - len(df)} 條包含缺失值的記錄")
    
    # 按日期排序
    df.sort_values('Date', inplace=True)
    
    # 檢查數據有效性
    if df['Close'].isnull().any() or df['Volume'].isnull().any():
        print("警告：數據中仍然存在空值")
    
    print(f"數據預處理完成，剩餘 {len(df)} 條有效記錄")
    return df

# ------------------------------
# 函式：繪製K線圖
# ------------------------------
def plot_ohlc_chart(df):
    """使用Plotly繪製互動式K線圖"""
    try:
        # 創建K線圖
        fig = go.Figure(data=[go.Candlestick(
            x=df['Date'],
            open=df['Open'],
            high=df['High'],
            low=df['Low'],
            close=df['Close'],
            name=TICKER,
            increasing_line_color='red',   # 港股上漲為紅色
            decreasing_line_color='green', # 港股下跌為綠色
        )])
        
        # 添加成交量圖
        fig.add_trace(go.Bar(
            x=df['Date'],
            y=df['Volume'],
            name="成交量",
            marker_color='rgba(100, 100, 100, 0.3)',
            yaxis="y2"
        ))
        
        # 獲取最新日期
        latest_date = df['Date'].max().strftime("%Y-%m-%d")
        
        # 設置圖表佈局
        fig.update_layout(
            title=f'{TICKER} 歷史K線圖 ({START_DATE} 至 {latest_date})',
            title_x=0.5,
            title_font=dict(size=20, color='darkblue'),
            xaxis_title='日期',
            yaxis_title='股價 (HKD)',
            template='plotly_white',
            hovermode='x unified',
            height=800,
            showlegend=True,
            # 添加右側y軸用於成交量
            yaxis2=dict(
                title="成交量",
                overlaying="y",
                side="right",
                showgrid=False
            ),
            xaxis_rangeslider_visible=False
        )
        
        # 更新x軸設置
        fig.update_xaxes(
            rangeslider_visible=False,
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1月", step="month", stepmode="backward"),
                    dict(count=6, label="6月", step="month", stepmode="backward"),
                    dict(count=1, label="1年", step="year", stepmode="backward"),
                    dict(count=5, label="5年", step="year", stepmode="backward"),
                    dict(step="all", label="全部")
                ])
            )
        )
        
        # 保存HTML文件
        fig.write_html(
            HTML_FILE, 
            auto_open=False,
            include_plotlyjs='cdn',
            full_html=True
        )
        
        print(f"圖表已保存為: {HTML_FILE}")
        print(f"文件大小: {os.path.getsize(HTML_FILE) / 1024 / 1024:.2f} MB")
        
        return HTML_FILE
        
    except Exception as e:
        error_msg = f"{datetime.now()}: 繪圖失敗 - {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        raise

# ------------------------------
# 主程序
# ------------------------------
if __name__ == "__main__":
    print("=" * 60)
    print(f"騰訊控股(0700.HK)歷史K線圖生成器")
    print(f"開始時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # 獲取數據
        df = fetch_and_cache_data()
        
        # 預處理數據
        df = preprocess_data(df)
        
        # 繪製圖表
        print("生成互動式K線圖...")
        chart_file = plot_ohlc_chart(df)
        
        # 輸出成功信息
        print("=" * 60)
        print("程式執行成功！")
        print(f"最終數據記錄數: {len(df)}")
        print(f"數據時間範圍: {df['Date'].min().strftime('%Y-%m-%d')} 至 {df['Date'].max().strftime('%Y-%m-%d')}")
        print(f"圖表文件: {os.path.abspath(chart_file)}")
        print("=" * 60)
        
    except Exception as e:
        print(f"程式執行失敗: {str(e)}")
        print(traceback.format_exc())
        sys.exit(1)
