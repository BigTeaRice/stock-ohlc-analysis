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
TICKER = "0700.HK"  # 騰訊港股代碼
START_DATE = "2004-06-16"  # 騰訊上市日期
END_DATE = datetime.today().strftime("%Y-%m-%d")  # 當前日期
CACHE_DIR = "stock_data"  # 緩存目錄
CACHE_FILE = os.path.join(CACHE_DIR, f"{TICKER.replace('.', '_')}.csv")  # 緩存文件路徑
HONG_KONG_TZ = pytz.timezone('Asia/Hong_Kong')  # 香港時區
MAX_RETRIES = 5  # 最大重試次數
RETRY_DELAY = 3  # 重試間隔(秒)

# ------------------------------
# 函式：獲取並緩存股票數據
# ------------------------------
def fetch_and_cache_data():
    """下載股票數據並緩存到本地文件"""
    try:
        # 創建緩存目錄
        os.makedirs(CACHE_DIR, exist_ok=True)
        
        # 嘗試讀取緩存
        if os.path.exists(CACHE_FILE):
            cache_date = datetime.fromtimestamp(os.path.getmtime(CACHE_FILE))
            today = datetime.now()
            if cache_date.date() == today.date():
                print("使用今日最新緩存數據")
                return pd.read_csv(CACHE_FILE, parse_dates=['Date'])
        
        # 從Yahoo Finance下載數據
        print(f"下載 {TICKER} 數據 ({START_DATE} 至 {END_DATE})...")
        for attempt in range(MAX_RETRIES):
            try:
                df = yf.download(
                    TICKER, 
                    start=START_DATE, 
                    end=END_DATE,
                    progress=False,
                    auto_adjust=True
                )
                
                # 檢查數據是否有效
                if df.empty:
                    raise ValueError("下載數據為空")
                
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
                df.to_csv(CACHE_FILE, index=False)
                print(f"數據已保存到 {CACHE_FILE}")
                return df
                
            except Exception as e:
                print(f"下載失敗 (嘗試 {attempt+1}/{MAX_RETRIES}): {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    print(f"{RETRY_DELAY}秒後重試...")
                    time.sleep(RETRY_DELAY)
                else:
                    print("達到最大重試次數，使用備用數據源")
                    # 嘗試從緩存載入舊數據（如果存在）
                    if os.path.exists(CACHE_FILE):
                        print("載入歷史緩存數據")
                        return pd.read_csv(CACHE_FILE, parse_dates=['Date'])
                    else:
                        raise RuntimeError("無法獲取股票數據")
    
    except Exception as e:
        error_msg = f"{datetime.now()}: 數據獲取失敗 - {str(e)}\n{traceback.format_exc()}"
        with open("stock_error.log", "a") as f:
            f.write(error_msg + "\n\n")
        raise

# ------------------------------
# 函式：數據預處理
# ------------------------------
def preprocess_data(df):
    """清洗和準備數據用於繪圖"""
    # 確保日期格式正確
    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        df['Date'] = pd.to_datetime(df['Date'])
    
    # 轉換為香港時區
    df['Date'] = df['Date'].dt.tz_localize('UTC').dt.tz_convert(HONG_KONG_TZ)
    
    # 處理缺失值
    df.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)
    
    # 按日期排序
    df.sort_values('Date', inplace=True)
    
    # 添加年月標記用於縮放
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month_name()
    
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
        
        # 計算移動平均線
        for period in [20, 50, 200]:
            if len(df) > period:
                ma = df['Close'].rolling(window=period).mean()
                fig.add_trace(go.Scatter(
                    x=df['Date'],
                    y=ma,
                    name=f'{period}日均線',
                    line=dict(width=1.5),
                    visible='legendonly'  # 預設隱藏
                ))
        
        # 設置圖表佈局
        fig.update_layout(
            title=f'{TICKER} 歷史K線圖 ({START_DATE} 至 {END_DATE})',
            title_x=0.5,
            title_font=dict(size=24, color='darkblue'),
            xaxis_title='日期',
            yaxis_title='股價 (HKD)',
            template='plotly_white',
            hovermode='x unified',
            height=800,
            # 添加範圍選擇器
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1, label="1個月", step="month", stepmode="backward"),
                        dict(count=6, label="6個月", step="month", stepmode="backward"),
                        dict(count=1, label="今年", step="year", stepmode="todate"),
                        dict(count=1, label="1年", step="year", stepmode="backward"),
                       "),
                        dict(step="all", label="全部數據")
                    ]),
                    bgcolor='lightblue',
                    font=dict(color='darkblue')
                ),
                rangeslider=dict(visible=True, thickness=0.05),
                type="date"
            ),
            # 添加成交量圖
            yaxis2=dict(
                title="成交量",
                overlaying="y",
                side="right",
                showgrid=False
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        # 添加成交量柱狀圖
        fig.add_trace(go.Bar(
            x=df['Date'],
            y=df['Volume'],
            name='成交量',
            marker_color='rgba(100, 100, 100, 0.5)',
            yaxis="y2"
        ))
        
        # 添加最新價格標記
        last_close = df.iloc[-1]['Close']
        fig.add_annotation(
            x=df.iloc[-1]['Date'],
            y=last_close,
            text=f'最新: {last_close:.2f}',
            showarrow=True,
            arrowhead=1,
            ax=-40,
            ay=-30,
            bgcolor="white",
            bordercolor="black"
        )
        
        # 保存並顯示圖表
        html_file = f"{TICKER.replace('.', '_')}_candlestick.html"
        fig.write_html(html_file, auto_open=True)
        print(f"圖表已保存為: {html_file}")
        
        return html_file
        
    except Exception as e:
        error_msg = f"{datetime.now()}: 繪圖失敗 - {str(e)}\n{traceback.format_exc()}"
        with open("plot_error.log", "a") as f:
            f.write(error_msg + "\n\n")
        raise

# ------------------------------
# 主程序
# ------------------------------
if __name__ == "__main__":
    print("=" * 50)
    print(f"騰訊控股(0700.HK)歷史K線圖生成器")
    print(f"日期範圍: {START_DATE} 至 {END_DATE}")
    print("=" * 50)
    
    try:
        # 獲取數據
        df = fetch_and_cache_data()
        
        # 預處理數據
        df = preprocess_data(df)
        print(f"數據預處理完成，共 {len(df)} 條記錄")
        
        # 繪製圖表
        print("生成互動式K線圖...")
        chart_file = plot_ohlc_chart(df)
        
        print("=" * 50)
        print("程式執行成功！")
        print(f"圖表已保存為: {os.path.abspath(chart_file)}")
        
    except Exception as e:
        print(f"程式執行失敗: {str(e)}")
        print("請查看錯誤日誌獲取詳細信息")
