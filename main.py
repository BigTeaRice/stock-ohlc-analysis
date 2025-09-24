# ------------------------------
# 導入所需套件
# ------------------------------
import pandas as pd
import mplfinance as mpf
from pathlib import Path
from datetime import datetime
import sys

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    数据预处理函数（统一列名、去重、去缺失、计算指标）
    
    参数:
        df (pd.DataFrame): 原始数据（需包含Date/Open/High/Low/Close/Volume列）
    返回:
        pd.DataFrame: 处理后的数据
    """
    try:
        # -------------------- 步骤1：统一列名（解决大小写/空格问题） --------------------
        # 定义目标列名（代码中使用的标准列名）
        target_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        # 将原始列名转为小写，匹配目标列名（比如'open'→'Open'，'High '→'High'）
        col_mapping = {col.strip().lower(): target_col for target_col in target_cols 
                       for col in df.columns if col.strip().lower() == target_col}
        # 仅保留目标列（避免无关列干扰）
        df = df.rename(columns=col_mapping)[target_cols]
        
        # -------------------- 步骤2：检查必要列是否存在 --------------------
        missing_cols = [col for col in target_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"数据缺失必要列：{missing_cols}，请检查CSV文件列名！")
        
        # -------------------- 步骤3：数据清洗 --------------------
        # 去除重复索引（同一日期多条数据，保留最后一条）
        df = df[~df.index.duplicated(keep='last')]
        # 去除关键数据缺失的行（Open/High/Low/Close不能为NaN）
        df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])
        # 确保收盘价大于0（避免涨跌幅计算错误）
        df = df[df['Close'] > 0]
        
        # -------------------- 步骤4：计算衍生指标 --------------------
        # 截取最近150个交易日（自动跳过非交易日）
        df = df.last('150D')
        if df.empty:
            raise ValueError("截取150个交易日后无有效数据，请检查数据时间范围！")
        
        # 计算涨跌幅（防御性处理除零错误）
        prev_close = df['Close'].shift(1)
        df['涨幅(%)'] = ((df['Close'] - prev_close) / prev_close * 100).round(2)
        df.loc[prev_close == 0, '涨幅(%)'] = 0.0  # 前一日收盘价为0时涨幅设为0
        
        # 计算多周期均线（默认5/10/20/30/60）
        ma_windows = [5, 10, 20, 30, 60]
        for window in ma_windows:
            df[f'MA{window}'] = df['Close'].rolling(window=window, min_periods=1).mean()
        
        return df.reset_index(drop=False)  # 保留Date列（后续绘图需要索引）
    
    except Exception as e:
        print(f"数据预处理失败：{str(e)}")
        sys.exit(1)  # 预处理失败直接退出


def plot_stock_kline(csv_path: str, 
                     title: str = "股票K线图", 
                     ma_windows: list = [5, 10, 20, 30, 60],
                     figsize: tuple = (14, 8)):
    """
    绘制专业股票K线图（含均线、成交量、最新行情标注）
    
    参数:
        csv_path (str): 股票数据CSV文件路径
        title (str): 图表标题
        ma_windows (list): 均线周期列表
        figsize (tuple): 图表尺寸（宽, 高）
    """
    try:
        # -------------------- 步骤1：读取并预处理数据 --------------------
        # 检查文件是否存在
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"文件不存在：
路径：{csv_path}")
        
        # 读取CSV（仅加载必要列，避免无关数据干扰）
        raw_df = pd.read_csv(
            csv_path,
            parse_dates=['Date'],       # 尝试解析Date列
            index_col='Date',           # 设为索引（方便后续处理）
            usecols=lambda col: col.strip().lower() in ['date', 'open', 'high', 'low', 'close', 'volume'],  # 匹配列名
            na_values=['', 'N/A', 'NaN', '无效日期'],  # 标记无效值
            dayfirst=True,              # 优先解析为DD/MM/YYYY
            date_parser=lambda x: datetime.strptime(x, "%d/%m/%Y")  # 强制解析格式
        )
        
        # 预处理数据
        df = preprocess_data(raw_df)
        df.set_index('Date', inplace=True)  # 恢复Date为索引（mplfinance要求）
        
        # -------------------- 步骤2：配置图表样式 --------------------
        # 自定义颜色（红涨绿跌）
        market_colors = mpf.make_marketcolors(
            up='red', down='green', inherit=True
        )
        # 自定义图表风格
        style = mpf.make_mpf_style(
            marketcolors=market_colors,
            gridstyle='--', gridcolor='lightgray',
            y_on_right=True,  # Y轴在右侧
            facecolor='white',
            rc={'font.size': 12, 'font.family': 'SimHei', 'axes.unicode_minus': False}
        )
        
        # 配置均线样式
        ma_styles = {
            5: {'color': 'crimson', 'width': 1.2},
            10: {'color': 'gold', 'width': 1.2},
            20: {'color': 'black', 'width': 1.5},
            30: {'color': 'darkcyan', 'width': 1.2},
            60: {'color': 'darkgreen', 'width': 1.2}
        }
        # 生成均线附加图
        add_plots = [
            mpf.make_addplot(df[f'MA{window}'], panel=0, **ma_styles.get(window, {'color': 'blue', 'width': 1}))
            for window in ma_windows
        ]
        
        # -------------------- 步骤3：绘制K线图 --------------------
        fig, axes = mpf.plot(
            df,
            type='candle',       # 蜡烛图
            style=style,         # 自定义风格
            addplot=add_plots,   # 添加均线
            title=title,         # 图表标题
            ylabel='价格（元）',  # Y轴标签
            volume=True,         # 显示成交量
            ylabel_lower='成交量（手）',  # 成交量Y轴标签
            datetime_format='%Y-%m-%d',  # 日期格式
            returnfig=True,      # 返回Figure对象（用于标注）
            figsize=figsize      # 图表尺寸
        )
        
        # -------------------- 步骤4：添加最新行情标注 --------------------
        latest = df.iloc[-1]
        prev_day = df.iloc[-2] if len(df) >= 2 else latest
        
        # 左上角：今日行情
        fig.text(
            0.05, 0.95,  # 位置（左上角）
            f"今日行情
"
            f"收盘价: {latest['Close']:.2f} 元
"
            f"开盘价: {latest['Open']:.2f} 元
"
            f"最高价: {latest['High']:.2f} 元
"
            f"最低价: {latest['Low']:.2f} 元
"
            f"涨跌幅: {latest['涨幅(%)']:.2f}%
"
            f"成交量: {latest['Volume']:,} 手",
            color='red' if latest['涨幅(%)'] > 0 else 'green',
            bbox=dict(facecolor='white', alpha=0.8, edgecolor='gray')
        )
        
        # 右上角：均线与数据时间
        ma_text = "
".join([f"MA{w}: {latest[f'MA{w}']:.2f} 元" for w in ma_windows])
        right_text = f"数据时间: {latest.name.strftime('%Y-%m-%d')}
{ma_text}"
        fig.text(
            0.95, 0.95,  # 位置（右上角）
            right_text,
            color='black',
            bbox=dict(facecolor='white', alpha=0.8, edgecolor='gray'),
            ha='right'
        )
        
        # 显示图表
        mpf.show()
        
    except Exception as e:
        print(f"程序运行失败：{str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    # ------------------- 使用示例 -------------------
    csv_file = r"d:\Users\felix\data\600519.csv"  # 替换为你的CSV路径
    plot_stock_kline(
        csv_path=csv_file,
        title="贵州茅台（600519）150天K线图",
        ma_windows=[5, 10, 20, 30, 60],
        figsize=(14, 8)
    )# ------------------------------
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
# 函式定義：數據獲取與緩存
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
        error_msg = f"時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
函式：fetch_and_cache_data
錯誤：{str(e)}
堆疊：{traceback.format_exc()}"
        with open("error.log", "w", encoding='utf-8') as f:
            f.write(error_msg)
        raise

# ------------------------------
# 函式定義：數據預處理（修復列名問題）
# ------------------------------
def preprocess_data(df):
    try:
        print("🔄 開始預處理數據...")
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
        error_msg = f"時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
函式：preprocess_data
錯誤：{str(e)}
堆疊：{traceback.format_exc()}"
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
        error_msg = f"時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
函式：plot_ohlc_chart
錯誤：{str(e)}
堆疊：{traceback.format_exc()}"
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
