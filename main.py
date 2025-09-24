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
    )
