# ------------------------------
# 依赖导入
# ------------------------------
import os
import argparse
import pandas as pd
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime
import time
import pytz
import traceback
import logging
from pydantic import BaseSettings, Field, validator
from pathlib import Path
from typing import List

# ------------------------------
# 配置管理（Pydantic 强验证）
# ------------------------------
class StockChartConfig(BaseSettings):
    """全局配置（环境变量优先，支持多股票、自定义样式）"""
    # 核心参数
    ticker: List[str] = Field(default=["0700.HK"], env="TICKER")  # 支持多股票（逗号分隔）
    start_date: str = Field(default="2004-06-16", env="START_DATE")
    end_date: str = Field(default=lambda: datetime.today().strftime("%Y-%m-%d"), env="END_DATE")
    
    # 路径与缓存
    cache_dir: str = Field(default="stock_data", env="CACHE_DIR")
    html_output_template: str = Field(default="ohlc_chart_{ticker}.html", env="HTML_OUTPUT_TEMPLATE")
    csv_output_template: str = Field(default="{ticker}.csv", env="CSV_OUTPUT_TEMPLATE")
    hong_kong_tz: str = Field(default="Asia/Hong_Kong", env="HONG_KONG_TZ")
    
    # 数据下载
    max_retries: int = Field(default=3, env="MAX_RETRIES")
    retry_delay: int = Field(default=5, env="RETRY_DELAY")
    
    # 图表样式
    increasing_color: str = Field(default="#ff0000", env="INCREASING_COLOR")  # 上涨红
    decreasing_color: str = Field(default="#00ff00", env="DECREASING_COLOR")  # 下跌绿
    chart_title_template: str = Field(default="{ticker} 港股历史K线（复权）", env="CHART_TITLE_TEMPLATE")
    
    # 调试
    debug_mode: bool = Field(default=False, env="DEBUG_MODE")

    # 验证逻辑（结束日期不能早于开始日期）
    @validator("end_date")
    def end_date_valid(cls, v: str, values: dict) -> str:
        if "start_date" in values and v < values["start_date"]:
            raise ValueError("结束日期不能早于开始日期！")
        return v


# ------------------------------
# 全局初始化（配置+日志）
# ------------------------------
config = StockChartConfig()  # 加载配置
logger = logging.getLogger(__name__)


def setup_logging(debug: bool = False) -> None:
    """初始化日志（文件+控制台）"""
    log_level = logging.DEBUG if debug else logging.INFO
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # 文件处理器（保存到 stock_chart.log）
    file_handler = logging.FileHandler("stock_chart.log")
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # 配置日志
    logging.basicConfig(
        level=log_level,
        handlers=[file_handler, console_handler],
        force=True  # 覆盖已有配置
    )


# ------------------------------
# 核心函数：缓存有效性检查（避免重复验证）
# ------------------------------
def is_cache_valid(cache_file: Path, max_age_days: int = 7) -> bool:
    """检查缓存是否有效（存在+未过期+有时区）"""
    if not cache_file.exists():
        return False
    
    # 检查缓存时间（7天内有效）
    cache_mtime = cache_file.stat().st_mtime
    now = time.time()
    if (now - cache_mtime) > (max_age_days * 86400):
        logger.warning(f"缓存过期：{cache_file}")
        return False
    
    # 检查时区（确保日期带香港时区）
    df = pd.read_csv(cache_file, parse_dates=["Date"])
    if df["Date"].dt.tz is None:
        logger.warning(f"缓存无时区：{cache_file}")
        return False
    
    return True


# ------------------------------
# 函数定义：数据下载（复用逻辑）
# ------------------------------
def download_stock_data(ticker: str, start: str, end: str, tz: str) -> pd.DataFrame:
    """下载单只股票数据（带重试）"""
    for attempt in range(config.max_retries):
        try:
            logger.info(f"[{ticker}] 下载数据（第{attempt+1}/{config.max_retries}次）...")
            df = yf.download(
                tickers=ticker,
                start=start,
                end=end,
                progress=False,
                auto_adjust=True,  # 复权
                actions=False      # 忽略分红/拆股
            )
            
            if df.empty:
                raise ValueError(f"[{ticker}] Yahoo返回空数据")
            
            # 修复合并列名+时区
            df = df.reset_index().rename(columns={"Datetime": "Date"})
            df.columns = df.columns.str.capitalize()
            df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize("UTC").dt.tz_convert(tz)
            
            # 验证必要列
            required_cols = ["Date", "Open", "High", "Low", "Close"]
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                raise ValueError(f"[{ticker}] 缺失列：{missing}")
            
            return df
        
        except Exception as e:
            logger.error(f"[{ticker}] 下载失败：{str(e)}")
            if attempt < config.max_retries - 1:
                time.sleep(config.retry_delay)
            else:
                raise RuntimeError(f"[{ticker}] 下载超过{config.max_retries}次") from e


# ------------------------------
# 函数定义：数据获取与缓存（支持多股票）
# ------------------------------
def fetch_and_cache_data(ticker: str) -> pd.DataFrame:
    """获取单只股票数据（优先缓存，无效则重下）"""
    cache_file = Path(config.cache_dir) / config.csv_output_template.format(ticker=ticker)
    logger.info(f"[{ticker}] 处理缓存：{cache_file}")
    
    # 1. 尝试加载有效缓存
    if is_cache_valid(cache_file):
        df = pd.read_csv(cache_file, parse_dates=["Date"])
        logger.info(f"[{ticker}] 缓存加载成功")
        return df
    
    # 2. 缓存无效/不存在，下载数据
    df = download_stock_data(ticker, config.start_date, config.end_date, config.hong_kong_tz)
    
    # 3. 保存缓存
    cache_file.parent.mkdir(exist_ok=True)
    df.to_csv(cache_file, index=False, encoding="utf-8")
    logger.info(f"[{ticker}] 缓存保存成功")
    return df


# ------------------------------
# 函数定义：数据预处理
# ------------------------------
def preprocess_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """预处理数据（排序+去重+补漏）"""
    logger.info(f"[{ticker}] 开始预处理...")
    
    # 统一列名+排序
    df.columns = df.columns.str.capitalize()
    df = df.sort_values("Date").reset_index(drop=True)
    
    # 去重+补漏
    df = df.drop_duplicates(subset=["Date"])
    initial = len(df)
    df = df.dropna(subset=["Open", "High", "Low", "Close", "Date"])
    deleted = initial - len(df)
    
    if deleted > 0:
        logger.warning(f"[{ticker}] 删除{deleted}行缺失数据")
    
    # 验证数据量
    if len(df) < 2:
        raise ValueError(f"[{ticker}] 预处理后数据不足（{len(df)}行）")
    
    logger.info(f"[{ticker}] 预处理完成（{len(df)}行）")
    return df


# ------------------------------
# 函数定义：绘制K线图（自定义样式）
# ------------------------------
def plot_ohlc_chart(df: pd.DataFrame, ticker: str) -> None:
    """生成单只股票K线图"""
    logger.info(f"[{ticker}] 开始绘图...")
    
    # 转换日期格式
    df["Date_Str"] = df["Date"].dt.strftime("%Y-%m-%d")
    
    # 生成K线
    fig = go.Figure(data=[go.Ohlc(
        x=df["Date_Str"],
        open=df["Open"],
        high=df["High"],
        low=df["Low"],
        close=df["Close"],
        name=ticker,
        increasing_line_color=config.increasing_color,
        decreasing_line_color=config.decreasing_color
    )])
    
    # 布局设置
    fig.update_layout(
        title={"text": config.chart_title_template.format(ticker=ticker), "x": 0.5},
        xaxis_title="日期",
        yaxis_title="价格（港元）",
        xaxis_rangeslider_visible=False,
        template="plotly_white",
        width=1200,
        height=600
    )
    
    # 输出图表
    output_path = Path(config.html_output_template.format(ticker=ticker))
    fig.write_html(output_path, include_plotlyjs="cdn", auto_open=config.debug_mode)
    logger.info(f"[{ticker}] 图表保存：{output_path}")


# ------------------------------
# 主程序入口
# ------------------------------
def main():
    setup_logging(config.debug_mode)
    logger.info("=== 启动股票K线图生成流程 ===")
    
    try:
        # 循环处理多只股票
        for ticker in config.ticker:
            try:
                # 1. 获取数据（缓存/下载）
                df = fetch_and_cache_data(ticker)
                # 2. 预处理
                processed = preprocess_data(df, ticker)
                # 3. 绘图
                plot_ohlc_chart(processed, ticker)
                logger.info(f"[{ticker}] 流程完成")
            
            except Exception as e:
                logger.error(f"[{ticker}] 流程失败：{str(e)}", exc_info=True)
                continue
    
    finally:
        logger.info("=== 流程结束 ===")


if __name__ == "__main__":
    main()
