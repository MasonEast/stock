import pandas as pd
import akshare as ak
from functools import lru_cache
import time
import logging

@lru_cache(maxsize=1)
def get_stock_list():
    return ak.stock_info_a_code_name()

def get_cyb_stock_codes():
    """获取创业板股票代码列表"""
    # 获取股票列表
    stock_list = get_stock_list()
    # 获取创业板股票代码
    cyb_stocks = stock_list[stock_list['code'].str.match(r'^(300|301)')]
    # 返回创业板股票代码列表
    return {
        'cyb_stock_list': cyb_stocks,
        'cyb_stock_code': cyb_stocks['code'].tolist()
    }

@lru_cache(maxsize=1)
def fetch_top_stocks(period: str = "近三月", min_buy_count: int = 1, logger: logging.Logger = None) -> list:
    """获取机构买入次数达标的股票代码（带缓存和重试机制）"""
    for attempt in range(3):  # 最多重试3次
        try:
            df = ak.stock_lhb_stock_statistic_em(symbol=period)
            mask = df['买方机构次数'] > min_buy_count
            return df.loc[mask, '代码'].tolist()
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
            time.sleep(2)  # 网络错误时延迟重试
    logger.error("Failed to fetch stock data after 3 attempts")
    return []  # 返回空列表避免后续崩溃

def has_zt_last60d(df):
    """检查近60个交易日是否有涨停"""
    return df['is_zt'].any()

def calc_15d_change(df):
    """计算最近15个交易日的涨幅"""
    if len(df) < 15:
        return 0
    latest_close = df.iloc[-1]['close']
    past_close = df.iloc[-15]['close']
    return (latest_close - past_close) / past_close * 100

def check_consecutive_down_with_volume_decrease(df, lookback_days=60):
    """
    判断最近lookback_days天内是否存在连续两天价跌量缩
    价跌：当日收盘价 < 前一日收盘价
    量缩：当日成交量 < 前一日成交量
    """
    if len(df) < 2:
        return False
    recent_df = df.tail(lookback_days).copy()
    # 计算价跌和量缩条件
    recent_df['price_down'] = recent_df['close'] < recent_df['close'].shift(1)
    recent_df['volume_down'] = recent_df['volume'] < recent_df['volume'].shift(1)
    # 标记连续两天满足条件的情况[2,6](@ref)
    recent_df['consecutive'] = recent_df['price_down'] & recent_df['volume_down'] & recent_df['price_down'].shift(1) & recent_df['volume_down'].shift(1)
    return recent_df['consecutive'].any()

def is_zhangting(row):
    """判断当日是否涨停"""
    pre_close = row['pre_close']
    zt_price = round(pre_close * 1.1, 2)
    return row['close'] >= zt_price

def get_history_data(code, days=60):
    """获取指定天数历史数据并标记涨停日"""
    try:
        start_date=pd.Timestamp.today() - pd.Timedelta(days=days*2)
        df = ak.stock_zh_a_hist(symbol=code, period="daily", 
                                start_date=start_date.strftime('%Y%m%d'),  
                                adjust="qfq")

        df['date'] = pd.to_datetime(df['日期'])
        df['pre_close'] = df['收盘'].shift(1).bfill()  # 前一日收盘价
        df['close'] = df['收盘']
        df['is_zt'] = df.apply(is_zhangting, axis=1)
        df['volume'] = df['成交量']

        return df[['date', 'close', 'volume', 'is_zt']]
    except Exception as e:
        print(f"获取 {code} 数据失败: {e}")
        return pd.DataFrame()
  