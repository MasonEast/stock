import logging
import os
import yaml

import pandas as pd
from tqdm import tqdm  # 进度条工具

from utils import has_zt_last60d, calc_15d_change, check_consecutive_down_with_volume_decrease, is_zhangting, get_history_data
from utils import get_cyb_stock_codes   
# 初始化日志配置（避免重复初始化）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='stock.log',
    filemode='a'
)
logger = logging.getLogger(__name__)

result = get_cyb_stock_codes()
cyb_stocks, cyb_codes = result['cyb_stock_list'], result['cyb_stock_code']

print(f"创业板股票数量: {len(cyb_codes)}")

results = []
for code in tqdm(cyb_codes[:100]):  
    df = get_history_data(code)
    if df.empty:
        continue
    
    # 条件1：近60日有涨停
    cond1 = has_zt_last60d(df.tail(60))
    
    # 条件2：近15日涨幅＞10%
    cond2 = calc_15d_change(df.tail(15)) > 10

    # 条件3：连续两天价跌量缩[2,6]
    cond3 = check_consecutive_down_with_volume_decrease(df, lookback_days=3)
    
    if cond1 and cond2 and cond3:
        latest_data = df.iloc[-1]
        results.append({
            '代码': code,
            '名称': cyb_stocks[cyb_stocks['code']==code]['name'].values[0],
            '当前价': latest_data['close'],
            '60日涨停次数': df.tail(60)['is_zt'].sum(),
            '15日涨幅(%)': round(calc_15d_change(df.tail(15)), 2),
            '连续价跌量缩': "是"
        })

result_df = pd.DataFrame(results)
# 将DataFrame转换为字符串并记录到日志
logger.info("当前数据:\n%s", df.to_string())




def load_config() -> dict:
    """加载配置文件并缓存结果（避免重复IO）"""
    root_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(root_dir, 'config.yaml')
    try:
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_file}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"YAML parsing error: {e}")
        raise


def main():
    """主逻辑封装为函数（避免全局变量污染）"""
    try:
        config = load_config()
        logger.info("Configuration loaded successfully")

        return {"config": config, }
    except Exception as e:
        logger.exception("Initialization failed")

if __name__ == '__main__':
    result = main()  # 明确调用入口
    if result:
        logger.debug("Initialization complete")  # 生产环境可改为DEBUG级别