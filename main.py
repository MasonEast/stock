import logging
import os
import yaml

from strategy import end
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
        end.end_buy_cyb(cyb_stocks, cyb_codes, logger)

        return {"config": config, }
    except Exception as e:
        logger.exception("Initialization failed")

if __name__ == '__main__':
    result = main()  # 明确调用入口
    if result:
        logger.debug("Initialization complete")  # 生产环境可改为DEBUG级别