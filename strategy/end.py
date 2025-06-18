import pandas as pd

from tqdm import tqdm  # 进度条工具
from utils import has_zt_last60d, calc_15d_change, check_consecutive_down_with_volume_decrease, is_zhangting, get_history_data

def end_buy_cyb(cyb_stocks, cyb_codes, logger):
    results = []
    for code in tqdm(cyb_codes):  
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
            print(f"创业板股票 {code} 满足条件")

    result_df = pd.DataFrame(results)
    # 将DataFrame转换为字符串并记录到日志
    logger.info("创业板,近60日有涨停,近15日涨幅>10%%,连续两天价跌量缩[2,6]的股票:\n%s", result_df.to_string())


    # 步骤1：批量获取所有股票历史数据（假设已经有一个包含所有股票历史数据的DataFrame：all_data）
    # 如果还没有，则循环获取并拼接（这里假设通过批量获取函数实现）
    all_data = get_all_history_data(cyb_codes)  # 需要实现这个批量获取函数

    # 步骤2：按股票代码分组
    grouped = all_data.groupby('code')

    # 步骤3：向量化计算条件
    # 条件1：近60日有涨停
    cond1 = grouped['is_zt'].tail(60).groupby('code').sum() > 0

    # 条件2：近15日涨幅>10%
    # 获取每只股票最后15天的数据
    last_15 = grouped.tail(15)
    # 计算每只股票15天内的涨幅
    def calc_change(group):
        if len(group) < 15:
            return np.nan
        start_price = group.iloc[0]['close']
        end_price = group.iloc[-1]['close']
        return (end_price - start_price) / start_price * 100
    changes = last_15.groupby('code').apply(calc_change)
    cond2 = changes > 10

    # 条件3：连续两天价跌量缩[2,6]（这里简化处理，实际需要更严谨的实现）
    # 我们定义一个函数来检测每只股票最后3天是否有连续两天价跌量缩
    def check_consecutive(group):
        # 取最后3天
        last_3 = group.tail(3)
        if len(last_3) < 3:
            return False
        # 判断连续两天价跌量缩
        for i in range(1, len(last_3)-1):
            # 第i天和第i-1天比较
            cond1 = (last_3.iloc[i]['close'] < last_3.iloc[i-1]['close']) and (last_3.iloc[i]['volume'] < last_3.iloc[i-1]['volume'])
            # 第i+1天和第i天比较
            cond2 = (last_3.iloc[i+1]['close'] < last_3.iloc[i]['close']) and (last_3.iloc[i+1]['volume'] < last_3.iloc[i]['volume'])
            if cond1 and cond2:
                return True
        return False
    cond3 = grouped.apply(check_consecutive)

    # 合并条件
    valid_codes = cond1 & cond2 & cond3
    valid_codes = valid_codes[valid_codes].index.tolist()

    # 步骤4：构造结果DataFrame
    results = []
    for code in valid_codes:
        # 获取该股票的最新数据
        latest_data = grouped.get_group(code).iloc[-1]
        # 从cyb_stocks中获取股票名称
        stock_name = cyb_stocks[cyb_stocks['code'] == code]['name'].values[0]
        results.append({
            '代码': code,
            '名称': stock_name,
            '当前价': latest_data['close'],
            '60日涨停次数': cond1.loc[code],  # 注意：cond1是Series，存放的是每只股票60日涨停次数是否大于0（布尔值），这里需要实际涨停次数
            # 所以需要重新计算实际涨停次数（或者之前cond1可以改为存储实际次数）
            '15日涨幅(%)': round(changes.loc[code], 2),
            '连续价跌量缩': "是"
        })
    result_df = pd.DataFrame(results)

    # 修正：60日涨停次数应该是实际次数，而不是布尔值
    # 所以我们重新计算实际次数（或者在上面计算cond1时保留次数）
    # 这里我们重新计算（如果性能影响不大）
    result_df['60日涨停次数'] = result_df['代码'].apply(lambda code: grouped.get_group(code)['is_zt'].tail(60).sum())

    # 记录日志
    logger.info("创业板,近60日有涨停,近15日涨幅>10%%,连续两天价跌量缩[2,6]的股票:\n%s", result_df.to_string(index=False))  # 注意：这里用index=False隐藏索引，避免之前遇到的格式化问题[6](@ref)

    return result_df