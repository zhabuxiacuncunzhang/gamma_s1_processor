import datetime
from typing import List, Dict
import re
import os

def parse_date(date_str: str) -> datetime.date:
    """解析日期字符串为datetime.date对象（支持YYYYMMDD格式）"""
    if not re.match(r'^\d{8}$', date_str):
        raise ValueError(f"日期格式错误 {date_str}，必须是YYYYMMDD格式（8位数字）")
    
    try:
        return datetime.date(
            int(date_str[:4]),  # 年
            int(date_str[4:6]), # 月
            int(date_str[6:8])  # 日
        )
    except ValueError as e:
        raise ValueError(f"日期 {date_str} 无效（如月份13、日期32等）: {e}")

def is_winter_month(month: int, winter_start: int = 11, winter_end: int = 3) -> bool:
    """判断月份是否为冬季（11-3月）"""
    if winter_start > winter_end:
        return month >= winter_start or month <= winter_end
    return winter_start <= month <= winter_end

def calculate_day_diff(date1: datetime.date, date2: datetime.date) -> int:
    """计算两个日期之间的天数差（date2 - date1，正数）"""
    delta = date2 - date1
    return delta.days

def generate_sbas_date_pairs_from_dict(
    date_dict: Dict[str, any],  # 日期作为key的字典
    sbas_config: dict
) -> List[str]:
    """
    从字典的keys中提取日期，生成符合SBAS规则的YYYYMMDD-YYYYMMDD时间对
    核心规则：
    1. 互斥规则：冬季模式/跨年模式只能开启一个，都关闭则用普通模式
    2. 冬季模式：仅保留11-3月的配对，天数差≤temp_baseline
    3. 跨年模式：仅保留 365*n - baseline_substract < 天数差 < 365*n + baseline_add 的配对
    4. 普通模式：仅保留天数差≤temp_baseline的配对
    5. 所有模式：单个影像向后连接个数 ≤ max_con_ifg
    """
    # 1. 提取并过滤字典中的日期key
    raw_dates = list(date_dict.keys())
    valid_dates = []
    invalid_dates = []
    
    for date_str in raw_dates:
        try:
            parse_date(date_str)  # 验证日期有效性
            valid_dates.append(date_str)
        except ValueError as e:
            invalid_dates.append((date_str, str(e)))
    
    # 打印无效日期提示
    if invalid_dates:
        print("⚠️  以下日期格式无效，已跳过：")
        for date, err in invalid_dates:
            print(f"   {date}: {err}")
    
    if not valid_dates:
        raise ValueError("字典中没有有效的YYYYMMDD格式日期！")
    
    # 2. 解析SBAS配置（重命名+新增参数+互斥校验）
    # 冬季模式配置
    if_winter_only = sbas_config['if_winter_only']['option']
    winter_start = sbas_config['if_winter_only']['winter_start']
    winter_end = sbas_config['if_winter_only']['winter_end']
    
    # 跨年模式配置（重命名参数+新增number_of_year）
    if_year_pair = sbas_config['if_year_pair']['option']
    number_of_year = sbas_config['if_year_pair']['number_of_year']  # 年数n
    baseline_substract = sbas_config['if_year_pair']['baseline_substract']  # 负向偏移
    baseline_add = sbas_config['if_year_pair']['baseline_add']  # 正向偏移
    
    # 通用配置
    temp_baseline = sbas_config['temp_baseline']  # 普通/冬季模式天数阈值（天）
    max_con_ifg = sbas_config['max_con_ifg']      # 单个影像向后连接个数
    
    # 关键：互斥校验（冬季模式和跨年模式只能开一个）
    if if_winter_only and if_year_pair:
        raise ValueError("❌ 错误：if_winter_only 和 if_year_pair 只能开启一个，不可同时启用！")
    
    # 3. 排序日期（按时间升序）
    parsed_dates = [(parse_date(d), d) for d in valid_dates]
    parsed_dates.sort(key=lambda x: x[0])
    sorted_dates = [d[1] for d in parsed_dates]
    sorted_parsed = [d[0] for d in parsed_dates]
    
    # 4. 生成时间对（核心逻辑：区分三种模式）
    date_pairs = []
    total_dates = len(sorted_dates)
    
    for i in range(total_dates):
        date1 = sorted_parsed[i]
        date1_str = sorted_dates[i]
        connect_count = 0  # 记录当前日期已连接的有效个数
        
        # 遍历当前日期之后的所有日期
        for j in range(i + 1, total_dates):
            date2 = sorted_parsed[j]
            date2_str = sorted_dates[j]
            
            # 计算天数差（date2 - date1，正数）
            day_diff = calculate_day_diff(date1, date2)
            valid_pair = False  # 标记当前配对是否有效
            
            # ========== 模式1：冬季模式（if_winter_only=True） ==========
            if if_winter_only:
                # 条件1：天数差 ≤ temp_baseline
                # 条件2：两个日期都在冬季（11-3月）
                if day_diff <= temp_baseline and \
                   is_winter_month(date1.month, winter_start, winter_end) and \
                   is_winter_month(date2.month, winter_start, winter_end):
                    valid_pair = True
            
            # ========== 模式2：跨年模式（if_year_pair=True） ==========
            elif if_year_pair:
                # 计算多年跨年区间：365*n - sub < 天数差 < 365*n + add
                year_days = 365 * number_of_year
                lower_bound = year_days - baseline_substract
                upper_bound = year_days + baseline_add
                if lower_bound < day_diff < upper_bound:
                    valid_pair = True
            
            # ========== 模式3：普通模式（两个模式都关闭） ==========
            else:
                if day_diff <= temp_baseline:
                    valid_pair = True
            
            # 基线不满足则跳过
            if not valid_pair:
                continue
            
            # 连接个数已达上限则终止当前日期的配对
            if connect_count >= max_con_ifg:
                break
            
            # 所有规则满足，生成时间对
            date_pair = f"{date1_str}-{date2_str}"
            date_pairs.append(date_pair)
            connect_count += 1  # 有效连接个数+1
    
    return date_pairs

def save_date_pairs_to_file(date_pairs: List[str], file_path: str = "sbas_date_pairs.txt"):
    """将时间对列表保存到文件，每行一个"""
    with open(file_path, 'w', encoding='utf-8') as f:
        for pair in date_pairs:
            f.write(pair + '\n')
    print(f"\n✅ 时间对已保存到文件：{file_path}")

# ---------------------- 主程序 ----------------------
def make_sbas_date_pairs(config, date_dict):
    
    try:
        date_pairs = generate_sbas_date_pairs_from_dict(date_dict, config['PROCESSING']['sbas'])
        
        # 保存到文件
        output_file = os.path.join(config['OUTPUT']['output_root'], "LOGs", "sbas_date_pairs.txt")
        save_date_pairs_to_file(date_pairs, output_file)
        
        # 输出变量和预览
        print("\n📌 生成的时间对变量（可直接使用）：")
        print(f"sbas_date_pairs = {date_pairs}")
        print(f"\n 共生成 {len(date_pairs)} 个有效时间对：")
        for idx, pair in enumerate(date_pairs, 1):
            print(f"   {idx}. {pair}")
    
    except ValueError as e:
        print(f"错误：{e}")