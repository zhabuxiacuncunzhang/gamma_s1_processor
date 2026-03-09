import os
import logging
import subprocess
import json
import sys
from contextlib import contextmanager
import py_gamma as pg
import glob
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np

# ========== 适配所有系统的衬线字体设置 ==========
plt.rcParams['font.family'] = 'DejaVu Serif'
plt.rcParams['font.serif'] = ['DejaVu Serif']  # 衬线字体指定为DejaVu Serif
plt.rcParams['axes.unicode_minus'] = False  # 负号正常显示
# ======================================================

# ========== 输出重定向上下文管理器 ==========
@contextmanager
def redirect_stdout_stderr(log_file_path):
    """
    临时重定向stdout和stderr到指定文件
    :param log_file_path: 日志文件路径
    """
    # 保存原始的stdout和stderr
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    
    # 打开日志文件（追加模式，编码utf-8）
    with open(log_file_path, 'a', encoding='utf-8') as log_file:
        # 替换stdout和stderr
        sys.stdout = log_file
        sys.stderr = log_file
        try:
            yield  # 执行被包裹的代码
        finally:
            # 恢复原始的stdout和stderr
            sys.stdout = original_stdout
            sys.stderr = original_stderr

def plot_relative_baseline_chart(file_path, output_dir, fig_name="baseline_time_plot", dpi=300):
    """
    Plot time-spatial baseline chart based on relative image connections (with center calibration)
    
    Parameters:
    file_path: str - Path to input data file (serial_num date1 date2 baseline_length other columns)
    output_dir: str - Folder to save the chart
    fig_name: str - Name of the output chart file
    dpi: int - Resolution of the chart
    """
    # ===================== Step 1: Read and parse data =====================
    col_names = ["serial_num", "date1", "date2", "baseline_length", "col5", "col6", "col7", "col8", "col9"]
    # Read space-separated text file
    df = pd.read_csv(
        file_path,
        sep=r'\s+',  # Match any number of spaces
        header=None,
        names=col_names,
        dtype={"date1": str, "date2": str, "baseline_length": float}
    )
    
    # Extract all unique image dates and convert to datetime format
    all_dates = pd.Series(list(df['date1'].unique()) + list(df['date2'].unique())).unique()
    date_dt = {date: pd.to_datetime(date, format='%Y%m%d') for date in all_dates}
    # Sort all image dates by time (for plotting)
    sorted_dates = sorted(all_dates, key=lambda x: date_dt[x])
    
    # ===================== Step 2: Calculate absolute spatial position of each image =====================
    # Initialization: Select the earliest date as baseline zero (modifiable as needed)
    base_date = sorted_dates[0]
    baseline_pos = {base_date: 0.0}  # Store absolute baseline position for each date
    
    # Iterate through all image pairs to derive absolute positions (handle bidirectional connections)
    for _, row in df.iterrows():
        d1, d2, bl = row['date1'], row['date2'], row['baseline_length']
        
        # Case 1: Known d1, derive d2
        if d1 in baseline_pos and d2 not in baseline_pos:
            baseline_pos[d2] = baseline_pos[d1] + bl
        # Case 2: Known d2, derive d1 (reverse)
        elif d2 in baseline_pos and d1 not in baseline_pos:
            baseline_pos[d1] = baseline_pos[d2] - bl
    
    # ===================== Step 3: Center calibration (balance chart layout) =====================
    # Calculate median of all absolute baselines as center offset
    all_positions = np.array(list(baseline_pos.values()))
    center_offset = np.median(all_positions)
    # Calibrate all positions (set median to 0 for centered chart)
    calibrated_pos = {date: pos - center_offset for date, pos in baseline_pos.items()}
    
    # ===================== Step 4: Plot the chart =====================
    plt.figure(figsize=(12, 7))
    
    # 1. Plot scatter points for all images (core data points)
    dates_dt_list = [date_dt[date] for date in sorted_dates]
    pos_list = [calibrated_pos[date] for date in sorted_dates]
    plt.scatter(
        dates_dt_list, pos_list,
        color='#1f77b4',  # 固定为蓝色（也可写 color='blue'）
        s=100, marker='o', 
        label='SAR Image', zorder=5
    )
    
    # 2. Plot connecting lines for image pairs (show relative relationships)
    for _, row in df.iterrows():
        d1, d2 = row['date1'], row['date2']
        # Get time and calibrated position for two dates
        x = [date_dt[d1], date_dt[d2]]
        y = [calibrated_pos[d1], calibrated_pos[d2]]
        plt.plot(x, y, color='#7f7f7f',  # 固定为灰色（也可写 color='gray'）
         linestyle='--', linewidth=1.5, alpha=0.7, zorder=3)
    
    # 3. Add date annotations for images (easy identification)
    for date in sorted_dates:
        plt.annotate(
            date,  # Annotation text (date)
            (date_dt[date], calibrated_pos[date]),
            xytext=(5, 5), textcoords='offset points',  # Offset to avoid occlusion
            fontsize=9, color='black'
        )
    
    # ===================== Step 5: Chart beautification and calibration =====================
    plt.xlabel('Time', fontsize=12, fontweight='bold')
    plt.ylabel('Perpendicular baseline (m)', fontsize=12, fontweight='bold')
    #plt.title('Spatio-temporal baseline', fontsize=14, fontweight='bold', pad=20)
    
    # Add horizontal center line (calibrated center)
    # plt.axhline(y=0, color='gray', linestyle='-', linewidth=1, alpha=0.8, label='Center Baseline (Median)')
    
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(loc='best', fontsize=10)
    # Auto-adjust x-axis time labels (avoid overlap)
    plt.gcf().autofmt_xdate()
    
    # ===================== Step 6: Save the chart =====================
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_path = os.path.join(output_dir, fig_name)
    plt.savefig(
        output_path + ".png", dpi=dpi, bbox_inches='tight', 
        facecolor='#f8f9fa'
    )
    plt.savefig(
        output_path + ".pdf", dpi=dpi, bbox_inches='tight', 
        facecolor='#f8f9fa'  
    )
    plt.close()
      
    return output_path

def calc_base(config, pairs_file, date_keep):
    """
    计算时空基线
    :param config: 配置对象
    :param pairs_file: 干涉对文件路径
    """
    logger = logging.getLogger('gamma_s1_processor')
    
    # 读取干涉对文件
    if not os.path.exists(pairs_file):
        logger.error(f"干涉对文件 {pairs_file} 不存在")
        return
    
    master_date = str(config['PROCESSING']['common_master_date'])
    output_root = config['OUTPUT']['output_root']
    ifgs_dir = os.path.join(output_root, "IFGs")
    logs_dir = os.path.join(output_root, "LOGs")
    base_dir = os.path.join(logs_dir, "BASE")

    SLC_tab = os.path.join(base_dir, "SLC_tab")
    bperp_file = os.path.join(base_dir, "bperp")
    itab = os.path.join(base_dir, "itab")

    try:
        if os.path.exists(base_dir):
            raise FileExistsError(f"输出目录 {base_dir} 已存在")
        # 目录不存在则创建
        os.makedirs(base_dir, exist_ok=False)
        logger.info(f"成功创建目录：{base_dir}")
    except FileExistsError as e:
        logger.warning(f"创建目录失败：{e}")

    logger.info(f"生成 SLC_tab 文件")
    with open(SLC_tab, 'w', encoding='utf-8') as f:
        for date_str, zip_list in date_keep.items():
            slc_path = os.path.join(ifgs_dir, date_str, f"{date_str}.slc")
            slc_par_path = os.path.join(ifgs_dir, date_str, f"{date_str}.slc.par")

            f.write(f"{slc_path} {slc_par_path}\n")

    logger.info(f"SLC_tab 文件已生成：{SLC_tab}")

    logger.info(f"生成 itab 文件")

    date_list = list(date_keep.keys())
    with open(pairs_file, 'r', encoding='utf-8') as f_in, \
         open(itab, 'w', encoding='utf-8') as f_out:
        
        line_num = 1  # 对应格式中的"序号"（逐行递增）
        for line in f_in:
            line = line.strip()
            if not line:
                continue  # 跳过空行

            # 拆分日期对
            try:
                date1_str, date2_str = line.split("-")
            except ValueError:
                logger.warning(f"警告：行格式错误，跳过该行：{line}")
                continue

            # 检查日期是否在date_keep中，并获取原索引（索引从0开始，如需从1开始则+1）
            try:
                # 核心：获取日期在原字典中的索引（不排序），这里+1让序号从1开始，可根据需求去掉
                date1_idx = date_list.index(date1_str) + 1
                date2_idx = date_list.index(date2_str) + 1
            except ValueError as e:
                logger.warning(f"警告：日期 {e} 不在date_keep中，跳过该行：{line}")
                continue
            
            # 按格式写入：日期序号1 日期序号2 序号 1
            write_line = f"{date1_idx} {date2_idx} {line_num} 1\n"
            f_out.write(write_line)
            
            line_num += 1
    
    logger.info(f"itab 文件已生成：{itab}")

    # 利用base_calc计算base
    logger.info(f"计算基线...")
    log_file_path = os.path.join(logs_dir, "step4", "base_calc.log")
    try:
        with redirect_stdout_stderr(log_file_path):
            print(f"========== base_calc 执行日志 ==========\n")
            current_dir = os.getcwd()
            os.chdir(base_dir)  # 切换到日期目录执行，确保输入输出在该目录
            stat = pg.base_calc(
                SLC_tab = SLC_tab,
                SLC_par = os.path.join(ifgs_dir, master_date, f"{master_date}.slc.par"), 
                bperp_file = bperp_file,
                itab = itab,
                itab_type = 1,
                plt_flg = 1,)
            os.chdir(current_dir)  # 恢复原始工作目录
            # 根据返回状态码判断执行结果
        if stat == 0:
            logger.info(f"bperp_file生成成功（状态码：{stat}）")
            # 验证文件是否生成（可选，增强鲁棒性）
            base_files = glob.glob(bperp_file)
            if base_files:
                logger.info(f"bperp_file生成成功，文件路径：{base_files[0]}") 
            else:
                logger.warning(f"未检测到生成的bperp文件！")
            logger.info(f"base_calc屏幕输出已保存至：{log_file_path}")
        elif stat == -1:
            # 状态码-1表示失败，主动抛出异常
            raise RuntimeError(f"base_calc执行失败（状态码：{stat}）")
        else:
            # 处理未知状态码（防止函数返回其他值）
            raise RuntimeError(f"base_calc返回未知状态码：{stat}（预期0/-1）")
            
    except Exception as e:
        logger.error(f"调用base_calc时发生未知错误：{e}")
        raise

    if not os.path.exists(bperp_file):
        logger.error(f"基线文件 {bperp_file} 未生成")
    else:
        logger.info(f"开始绘制基线文件：{bperp_file}")
        plot_relative_baseline_chart(bperp_file, base_dir, fig_name="baseline_time_plot", dpi=300)
        logger.info(f"基线图已生成：{base_dir}/baseline_time_plot.png")

