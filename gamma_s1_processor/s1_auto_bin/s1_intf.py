import logging
import os
import sys
from contextlib import contextmanager
import json
import datetime
import py_gamma as pg
import time
import glob
import psutil  # 用于监控CPU状态
from concurrent.futures import ProcessPoolExecutor, as_completed
from . import s1_process

# ========== 配置项 ==========
CPU_USAGE_THRESHOLD = 20.0  # CPU使用率阈值（%），超过则减少并行数
MIN_WORKERS = 1  # 最小并行进程数
#MAX_WORKERS = os.cpu_count() or 1  # 最大并行进程数
MAX_WORKERS = 2
CHECK_INTERVAL = 2  # CPU状态检查间隔（秒）

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

def validate_date_format(date_str):
    """
    验证日期字符串是否符合YYYYMMDD格式且是有效的日期
    :param date_str: 日期字符串，如 "20201207"
    :return: 验证通过返回True，否则返回False
    """
    if len(date_str) != 8:
        return False
    try:
        # 尝试将字符串解析为日期，验证格式和有效性
        datetime.datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False
    
def get_current_cpu_usage() -> float:
    """
    获取当前CPU的平均使用率（百分比）
    Returns:
        CPU使用率（0-100）
    """
    # interval=1：采样1秒内的CPU使用率，结果更准确
    return psutil.cpu_percent(interval=1)

def get_dynamic_worker_count() -> int:
    """
    根据当前CPU使用率动态计算可用的进程数
    
    Returns:
        建议的进程数
    """
    cpu_usage = get_current_cpu_usage()
    max_possible = MAX_WORKERS
    
    # 根据CPU使用率动态调整
    if cpu_usage > CPU_USAGE_THRESHOLD:
        # CPU负载高，减少进程数（按使用率比例调整）
        usage_ratio = cpu_usage / 100
        dynamic_workers = max(MIN_WORKERS, int(max_possible * (1 - (usage_ratio - 0.7))))
    else:
        # CPU负载低，使用较多进程数
        dynamic_workers = max_possible
    
    # 确保进程数在合理范围
    dynamic_workers = max(MIN_WORKERS, min(dynamic_workers, max_possible))
    
    #print(f"当前CPU使用率: {cpu_usage:.1f}% | 动态调整进程数为: {dynamic_workers}")
    return dynamic_workers

# ========== 单干涉对处理函数 ==========
def process_single_ifg_pair(args):
    """
    处理单个干涉图对的核心函数
    :param args: 元组 (config, start_date, end_date, intf_num)
    :return: 处理结果（成功/失败）
    """
    config, start_date, end_date, intf_num = args
    logger = logging.getLogger('gamma_s1_processor')

    output_root = config['OUTPUT']['output_root']
    log_root = os.path.join(output_root, "LOGs")
    step5_log_dir = os.path.join(log_root, "step5")
    log_file_path = os.path.join(step5_log_dir, f"{start_date}-{end_date}_intf.log")
    range_looks = config['PROCESSING']['multilook']['range_looks']
    azimuth_looks = config['PROCESSING']['multilook']['azimuth_looks']
    unwrap_thre = config['PROCESSING']['unwrap']['unw_thre']
    ifgs_dir = os.path.join(output_root, "IFGs")
    master_date = str(config['PROCESSING']['common_master_date'])
    master_dir = os.path.join(ifgs_dir, master_date)
    master_slc_par = os.path.join(master_dir, f"{master_date}.slc.par")
    start_date_dir = os.path.join(ifgs_dir, start_date)
    end_date_dir = os.path.join(ifgs_dir, end_date)

    master_mli_par = os.path.join(master_dir, f"{master_date}.mli.par")
    mli_width = pg.ParFile(master_mli_par).get_value("range_samples")

    if start_date != master_date:
        start_slc = os.path.join(start_date_dir, f"{start_date}.rslc")
        start_slc_par = os.path.join(start_date_dir, f"{start_date}.rslc.par")
    else:
        start_slc = os.path.join(master_dir, f"{master_date}.slc")
        start_slc_par = os.path.join(master_dir, f"{master_date}.slc.par")
    
    if end_date != master_date:
        end_slc = os.path.join(end_date_dir, f"{end_date}.rslc")
        end_slc_par = os.path.join(end_date_dir, f"{end_date}.rslc.par")
    else:
        end_slc = os.path.join(master_dir, f"{master_date}.slc")
        end_slc_par = os.path.join(master_dir, f"{master_date}.slc.par")

    if glob.glob(os.path.join(start_date_dir, f"*{start_date}*.off")):
        OFF_par = glob.glob(os.path.join(start_date_dir, f"*{start_date}*.off"))[0]
    elif glob.glob(os.path.join(end_date_dir, f"*{end_date}*.off")):
        OFF_par = glob.glob(os.path.join(end_date_dir, f"*{end_date}*.off"))[0]
    else:
        logger.error(f"未找到OFF_par文件！在 {start_date_dir} 和 {end_date_dir} 中搜索 *{start_date}*.off 和 *{end_date}*.off")
        raise FileNotFoundError(f"未找到OFF_par文件！在 {start_date_dir} 和 {end_date_dir} 中搜索 *{start_date}*.off 和 *{end_date}*.off")
    
    hgt = os.path.join(master_dir, f"{master_date}.hgt")

    intf_dir = os.path.join(ifgs_dir, f"{start_date}-{end_date}")
    os.makedirs(intf_dir, exist_ok=True)

    ## phase_sim_orb
    output_file = os.path.join(intf_dir, f"{start_date}-{end_date}.sim_unw")
    if not os.path.exists(output_file):
        logger.info(f"phase_sim_orb 处理...")
        try:
            with redirect_stdout_stderr(log_file_path):
                print(f"========== phase_sim_orb 执行日志 ==========\n")
                current_dir = os.getcwd()
                os.chdir(intf_dir)  # 切换到日期目录执行，确保输入输出在该目录
                
                stat = pg.phase_sim_orb(
                    SLC1_par = start_slc_par,
                    SLC2R_par = end_slc_par,
                    OFF_par = OFF_par,
                    hgt = hgt,
                    sim_orb = output_file,
                    SLC_ref_par = master_slc_par,
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
                # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"phase_sim_orb 处理成功（状态码：{stat}）")
                
                if os.path.exists(output_file):
                    logger.info(f"phase_sim_orb 处理成功 → {output_file}")
                else:
                    logger.warning(f"未检测到生成的sim_unw文件 {output_file}！")
                logger.info(f"phase_sim_orb {log_file_path}")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"phase_sim_orb执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"phase_sim_orb返回未知状态码：{stat}（预期0/-1）")

        except Exception as e:
            error_msg = f"第 {intf_num} 对 {start_date}-{end_date} 处理失败: {str(e)}"
            logger.error(error_msg)
            with redirect_stdout_stderr(log_file_path):
                print(f"[{datetime.datetime.now()}] 错误: {error_msg}")
            return {
                "pair": f"{start_date}-{end_date}",
                "status": "failed",
                "error": str(e),
                "intf_num": intf_num
                }
        logger.info(f"sim_unw文件已生成至：{output_file}")
    else:
        logger.info(f"sim_unw文件已存在，跳过phase_sim_orb处理 → {output_file}")

    ## SLC_diff_intf
    output_file = os.path.join(intf_dir, f"{start_date}-{end_date}.diff")
    if not os.path.exists(output_file):
        logger.info(f"SLC_diff_intf 处理...")
        try:
            with redirect_stdout_stderr(log_file_path):
                print(f"========== SLC_diff_intf 执行日志 ==========\n")
                current_dir = os.getcwd()
                os.chdir(intf_dir)  # 切换到日期目录执行，确保输入输出在该目录
                sim_unw = os.path.join(intf_dir, f"{start_date}-{end_date}.sim_unw")
                stat = pg.SLC_diff_intf(
                    SLC1 = start_slc,
                    SLC2R = end_slc,
                    SLC1_par = start_slc_par,
                    SLC2R_par = end_slc_par,
                    OFF_par = OFF_par,
                    sim_unw = sim_unw,
                    diff_int = output_file,
                    rlks = range_looks,
                    azlks = azimuth_looks,
                    sps_flg = 0,
                    azf_flg = 0,
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
                # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"SLC_diff_intf 处理成功（状态码：{stat}）")
                
                if os.path.exists(output_file):
                    logger.info(f"SLC_diff_intf 处理成功 → {output_file}")
                else:
                    logger.warning(f"未检测到生成的diff文件 {output_file}！")
                logger.info(f"SLC_diff_intf {log_file_path}")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"SLC_diff_intf执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"SLC_diff_intf返回未知状态码：{stat}（预期0/-1）")

        except Exception as e:
            error_msg = f"第 {intf_num} 对 {start_date}-{end_date} 处理失败: {str(e)}"
            logger.error(error_msg)
            with redirect_stdout_stderr(log_file_path):
                print(f"[{datetime.datetime.now()}] 错误: {error_msg}")
            return {
                "pair": f"{start_date}-{end_date}",
                "status": "failed",
                "error": str(e),
                "intf_num": intf_num
                }
        logger.info(f"diff文件已生成至：{output_file}")
    else:
        logger.info(f"diff文件已存在，跳过SLC_diff_intf处理 → {output_file}")

    ## adf
    output_file = os.path.join(intf_dir, f"{start_date}-{end_date}.cc")
    if not os.path.exists(output_file):
        logger.info(f"adf 处理...")
        try:
            with redirect_stdout_stderr(log_file_path):
                print(f"========== adf 执行日志 ==========\n")
                current_dir = os.getcwd()
                os.chdir(intf_dir)  # 切换到日期目录执行，确保输入输出在该目录
                sm = os.path.join(intf_dir, f"{start_date}-{end_date}.sm")
                stat = pg.adf(
                    interf = os.path.join(intf_dir, f"{start_date}-{end_date}.diff"),
                    sm = sm,
                    cc = output_file,
                    width = mli_width,
                    alpha = 0.6,
                    nfft = 8,
                    cc_win = 7,
                    wfrac = 0.25,
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
                # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"adf 处理成功（状态码：{stat}）")
                
                if os.path.exists(output_file):
                    logger.info(f"adf 处理成功 → {output_file}")
                else:
                    logger.warning(f"未检测到生成的cc文件 {output_file}！")
                logger.info(f"adf {log_file_path}")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"adf执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"adf返回未知状态码：{stat}（预期0/-1）")

        except Exception as e:
            error_msg = f"第 {intf_num} 对 {start_date}-{end_date} 处理失败: {str(e)}"
            logger.error(error_msg)
            with redirect_stdout_stderr(log_file_path):
                print(f"[{datetime.datetime.now()}] 错误: {error_msg}")
            return {
                "pair": f"{start_date}-{end_date}",
                "status": "failed",
                "error": str(e),
                "intf_num": intf_num
                }
        logger.info(f"cc文件已生成至：{output_file}")
    else:
        logger.info(f"cc文件已存在，跳过adf处理 → {output_file}")

    ## rascc_mask
    output_file = os.path.join(intf_dir, f"{start_date}-{end_date}.mask.bmp")
    if not os.path.exists(output_file):
        logger.info(f"rascc_mask处理...")
        try:
            with redirect_stdout_stderr(log_file_path):
                print(f"========== rascc_mask 执行日志 ==========\n")
                current_dir = os.getcwd()
                cc = os.path.join(intf_dir, f"{start_date}-{end_date}.cc")
                os.chdir(intf_dir)  # 切换到日期目录执行，确保输入输出在该目录
                stat = pg.rascc_mask(
                    cc = cc,
                    width = mli_width,
                    cc_thres = unwrap_thre,
                    cc_min = 0.2,
                    cc_max = 0.8,
                    rasf = output_file,
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
                # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"rascc_mask 处理成功（状态码：{stat}）")
                
                if os.path.exists(output_file):
                    logger.info(f"rascc_mask 处理成功 → {output_file}")
                else:
                    logger.warning(f"未检测到生成的mask文件 {output_file}！")
                logger.info(f"rascc_mask {log_file_path}")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"rascc_mask执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"rascc_mask返回未知状态码：{stat}（预期0/-1）")

        except Exception as e:
            error_msg = f"第 {intf_num} 对 {start_date}-{end_date} 处理失败: {str(e)}"
            logger.error(error_msg)
            with redirect_stdout_stderr(log_file_path):
                print(f"[{datetime.datetime.now()}] 错误: {error_msg}")
            return {
                "pair": f"{start_date}-{end_date}",
                "status": "failed",
                "error": str(e),
                "intf_num": intf_num
                }
        logger.info(f"mask文件已生成至：{output_file}")
    else:
        logger.info(f"mask文件已存在，跳过rascc_mask处理 → {output_file}")

    ## mcf
    output_file = os.path.join(intf_dir, f"{start_date}-{end_date}.unw")
    if not os.path.exists(output_file):
        logger.info(f"mcf 处理...")
        try:
            with redirect_stdout_stderr(log_file_path):
                print(f"========== mcf 执行日志 ==========\n")
                current_dir = os.getcwd()
                os.chdir(intf_dir)  # 切换到日期目录执行，确保输入输出在该目录
                sm = os.path.join(intf_dir, f"{start_date}-{end_date}.sm")
                cc = os.path.join(intf_dir, f"{start_date}-{end_date}.cc")
                mask_file = os.path.join(intf_dir, f"{start_date}-{end_date}.mask.bmp")
                stat = pg.mcf(
                    interf = sm,
                    wgt = cc,
                    mask = mask_file,
                    unw = output_file,
                    width = mli_width,
                    ovrlap = 4096,
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
                # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"mcf 处理成功（状态码：{stat}）")
                
                if os.path.exists(output_file):
                    logger.info(f"mcf 处理成功 → {output_file}")
                else:
                    logger.warning(f"未检测到生成的unw文件 {output_file}！")
                logger.info(f"mcf {log_file_path}")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"mcf执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"mcf返回未知状态码：{stat}（预期0/-1）")

        except Exception as e:
            error_msg = f"第 {intf_num} 对 {start_date}-{end_date} 处理失败: {str(e)}"
            logger.error(error_msg)
            with redirect_stdout_stderr(log_file_path):
                print(f"[{datetime.datetime.now()}] 错误: {error_msg}")
            return {
                "pair": f"{start_date}-{end_date}",
                "status": "failed",
                "error": str(e),
                "intf_num": intf_num
                }
        logger.info(f"unw文件已生成至：{output_file}")
    else:
        logger.info(f"unw文件已存在，跳过mcf处理 → {output_file}")

    data1 = os.path.join(intf_dir, f"{start_date}-{end_date}.unw")
    data1_out = os.path.join(intf_dir, f"{start_date}-{end_date}.unw.geo")
    data2 = os.path.join(intf_dir, f"{start_date}-{end_date}.cc")
    data2_out = os.path.join(intf_dir, f"{start_date}-{end_date}.cc.geo")
    data3 = os.path.join(intf_dir, f"{start_date}-{end_date}.diff")
    data3_out = os.path.join(intf_dir, f"{start_date}-{end_date}.diff.geo")


    with redirect_stdout_stderr(log_file_path):
        if not os.path.exists(data1_out):
            s1_process.geocode_image(config, data1, data1_out, type="FLOAT", cmap="rmg.cm")
        if not os.path.exists(data2_out):
            s1_process.geocode_image(config, data2, data2_out, type="FLOAT", cmap="cc.cm")
        if not os.path.exists(data3_out):
            s1_process.geocode_image(config, data3, data3_out, type="FCOMPLEX", cmap="rmg.cm")
    

    logger.info(f"第 {intf_num} 对 {start_date}-{end_date} 处理完成")
    return {
        "pair": f"{start_date}-{end_date}",
        "status": "success",
        "intf_num": intf_num
        }
    
def make_interferogram(config):
    logger = logging.getLogger('gamma_s1_processor')

    output_root = config['OUTPUT']['output_root']
    log_root = os.path.join(output_root, "LOGs")
    step5_log_dir = os.path.join(log_root, "step5")
    ifgs_dir = os.path.join(output_root, "IFGs")
    master_date = str(config['PROCESSING']['common_master_date'])
    master_dir = os.path.join(ifgs_dir, master_date)
    os.makedirs(step5_log_dir, exist_ok=True)

    log_file_path = os.path.join(step5_log_dir, f"make_interferogram.log")
    if "pairs_file" not in config['PROCESSING']['sbas'] or not os.path.exists(config['PROCESSING']['sbas']['pairs_file']):
        logger.info("使用sbas_date_pairs.txt进行SBAS干涉处理...")
        pairs_file = os.path.join(log_root, "sbas_date_pairs.txt")
    else:
        logger.info(f"已找到有效的pairs_file配置，正在使用 {config['PROCESSING']['sbas']['pairs_file']} 进行SBAS干涉处理...")
        pairs_file = config['PROCESSING']['sbas']['pairs_file']

    with redirect_stdout_stderr(log_file_path):
        print("检查date_keep.json和SBAS配对文件格式...")

        date_file_path = os.path.join(log_root, "date_keep.json")
        try:
            with open(date_file_path, 'r', encoding='utf-8') as f:
                date_keep = json.load(f)  # 直接得到字典变量
                print(f"成功加载 date_keep，从 {date_file_path} 中读取到 {len(date_keep)} 个日期条目")

            if master_date not in date_keep.keys():
                raise ValueError(f"主影像日期 {master_date} 不在 date_keep 中！请检查配置的 common_master_date 是否正确，或确认 date.json 中包含该日期。")

        except FileNotFoundError:
            logger.error(f"未找到文件：{date_file_path}")
            date_keep = {}  # 兜底：返回空字典
        except ValueError as e:
            logger.error(f"主影像日期错误：{e}")
            raise

        try:
        # 读取文件内容
            with open(pairs_file, 'r', encoding='utf-8') as f:
                intf_pair = f.readlines()
            print(f"成功读取文件: {pairs_file}，共 {len(intf_pair)} 行内容")
        except FileNotFoundError:
            logger.error(f"文件不存在: {pairs_file}")
            return
        except Exception as e:
            logger.error(f"读取文件时发生错误: {str(e)}")
            return

        # 第一步：验证所有影像对格式和有效性
        valid_pairs = []
        for intf_num, line in enumerate(intf_pair, 1):
            line = line.strip()
            if not line:
                logger.debug(f"第 {intf_num} 行: 空行，跳过检查")
                continue
            
            if '-' not in line:
                logger.error(f"第 {intf_num} 行格式错误：缺少分隔符 '-'")
                raise ValueError(f"第 {intf_num} 行格式错误：缺少分隔符 '-'")
            
            date_parts = line.split('-')
            if len(date_parts) != 2:
                logger.error(f"第 {intf_num} 行格式错误：日期区间格式应为 YYYYMMDD-YYYYMMDD")
                raise ValueError(f"第 {intf_num} 行格式错误：日期区间格式应为 YYYYMMDD-YYYYMMDD")
            
            start_date, end_date = date_parts
            
            if not validate_date_format(start_date):
                raise ValueError(f"第 {intf_num} 行开始日期 '{start_date}' 格式错误或日期无效")
            if start_date not in date_keep:
                raise ValueError(f"第 {intf_num} 行开始日期 '{start_date}' 不存在于date_keep中")
            
            if not validate_date_format(end_date):
                raise ValueError(f"第 {intf_num} 行结束日期 '{end_date}' 格式错误或日期无效")
            if end_date not in date_keep:
                raise ValueError(f"第 {intf_num} 行结束日期 '{end_date}' 不存在于date_keep中")
            
            valid_pairs.append((intf_num, start_date, end_date))
        
        print(f"格式验证完成，共 {len(valid_pairs)} 个有效影像对待处理")
        logger.info(f"格式验证完成，共 {len(valid_pairs)} 个有效影像对待处理")

    # 第二步：动态并行处理有效影像对
    results = []
    # 初始化进程池（动态调整worker数）
    worker_count = get_dynamic_worker_count()
    print(f"初始并行进程数: {worker_count} (CPU使用率: {get_current_cpu_usage():.1f}%)")
    logger.info(f"开始并行处理干涉图，初始进程数: {worker_count}")

    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        # 提交所有任务
        future_to_pair = {}
        for intf_num, start_date, end_date in valid_pairs:
            # 构造任务参数
            task_args = (config, start_date, end_date, intf_num)
            future = executor.submit(process_single_ifg_pair, task_args)
            future_to_pair[future] = (intf_num, start_date, end_date)
            
            # 每提交CHECK_INTERVAL个任务，检查一次CPU状态并动态调整进程池（可选）
            if len(future_to_pair) % CHECK_INTERVAL == 0:
                current_cpu = get_current_cpu_usage()
                new_worker_count = get_dynamic_worker_count()
                if new_worker_count != worker_count:
                    # 注：ProcessPoolExecutor不支持动态调整max_workers，此处可记录日志+重启池（可选）
                    logger.info(f"CPU使用率变化 ({current_cpu:.1f}%)，建议调整进程数: {new_worker_count} (当前: {worker_count})")
                    print(f"CPU使用率变化 ({current_cpu:.1f}%)，建议进程数: {new_worker_count} (当前: {worker_count})")
        
        # 收集处理结果
        for future in as_completed(future_to_pair):
            intf_num, start_date, end_date = future_to_pair[future]
            try:
                result = future.result()
                results.append(result)
                if result["status"] == "success":
                    print(f"完成处理: 第 {intf_num} 对 {start_date}-{end_date}")
                else:
                    print(f"处理失败: 第 {intf_num} 对 {start_date}-{end_date} | 错误: {result['error']}")
            except Exception as e:
                error_msg = f"第 {intf_num} 对 {start_date}-{end_date} 执行异常: {str(e)}"
                logger.error(error_msg)
                print(error_msg)
                results.append({
                    "pair": f"{start_date}-{end_date}",
                    "status": "failed",
                    "error": error_msg,
                    "intf_num": intf_num
                })

    # 第三步：输出处理汇总
    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = len(results) - success_count
    summary = f"干涉图处理完成 | 总计: {len(results)} | 成功: {success_count} | 失败: {failed_count}"
    print(summary)
    logger.info(summary)
    
    # 将结果写入日志文件
    with redirect_stdout_stderr(log_file_path):
        print(f"\n=== 处理汇总 [{datetime.datetime.now()}] ===")
        print(summary)
        if failed_count > 0:
            print("失败列表:")
            for r in results:
                if r["status"] == "failed":
                    print(f"  第 {r['intf_num']} 对 {r['pair']}: {r['error']}")
    
    return results