import os
import logging
import subprocess
import json
import sys
from contextlib import contextmanager
import py_gamma as pg
import glob

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

def read_burst_number_table(file_path):
    """
    读取burst_number_table文件，提取所有iw的number_of_bursts值
    :param file_path: burst_number_table文件路径
    :return: 字典，格式如 {'iw1': 5, 'iw2': xx, 'iw3': xx}
    """
    # 初始化存储number_of_bursts的变量
    number_of_bursts_dict = {}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # 逐行读取文件
            for line in f:
                line = line.strip()
                # 跳过空行和非number_of_bursts的行
                if not line or 'number_of_bursts' not in line:
                    continue
                
                # 分割键值对（处理冒号后有空格的情况）
                key_part, value_part = line.split(':', 1)
                key = key_part.strip()
                value = value_part.strip()
                
                # 提取iw标识（iw1/iw2/iw3）
                iw_prefix = key.split('_')[0]  # 从iw1_number_of_bursts中提取iw1
                # 转换为整数并存储
                number_of_bursts_dict[iw_prefix] = int(value)
                
    except FileNotFoundError:
        print(f"错误：未找到文件 {file_path}")
    except ValueError:
        print(f"错误：文件 {file_path} 中number_of_bursts的值不是有效整数")
    except Exception as e:
        print(f"读取文件时发生错误：{str(e)}")
    
    return number_of_bursts_dict

def coregister_s1_images(config):
    """
    执行S1影像配准操作
    """
    logger = logging.getLogger('gamma_s1_processor')
    try:
        bin_dir = config['GAMMA_PATH']['bin_dir']
        output_root = config['OUTPUT']['output_root']
        log_root = os.path.join(output_root, "LOGs")
        step4_log_dir = os.path.join(log_root, "step4")
        date_file_path = os.path.join(log_root, "date.json")
        orbit_dir = config['GAMMA_PATH']['orbit_dir']
        kml_path = config['PROCESSING']['kml_path']
        ifgs_dir = os.path.join(output_root, "IFGs")
        master_date = str(config['PROCESSING']['common_master_date'])
        master_dir = os.path.join(ifgs_dir, master_date)
        range_looks = config['PROCESSING']['multilook']['range_looks']
        azimuth_looks = config['PROCESSING']['multilook']['azimuth_looks']


        #log_file_path = os.path.join(step4_log_dir, f"{master_date}_coregister.log")

        try:
            with open(date_file_path, 'r', encoding='utf-8') as f:
                date_zip_map = json.load(f)  # 直接得到字典变量
                logger.info(f"成功加载 date_zip_map，从 {date_file_path} 中读取到 {len(date_zip_map)} 个日期条目")

            if master_date not in date_zip_map.keys():
                raise ValueError(f"主影像日期 {master_date} 不在 date_zip_map 中！请检查配置的 common_master_date 是否正确，或确认 date.json 中包含该日期。")

        except FileNotFoundError:
            logger.error(f"未找到文件：{date_file_path}")
            date_zip_map = {}  # 兜底：返回空字典
        except ValueError as e:
            logger.error(f"主影像日期错误：{e}")
            raise

        
        for date_str, zip_list in date_zip_map.items():
            # 创建日期子目录（如SLCs/20201231）
            if date_str != master_date:
                date_dir = os.path.join(ifgs_dir, date_str)
                os.makedirs(date_dir, exist_ok=True)

                # 定义该日期的日志文件路径（如：LOGs/step4/20201231_read_S1_TOPS_SLC.log）
                log_file_path = os.path.join(step4_log_dir, f"{date_str}_read_S1_TOPS_SLC.log")
                # 生成ziplist文件（记录该日期的所有压缩包路径）
                ziplist_path = os.path.join(date_dir, "ziplist.txt")
                # 写入ziplist（每行一个压缩包路径）
                with open(ziplist_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(zip_list))

                logger.info(f"日期 {date_str}：创建目录 {date_dir} | 生成ziplist {ziplist_path} | 包含 {len(zip_list)} 个文件")
        
                # 利用read_S1_TOPS_SLC.py脚本得到每个日期的kml
                logger.info(f"开始为日期 {date_str} 调用read_S1_TOPS_SLC.py生成slc...")
                slc_files = glob.glob(os.path.join(date_dir, f"{date_str}*.slc"))
                if slc_files:
                    logger.info(f"日期 {date_str} SLC → {slc_files[:2]}已存在") 
                    continue  # 跳过已存在的SLC文件，避免重复处理

                try:
                    with redirect_stdout_stderr(log_file_path):
                        print(f"========== {date_str} read_S1_TOPS_SLC 执行日志 ==========\n")
                        stat = pg.read_S1_TOPS_SLC(
                        input = ziplist_path,
                        root_name = date_str, 
                        pol = 'VV',
                        burst_sel = kml_path,
                        OPOD_dir = orbit_dir,
                        out_dir = date_dir,
                        kml = True,)
                    # 根据返回状态码判断执行结果
                    if stat == 0:
                        logger.info(f"日期 {date_str}：read_S1_TOPS_SLC执行成功（状态码：{stat}）")
                    # 验证KML文件是否生成（可选，增强鲁棒性）
                        kml_files = glob.glob(os.path.join(date_dir, f"{date_str}*.kml"))
                        if kml_files:
                            logger.info(f"日期 {date_str}：成功生成 {len(kml_files)} 个KML文件 → {kml_files[:2]}...") 
                        else:
                            logger.warning(f"日期 {date_str}：未检测到生成的KML文件！")
                        logger.info(f"日期 {date_str}：read_S1_TOPS_SLC屏幕输出已保存至：{log_file_path}")
                    elif stat == -1:
                        # 状态码-1表示失败，主动抛出异常
                        raise RuntimeError(f"read_S1_TOPS_SLC执行失败（状态码：{stat}）")
                    else:
                        # 处理未知状态码（防止函数返回其他值）
                        raise RuntimeError(f"read_S1_TOPS_SLC返回未知状态码：{stat}（预期0/-1）")

                except Exception as e:
                    logger.error(f"日期 {date_str}：调用read_S1_TOPS_SLC.py时发生未知错误：{e}")
                    raise

        date_zip_map_keep = {}
        date_zip_map_delete = {}
        for date_str, zip_list in date_zip_map.items():
            if date_str != master_date:
                date_dir = os.path.join(ifgs_dir, date_str)
                master_burst_path = os.path.join(master_dir, f"{master_date}.burst_number_table")
                date_burst_path =os.path.join(date_dir, f"{date_str}.burst_number_table")

                master_burst = read_burst_number_table(master_burst_path)
                date_burst = read_burst_number_table(date_burst_path)

                if date_burst == master_burst:
                    date_zip_map_keep[date_str] = zip_list
                    logger.info(f"日期 {date_str}：burst_number_table匹配成功，保留该日期进行后续处理。")
                else:
                    date_zip_map_delete[date_str] = zip_list
                    logger.warning(f"日期 {date_str}：burst_number_table不匹配，排除该日期进行后续处理。")
                date_zip_map_keep[master_date] = date_zip_map[master_date]  # 确保主影像日期始终保留

        json_file_path = os.path.join(log_root, "date_keep.json")
        try:
            with open(json_file_path, 'w', encoding='utf-8') as f:
                # indent=4 格式化输出，便于手动查看；ensure_ascii=False 兼容中文（如果有）
                json.dump(date_zip_map_keep, f, ensure_ascii=False, indent=4)
            
            logger.info(f"date_zip_map_keep 已成功保存到：{json_file_path}")
        except Exception as e:
            logger.error(f"保存 date_zip_map_keep 失败：{str(e)}")
            raise  # 可选：保存失败时终止程序，根据需求调整

        json_file_path = os.path.join(log_root, "date_delete.json")
        try:
            with open(json_file_path, 'w', encoding='utf-8') as f:
                # indent=4 格式化输出，便于手动查看；ensure_ascii=False 兼容中文（如果有）
                json.dump(date_zip_map_delete, f, ensure_ascii=False, indent=4)
            
            logger.info(f"date_zip_map_delete 已成功保存到：{json_file_path}")
        except Exception as e:
            logger.error(f"保存 date_zip_map_delete 失败：{str(e)}")
            raise  # 可选：保存失败时终止程序，根据需求调整


        # 将YYYYMMDD格式的日期键排序
        sorted_dates = sorted(date_zip_map_keep.keys())
        master_idx = sorted_dates.index(master_date)  # 核心日期在有序列表中的索引

        # 向前处理（更早的日期：从master_idx-1到0，反转后从最近的向前日期开始）
        logger.info("===== 开始从主影像向前配准 =====")
        forward_dates = sorted_dates[:master_idx][::-1]  # 反转，确保从离核心最近的向前日期开始
        for i, date_str in enumerate(forward_dates):
            log_file_path = os.path.join(step4_log_dir, f"{date_str}_ScanSAR_coreg.log")
            date_dir = os.path.join(ifgs_dir, date_str)

            slc_file = glob.glob(os.path.join(date_dir, f"{date_str}.rslc"))
            if slc_file:
                logger.info(f"影像 {date_str} SLC → {slc_file[0]}已存在") 
                continue  # 影像已存在，跳过生成步骤
            
            if i == 0:
                # 第一个向前日期：特殊处理
                logger.info(f"【向前-特殊处理】日期 {date_str}")
                
                ## ScanSAR_coreg.py
                logger.info(f"ScanSAR_coreg.py 处理...")
                try:
                    with redirect_stdout_stderr(log_file_path):
                        print(f"========== ScanSAR_coreg.py 执行日志 ==========\n")
                        current_dir = os.getcwd()
                        os.chdir(date_dir)  # 切换到日期目录执行，确保输入输出在该目录
                        stat = pg.ScanSAR_coreg(
                                SLC1_tab = os.path.join(master_dir, f"{master_date}.vv.SLC_tab"),
                                SLC1_ID = master_date,
                                SLC2_tab = os.path.join(date_dir, f"{date_str}.vv.SLC_tab"),
                                SLC2_ID = date_str,
                                RSLC2_tab = os.path.join(date_dir, f"{date_str}.RSLC_tab"),
                                hgt = os.path.join(master_dir, f"{master_date}.hgt"),
                                rlks = range_looks,
                                azlks = azimuth_looks, 
                                )
                        os.chdir(current_dir)  # 恢复原始工作目录
                    # 根据返回状态码判断执行结果
                    if stat == 0:
                        logger.info(f"ScanSAR_coreg.py 处理成功（状态码：{stat}）")
                        if os.path.exists(os.path.join(date_dir, f"{date_str}.rslc")):
                            logger.info(f"ScanSAR_coreg.py 处理成功 → {os.path.join(date_dir, f'{date_str}.rslc')}")
                        else:
                            logger.warning(f"未检测到生成的 {os.path.join(date_dir, f'{date_str}.rslc')}！")
                        logger.info(f"ScanSAR_coreg.py {log_file_path}")
                    elif stat == -1:
                        # 状态码-1表示失败，主动抛出异常
                        raise RuntimeError(f"ScanSAR_coreg.py执行失败（状态码：{stat}）")
                    else:
                        # 处理未知状态码（防止函数返回其他值）
                        raise RuntimeError(f"ScanSAR_coreg.py返回未知状态码：{stat}（预期0/-1）")

                except Exception as e:
                    logger.error(f"调用ScanSAR_coreg.py时发生未知错误：{e}")
                    raise
                logger.info(f"rslc已生成至：{os.path.join(date_dir, f'{date_str}.rslc')}")

            else:
                # 其余向前日期：常规处理
                logger.info(f"【向前-常规处理】日期 {date_str}")
                prev_date_str = forward_dates[i-1]
                prev_date_dir = os.path.join(ifgs_dir, prev_date_str)
                
                ## ScanSAR_coreg.py
                logger.info(f"ScanSAR_coreg.py 处理...")
                try:
                    with redirect_stdout_stderr(log_file_path):
                        print(f"========== ScanSAR_coreg.py 执行日志 ==========\n")
                        current_dir = os.getcwd()
                        os.chdir(date_dir)  # 切换到日期目录执行，确保输入输出在该目录
                        stat = pg.ScanSAR_coreg(
                                SLC1_tab = os.path.join(master_dir, f"{master_date}.vv.SLC_tab"),
                                SLC1_ID = master_date,
                                SLC2_tab = os.path.join(date_dir, f"{date_str}.vv.SLC_tab"),
                                SLC2_ID = date_str,
                                RSLC2_tab = os.path.join(date_dir, f"{date_str}.RSLC_tab"),
                                hgt = os.path.join(master_dir, f"{master_date}.hgt"),
                                rlks = range_looks,
                                azlks = azimuth_looks, 
                                RSLC3_tab = os.path.join(prev_date_dir, f"{prev_date_str}.RSLC_tab"),
                                RSLC3_ID = prev_date_str,
                                )
                        os.chdir(current_dir)  # 恢复原始工作目录
                    # 根据返回状态码判断执行结果
                    if stat == 0:
                        logger.info(f"ScanSAR_coreg.py 处理成功（状态码：{stat}）")
                        if os.path.exists(os.path.join(date_dir, f"{date_str}.rslc")):
                            logger.info(f"ScanSAR_coreg.py 处理成功 → {os.path.join(date_dir, f'{date_str}.rslc')}")
                        else:
                            logger.warning(f"未检测到生成的 {os.path.join(date_dir, f'{date_str}.rslc')}！")
                        logger.info(f"ScanSAR_coreg.py {log_file_path}")
                    elif stat == -1:
                        # 状态码-1表示失败，主动抛出异常
                        raise RuntimeError(f"ScanSAR_coreg.py执行失败（状态码：{stat}）")
                    else:
                        # 处理未知状态码（防止函数返回其他值）
                        raise RuntimeError(f"ScanSAR_coreg.py返回未知状态码：{stat}（预期0/-1）")

                except Exception as e:
                    logger.error(f"调用ScanSAR_coreg.py时发生未知错误：{e}")
                    raise
                logger.info(f"rslc已生成至：{os.path.join(date_dir, f'{date_str}.rslc')}")


        # 向后处理（更晚的日期：从master_idx+1到最后）
        logger.info("===== 开始从主影像向后配准 =====")
        backward_dates = sorted_dates[master_idx+1:]
        for i, date_str in enumerate(backward_dates):
            log_file_path = os.path.join(step4_log_dir, f"{date_str}_ScanSAR_coreg.log")
            date_dir = os.path.join(ifgs_dir, date_str)
            slc_file = glob.glob(os.path.join(date_dir, f"{date_str}.rslc"))
            if slc_file:
                logger.info(f"影像 {date_str} SLC → {slc_file[0]}已存在") 
                continue  # 影像已存在，跳过生成步骤

            if i == 0:
                # 第一个向后日期：特殊处理
                logger.info(f"【向后-特殊处理】日期 {date_str}")
                
                ## ScanSAR_coreg.py
                logger.info(f"ScanSAR_coreg.py 处理...")
                try:
                    with redirect_stdout_stderr(log_file_path):
                        print(f"========== ScanSAR_coreg.py 执行日志 ==========\n")
                        current_dir = os.getcwd()
                        os.chdir(date_dir)  # 切换到日期目录执行，确保输入输出在该目录
                        stat = pg.ScanSAR_coreg(
                                SLC1_tab = os.path.join(master_dir, f"{master_date}.vv.SLC_tab"),
                                SLC1_ID = master_date,
                                SLC2_tab = os.path.join(date_dir, f"{date_str}.vv.SLC_tab"),
                                SLC2_ID = date_str,
                                RSLC2_tab = os.path.join(date_dir, f"{date_str}.RSLC_tab"),
                                hgt = os.path.join(master_dir, f"{master_date}.hgt"),
                                rlks = range_looks,
                                azlks = azimuth_looks, 
                                )
                        os.chdir(current_dir)  # 恢复原始工作目录
                    # 根据返回状态码判断执行结果
                    if stat == 0:
                        logger.info(f"ScanSAR_coreg.py 处理成功（状态码：{stat}）")
                        if os.path.exists(os.path.join(date_dir, f"{date_str}.rslc")):
                            logger.info(f"ScanSAR_coreg.py 处理成功 → {os.path.join(date_dir, f'{date_str}.rslc')}")
                        else:
                            logger.warning(f"未检测到生成的 {os.path.join(date_dir, f'{date_str}.rslc')}！")
                        logger.info(f"ScanSAR_coreg.py {log_file_path}")
                    elif stat == -1:
                        # 状态码-1表示失败，主动抛出异常
                        raise RuntimeError(f"ScanSAR_coreg.py执行失败（状态码：{stat}）")
                    else:
                        # 处理未知状态码（防止函数返回其他值）
                        raise RuntimeError(f"ScanSAR_coreg.py返回未知状态码：{stat}（预期0/-1）")

                except Exception as e:
                    logger.error(f"调用ScanSAR_coreg.py时发生未知错误：{e}")
                    raise
                logger.info(f"rslc已生成至：{os.path.join(date_dir, f'{date_str}.rslc')}")
            else:
                # 其余向后日期：常规处理
                logger.info(f"【向后-常规处理】日期 {date_str}")
                
                prev_date_str = backward_dates[i-1]
                prev_date_dir = os.path.join(ifgs_dir, prev_date_str)
                
                ## ScanSAR_coreg.py
                logger.info(f"ScanSAR_coreg.py 处理...")
                try:
                    with redirect_stdout_stderr(log_file_path):
                        print(f"========== ScanSAR_coreg.py 执行日志 ==========\n")
                        current_dir = os.getcwd()
                        os.chdir(date_dir)  # 切换到日期目录执行，确保输入输出在该目录
                        stat = pg.ScanSAR_coreg(
                                SLC1_tab = os.path.join(master_dir, f"{master_date}.vv.SLC_tab"),
                                SLC1_ID = master_date,
                                SLC2_tab = os.path.join(date_dir, f"{date_str}.vv.SLC_tab"),
                                SLC2_ID = date_str,
                                RSLC2_tab = os.path.join(date_dir, f"{date_str}.RSLC_tab"),
                                hgt = os.path.join(master_dir, f"{master_date}.hgt"),
                                rlks = range_looks,
                                azlks = azimuth_looks, 
                                RSLC3_tab = os.path.join(prev_date_dir, f"{prev_date_str}.RSLC_tab"),
                                RSLC3_ID = prev_date_str,
                                )
                        os.chdir(current_dir)  # 恢复原始工作目录
                    # 根据返回状态码判断执行结果
                    if stat == 0:
                        logger.info(f"ScanSAR_coreg.py 处理成功（状态码：{stat}）")
                        if os.path.exists(os.path.join(date_dir, f"{date_str}.rslc")):
                            logger.info(f"ScanSAR_coreg.py 处理成功 → {os.path.join(date_dir, f'{date_str}.rslc')}")
                        else:
                            logger.warning(f"未检测到生成的 {os.path.join(date_dir, f'{date_str}.rslc')}！")
                        logger.info(f"ScanSAR_coreg.py {log_file_path}")
                    elif stat == -1:
                        # 状态码-1表示失败，主动抛出异常
                        raise RuntimeError(f"ScanSAR_coreg.py执行失败（状态码：{stat}）")
                    else:
                        # 处理未知状态码（防止函数返回其他值）
                        raise RuntimeError(f"ScanSAR_coreg.py返回未知状态码：{stat}（预期0/-1）")

                except Exception as e:
                    logger.error(f"调用ScanSAR_coreg.py时发生未知错误：{e}")
                    raise
                logger.info(f"rslc已生成至：{os.path.join(date_dir, f'{date_str}.rslc')}")



    except KeyError as e:
        raise KeyError(f"配置文件缺少必要的键：{e}")
    except Exception as e:
        raise Exception(f"读取配置文件时发生错误：{e}")