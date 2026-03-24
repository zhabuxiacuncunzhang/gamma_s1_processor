#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import os
import sys
import subprocess
import logging
from logging.handlers import RotatingFileHandler
import glob
from contextlib import contextmanager
from gamma_s1_processor import load_config, setup_logger, redirect_stdout_stderr
from pathlib import Path
from osgeo import gdal

def create_symlink(src, dst, is_directory=False):
    """
    创建软链接的通用函数（兼容文件/目录）
    :param src: 源文件/目录路径
    :param dst: 软链接路径
    :param is_directory: 是否为目录（src是目录时设为True）
    :return: 成功返回True，失败返回False
    """
    # 转换为绝对路径（避免相对路径问题）
    src_abs = os.path.abspath(src)
    dst_abs = os.path.abspath(dst)
    
    # 检查源文件/目录是否存在
    if not os.path.exists(src_abs):
        print(f"错误：源路径 {src_abs} 不存在！")
        return False
    
    # 检查软链接是否已存在
    if os.path.exists(dst_abs):
        print(f"警告：软链接 {dst_abs} 已存在，跳过创建")
        return False
    
    try:
        # 方式1：使用 os.symlink（兼容低版本）
        # if sys.version_info >= (3, 8):
        #     os.symlink(src_abs, dst_abs, target_is_directory=is_directory)
        # else:
        #     os.symlink(src_abs, dst_abs)
        
        # 方式2：使用 pathlib（推荐，注释掉上面可切换）
        Path(dst_abs).symlink_to(src_abs, target_is_directory=is_directory)
        
        print(f"成功创建软链接：{dst_abs} -> {src_abs}")
        return True
    except PermissionError:
        print(f"错误：权限不足，无法创建软链接 {dst_abs}")
        return False
    except OSError as e:
        print(f"错误：创建软链接失败 - {e}")
        return False
    
def get_tiff_metadata(tif_path):
    """
    读取TIFF文件的元数据（宽、高、地理坐标、分辨率等）
    :param tif_path: TIFF文件路径
    :return: 元数据字典
    """
    try:
        dataset = gdal.Open(tif_path)
        if not dataset:
            raise Exception(f"无法打开TIFF文件: {tif_path}")
        
        # 获取基础尺寸
        width = dataset.RasterXSize
        file_length = dataset.RasterYSize
        
        # 获取地理变换参数 (x0, dx, 0, y0, 0, dy)
        geotrans = dataset.GetGeoTransform()
        x_first = geotrans[0]  # 左上角X坐标
        y_first = geotrans[3]  # 左上角Y坐标
        x_step = geotrans[1]   # X方向分辨率
        y_step = geotrans[5]   # Y方向分辨率
        
        # 关闭数据集
        dataset = None
        
        return {
            "WIDTH": width,
            "FILE_LENGTH": file_length,
            "X_FIRST": x_first,
            "Y_FIRST": y_first,
            "X_STEP": x_step,
            "Y_STEP": y_step
        }
    except Exception as e:
        print(f"读取TIFF元数据失败: {e}")
        return None


def generate_rsc_file(rsc_path, metadata, wavelength=0.055165, z_offset=0, z_scale=1.0):
    """
    生成指定格式的.rsc文件
    :param rsc_path: 输出.rsc文件路径
    :param metadata: TIFF元数据字典
    :param wavelength: 波长（固定值0.055165）
    :param z_offset: Z偏移（固定值0）
    :param z_scale: Z缩放（默认1.0，可根据需求调整）
    """
    # 格式化数值（保留足够小数位，匹配示例格式）
    rsc_content = f"""WIDTH {metadata['WIDTH']}
FILE_LENGTH {metadata['FILE_LENGTH']}
X_FIRST {metadata['X_FIRST']:.12f}
Y_FIRST {metadata['Y_FIRST']:.12f}
X_STEP {metadata['X_STEP']:.12f}
Y_STEP {metadata['Y_STEP']:.12f}
X_UNIT degres
Y_UNIT degres
WAVELENGTH {wavelength:.6f}
Z_OFFSET {z_offset}
Z_SCALE {z_scale}
PROJECTION LATLON
DATUM WGS84
"""
    # 写入文件
    try:
        with open(rsc_path, 'w', encoding='utf-8') as f:
            f.write(rsc_content)
        print(f"成功生成RSC文件: {rsc_path}")
    except Exception as e:
        print(f"生成RSC文件失败: {e}")


def tif2phs_with_rsc(tif_path, phs_output_path, rsc_output_path=None):
    """
    核心函数：TIFF转PHS（二进制） + 生成RSC文件
    :param tif_path: 输入TIFF文件路径
    :param phs_output_path: 输出PHS文件路径
    :param rsc_output_path: 输出RSC文件路径（默认和PHS同目录同前缀）
    """
    # 1. 读取TIFF元数据
    metadata = get_tiff_metadata(tif_path)
    if not metadata:
        return
    
    # 2. 自动补全RSC路径（如果未指定）
    if not rsc_output_path:
        rsc_output_path = phs_output_path + ".rsc"
    
    # 3. 调用gdal_translate转换TIFF为PHS（纯二进制）
    try:
        # gdal_translate命令：输出ENVI格式（纯二进制），32位浮点，取消缩放
        cmd = [
            "gdal_translate",
            "-of", "ENVI",          # 无表头的纯二进制格式
            "-ot", "Float32",       # 输出32位浮点（可根据TIFF数据类型调整为UInt16/Int32）
            "-unscale",             # 取消数据缩放，保留原始值
            "-q",                   # 静默模式（减少输出）
            tif_path,
            phs_output_path
        ]
        # 执行命令
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"成功转换TIFF为PHS: {phs_output_path}")
    except subprocess.CalledProcessError as e:
        print(f"gdal_translate执行失败: {e.stderr}")
        return
    except FileNotFoundError:
        print("错误：未找到gdal_translate命令，请确保GDAL已安装并配置环境变量")
        return
    
    # 4. 生成RSC文件
    generate_rsc_file(rsc_output_path, metadata)

def step1_generate_cfg(config):
    """
    步骤1：准备文件结构和cfg
    """
    logger = logging.getLogger('run_insarts')
    
    try:
        # 获取输出根目录
        output_root = config['OUTPUT']['output_root']
        insarts_work = "insarts"
        log_root = os.path.join(output_root, "LOGs")
        insarts_log_dir = os.path.join(log_root, "insarts")
        os.makedirs(insarts_log_dir, exist_ok=True)

        insarts_path = os.path.join(output_root, insarts_work)
        # 先检查目录是否存在，存在则抛异常
        try:
            if os.path.exists(insarts_path):
                raise FileExistsError(f"输出目录 {insarts_path} 已存在")
            # 目录不存在则创建
            os.makedirs(insarts_path, exist_ok=False)
            logger.info(f"成功创建目录：{insarts_path}")
        except FileExistsError as e:
            logger.warning(f"创建目录失败：{e}")

        dirs_to_create = ["data", "result"]
        
        # 遍历检查并创建目录
        for dir_name in dirs_to_create:
            dir_path = os.path.join(insarts_path, dir_name)
            # 先检查目录是否存在，存在则抛异常
            try:
                if os.path.exists(dir_path):
                    raise FileExistsError(f"输出目录 {dir_path} 已存在")
                # 目录不存在则创建
                os.makedirs(dir_path, exist_ok=False)
                logger.info(f"成功创建目录：{dir_path}")
            except FileExistsError as e:
                logger.warning(f"创建目录失败：{e}")
            continue

        logger.info("开始准备文件")

        ifgs_dir = os.path.join(output_root, "IFGs")
        log_root = os.path.join(output_root, "LOGs")

        if "pairs_file" not in config['PROCESSING']['sbas'] or not os.path.exists(config['PROCESSING']['sbas']['pairs_file']):
            logger.info("使用sbas_date_pairs.txt进行SBAS干涉处理...")
            pairs_file = os.path.join(log_root, "sbas_date_pairs.txt")
        else:
            logger.info(f"已找到有效的pairs_file配置，正在使用 {config['PROCESSING']['sbas']['pairs_file']} 进行SBAS干涉处理...")
            pairs_file = config['PROCESSING']['sbas']['pairs_file']

        create_symlink(os.path.join(log_root, "BASE", "baseline_for_insarts.txt"),
                       os.path.join(insarts_path, "base"))

        try:
        # 读取文件内容
            with open(pairs_file, 'r', encoding='utf-8') as f:
                intf_pair = f.readlines()
            logger.info(f"成功读取文件: {pairs_file}，共 {len(intf_pair)} 行内容")
        except FileNotFoundError:
            logger.error(f"文件不存在: {pairs_file}")
            return
        except Exception as e:
            logger.error(f"读取文件时发生错误: {str(e)}")
            return
        
        try:
            valid_pairs = []
            for intf_num, line in enumerate(intf_pair, 1):
                line = line.strip()
                # 跳过空行（增强鲁棒性）
                if not line:
                    logger.warning(f"第 {intf_num} 行是空行，跳过")
                    continue
                
                # ========== 核心修改：优先查找gacos版本，再找普通版本 ==========
                # 1. 先拼接gacos版本的路径
                gacos_unw_path = os.path.join(ifgs_dir, line, f"{line}.unw.geo.gacos.tif")
                # 2. 再拼接普通版本的路径
                normal_unw_path = os.path.join(ifgs_dir, line, f"{line}.unw.geo.tif")
                
                # 检查文件是否存在（优先gacos版本）
                gacos_unw_exists = os.path.exists(gacos_unw_path)
                normal_unw_exists = os.path.exists(normal_unw_path)
                
                # 确定最终使用的文件路径
                if gacos_unw_exists:
                    final_unw_path = gacos_unw_path
                    file_type = "unw.geo.gacos.tif（优先使用）"
                elif normal_unw_exists:
                    final_unw_path = normal_unw_path
                    file_type = "unw.geo.tif"
                else:
                    final_unw_path = None
                    file_type = None
                
                # 仅当找到有效文件时，加入有效列表
                if final_unw_path:
                    logger.info(f"第 {intf_num} 对 | {line} 文件正常（使用：{file_type}）")
                    # 存入有效列表时，同时记录最终使用的文件路径
                    valid_pairs.append((intf_num, line, final_unw_path))  
                else:
                    warning_msg = f"第 {intf_num} 对 | "
                    warning_msg += f"unw.geo.gacos.tif 和 unw.geo.tif 均不存在；"
                    warning_msg += f"检查路径：{gacos_unw_path} 或 {normal_unw_path}"
                    logger.warning(warning_msg)

            logger.info("生成 data 文件夹")
            # 复制干涉图等文件到 data 文件夹
            interf_dir = os.path.join(insarts_path, "data")
            # 确保data文件夹存在（新增：避免文件夹不存在报错）
            os.makedirs(interf_dir, exist_ok=True)
            file_list = os.path.join(insarts_path, "filelist")
            with open(file_list, "w", encoding="utf-8") as f:
                # ========== 读取有效列表时，取出最终的文件路径 ==========
                for intf_num, line, final_unw_path in valid_pairs:
                    line = line.strip()
                    phs_path = os.path.join(interf_dir, line + ".phs")
                    # 使用最终确定的unw路径转换为phs
                    tif2phs_with_rsc(final_unw_path, phs_path)
                    f.write(line + ".phs" + "\n")  # 新增换行符，避免路径粘连
        except Exception as e:
            logger.error(f"处理干涉对文件时出错：{str(e)}", exc_info=True)

            # 生成cfg文件
            cfg_name =  f"cfg"
            cfg_content = f"""
##############################
#Define filenames and foldernames
resultfolder   {os.path.join(insarts_path, "result")}
headerfile     {phs_path + ".rsc"}
filelist       {os.path.join(insarts_path, "filelist")}
datafolder     {os.path.join(insarts_path, "data")}
baselines      {os.path.join(insarts_path, "base")}
#
###############################
#Define switches
if_remove_orbit         1
if_remove_eds           0
if_check_loop_closure   1
if_inverse_time_series  1
if_stacking             1
if_apply_filter         5   #0:off; 1:spatial; 2:temporal; 3:temporal-spatial; 4:spatial-temporal; 5: 3D filter
#
###############################
#Define system related parameters
maxcap           500000
max_parallel_threads 8
#
#################################
#Define reference point/area
ref_method       2   # 0:arbitrarily; 1:global_mean; 2:min_std (slow)
ref_row          0
ref_col          0
ref_window       5      
#
################################
#Define interferogram quality control parameters
if_use_existing_corrected_ifg 1
orbit_quad_fit_sampling       4         
eds_block_size   50  #in km
demfile          /mnt/hpc2/processing/Chen_Yu/insar/dem_cut
demfile_header   /mnt/hpc2/processing/Chen_Yu/insar/dem_cut.rsc
if_keep_corrected_ifg 1
minimum_temporal_ifg_ratio 0.3
#
#############################
#Define phase closure checking parameters (unwrapping erros)   
if_use_existing_phasemask 1 
if_delete_bad_ifgs        1
whole_ifg_closure_std_threshold -1
loop_misclosure_threshold 3.14
loop_misclosure_ratio_threshold 0.5
loop_closure_mean_std_threshold -1
confidence_level 3.0
if_allow_unlooped_ifgs  1                 
if_allow_unlooped_pixel 1 
minimun_loop_num 0      
#
############################
#Definbe constant parameters
wavelength        0.055165 
incidence         39.276     
orbit_altitude    693000
#
############################
#Define time series inversion parameters
if_est_hgt                   1             
temporal_constraint          1  #0: no constraint; 1: linear; 2: a+b*ln(T-T0);
temporal_strength            1.0
simple_log_origin            19990921
############################
#Define simple stacking parameters
#
############################
#Define time series filter parameters
spatial_filter_method      0   
spatial_filter_sigma      -1
temporal_filter_method     0
temporal_filter_sigma     -1
#
"""
            cfg_path = os.path.join(insarts_path, cfg_name)
            with open(cfg_path, 'w', encoding='utf-8') as f:
                f.write(cfg_content)


        except Exception as e:
            # 仅捕获未知严重错误（如权限/路径非法），才抛出异常
            logger.error(f"查找文件时发生未知错误：{e}", exc_info=True)
            raise
  
    except ValueError as e:
        logger.error(f"配置参数/文件名解析失败：{e}")
        raise
    except PermissionError as e:
        # 补充权限异常捕获
        logger.error(f"创建目录失败：权限不足 - {e}")
        sys.exit(1)
    except Exception as e:
        # 捕获其他未知异常（如路径非法、磁盘满等）
        logger.error(f"发生未知错误：{e}", exc_info=True)
        sys.exit(1)


def step2_run_insarts(config):
    """
    步骤2：提示如何执行
    """
    logger = logging.getLogger('run_insarts')
    logger.info("======= 请执行insarts =======")
    logger.info("insarts ./cfg ")
    return

def main():
    """主函数：解析参数、加载配置、执行步骤"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Sentinel-1 insarts 处理程序')
    parser.add_argument('config_file', help='YAML配置文件路径（如gamma_s1_config.yml）')
    parser.add_argument('start', type=int, help='起始步骤（1-2）')
    parser.add_argument('end', type=int, help='结束步骤（1-2）')
    args = parser.parse_args()
    
    # 临时初始化基础日志（加载配置前）
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('run_insarts')
    
    # 校验步骤参数
    try:
        if args.start < 1 or args.end > 2 or args.start > args.end:
            raise ValueError(f"步骤参数无效！需满足 1≤start≤end≤2，当前：start={args.start}, end={args.end}")
    except ValueError as e:
        logger.error(e)
        sys.exit(1)
    
    # 加载配置
    config = load_config(args.config_file)
    
    # 重新初始化完整日志系统
    logger = setup_logger(config, filename = './run_insarts.log', name = 'run_insarts')
    logger.info("="*10)
    logger.info(f"启动 insarts 处理程序 | 执行步骤：{args.start} - {args.end}")
    logger.info("="*10)
    
    # 步骤映射
    steps = {
        1: step1_generate_cfg,
        2: step2_run_insarts,
    }
    
    # 执行指定步骤
    try:
        for step_num in range(args.start, args.end + 1):
            logger.info(f"---------- 开始执行步骤 {step_num} ----------")
            steps[step_num](config)
            logger.info(f"---------- 步骤 {step_num} 执行完成 ----------")
        
        logger.info("所有指定步骤执行完成！程序正常退出")
    except Exception as e:
        logger.critical(f"程序执行失败：{e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()