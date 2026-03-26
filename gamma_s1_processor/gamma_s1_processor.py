#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sentinel-1 数据 GAMMA 处理主程序
python gamma_s1_processor.py gamma_s1_config.yml start_step end_step
python gamma_s1_processor.py gamma_s1_config.yml 1 1
python gamma_s1_processor.py gamma_s1_config.yml 1 5
"""

import configparser
import argparse
import os
import sys
import subprocess
import yaml
import logging
import time
import glob
import shutil
import json

from logging.handlers import RotatingFileHandler
from functools import wraps
from PIL import Image
from contextlib import contextmanager
import matplotlib.pyplot as plt
import py_gamma as pg


from .s1_auto_bin import s1_coregister as step4_coreg
from .s1_auto_bin import s1_pair as step4_pair
from .s1_auto_bin import s1_base as step4_base
from .s1_auto_bin import s1_intf as step5_intf
from .s1_auto_bin import plot_IW_kml_enhance as plot_kml

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

def setup_logger(config, filename = './gamma_s1_process.log', name = 'gamma_s1_processor'):
    """
    配置日志系统：控制台+文件双输出，按大小轮转
    """
    log_path = config.get('OUTPUT', {}).get('log_path', filename)
    log_dir = os.path.dirname(log_path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    log_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    # 防止重复添加 handler 导致重复日志输出
    if logger.handlers:
        # 清除已有 handlers（例如来自 basicConfig 或上次调用），避免重复输出
        logger.handlers.clear()
    logger.propagate = False

    # 控制台处理器（INFO级别）
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_format)
    
    # 文件处理器（DEBUG级别，50MB/个，保留5个备份）
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=50*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(log_format)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def load_config(config_file):
    """
    加载YAML配置文件
    """
    logger = logging.getLogger('gamma_s1_processor')
    try:
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"配置文件 {config_file} 不存在")

        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}

        # 验证关键配置项（适配不同配置命名）
        required_sections = ['GAMMA_PATH', 'PROCESSING', 'OUTPUT']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"配置文件缺少关键节点：{section}")

        # 规范化并检查各个路径
        gamma = config.get('GAMMA_PATH', {})
        proc = config.get('PROCESSING', {})
        out = config.get('OUTPUT', {})

        # 常见路径字段
        bin_dir = os.path.expanduser(str(gamma.get('bin_dir', ''))).strip()
        orbit_dir = os.path.expanduser(str(gamma.get('orbit_dir', ''))).strip()
        rawdata_dir = os.path.expanduser(str(proc.get('rawdata_dir', ''))).strip()
        kml_path = os.path.expanduser(str(proc.get('kml_path', ''))).strip()
        dem_path = os.path.expanduser(str(proc.get('dem_path', ''))).strip()
        output_root = os.path.expanduser(str(out.get('output_root', './output'))).strip()

        # 把规范化路径写回config，方便其它代码使用绝对路径
        gamma['bin_dir'] = os.path.abspath(bin_dir) if bin_dir else ''
        gamma['orbit_dir'] = os.path.abspath(orbit_dir) if orbit_dir else ''
        proc['rawdata_dir'] = os.path.abspath(rawdata_dir) if rawdata_dir else ''
        proc['kml_path'] = os.path.abspath(kml_path) if kml_path else ''
        proc['dem_path'] = os.path.abspath(dem_path) if dem_path else ''
        out['output_root'] = os.path.abspath(output_root)

        # 检查存在性：目录必须存在（除output_root可创建），DEM必须是文件
        dir_checks = [
            ('GAMMA bin_dir', gamma.get('bin_dir')),
            ('GAMMA orbit_dir', gamma.get('orbit_dir')),
            ('rawdata_dir', proc.get('rawdata_dir')),
        ]
        for name, p in dir_checks:
            if not p:
                raise ValueError(f"配置中缺少路径字段：{name}")
            if not os.path.exists(p):
                raise FileNotFoundError(f"配置中指定的路径不存在：{name} -> {p}")
            if not os.path.isdir(p):
                raise NotADirectoryError(f"预期为目录，但不是目录：{name} -> {p}")

        # 检查KML（可为文件）
        if proc.get('kml_path'):
            if not os.path.exists(proc['kml_path']):
                raise FileNotFoundError(f"KML 文件不存在：{proc['kml_path']}")

        # 检查DEM文件存在
        if not proc.get('dem_path'):
            raise ValueError("配置中未指定 dem_path")
        if not os.path.exists(proc['dem_path']):
            raise FileNotFoundError(f"DEM 文件不存在：{proc['dem_path']}")
        if not os.path.isfile(proc['dem_path'] + '.par'):
            raise FileNotFoundError(f"DEM par文件不存在{proc['dem_path'] + '.par'}")

        # 如果输出目录不存在则创建
        if not os.path.exists(out['output_root']):
            os.makedirs(out['output_root'], exist_ok=True)
            logger.info(f"已创建输出目录：{out['output_root']}")

        # 检查 rawdata 中是否有 Sentinel-1 数据
        s1_globs = [
            os.path.join(proc['rawdata_dir'], 'S1*', '*.zip'),
            os.path.join(proc['rawdata_dir'], '*.zip'),
        ]
        s1_files = []
        for pattern in s1_globs:
            s1_files.extend(glob.glob(pattern))
        # 去重并规范为绝对路径
        s1_files = list({os.path.abspath(p) for p in s1_files})
        if not s1_files:
            raise FileNotFoundError(f"原始数据目录 {proc['rawdata_dir']} 未找到 Sentinel-1 数据文件（.zip/.SAFE，支持S1*/子目录）")
        logger.info(f"检测到 {len(s1_files)} 个 Sentinel-1 原始数据文件或包")

        # 写回修改后的字典
        config['GAMMA_PATH'] = gamma
        config['PROCESSING'] = proc
        config['OUTPUT'] = out

        logger.info(f"成功加载并验证配置文件：{config_file}")
        return config
    
    except FileNotFoundError as e:
        logger.error(f"配置文件加载失败：{e}")
        sys.exit(1)
    except yaml.YAMLError as e:
        logger.error(f"YAML解析错误：{e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"配置文件内容错误：{e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"加载配置异常：{e}", exc_info=True)
        sys.exit(1)

# 定义排序函数：从文件路径中提取完整时间戳作为排序key
def get_timestamp_from_path(file_path):
    """从文件路径中解析完整时间戳（如20201231T225656）"""
    filename = os.path.basename(file_path)
    parts = filename.split('_')
    return parts[5]  # 返回完整时间戳，用于排序

def step1_plot_kml(config):
    """
    步骤1：绘制所有数据范围图片
    """
    logger = logging.getLogger('gamma_s1_processor')
    
    try:
        # 获取输出根目录
        output_root = config['OUTPUT']['output_root']
        kml_path = config['PROCESSING']['kml_path']
        orbit_dir = config['GAMMA_PATH']['orbit_dir']
        bin_dir = config['GAMMA_PATH']['bin_dir']
        # 定义需要创建的目录列表（便于扩展）
        dirs_to_create = ["IFGs", "SLCs", "LOGs"]
        
        # 遍历检查并创建目录
        for dir_name in dirs_to_create:
            dir_path = os.path.join(output_root, dir_name)
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

        # 根据rawdata中的文件，在SLC文件夹中创建每个日期的目录，以及对应的ziplist
        raw_data_dir = config['PROCESSING']['rawdata_dir']
        slc_dir = os.path.join(output_root, "SLCs")
        log_root = os.path.join(output_root, "LOGs")
        step1_log_dir = os.path.join(log_root, "step1")
        os.makedirs(step1_log_dir, exist_ok=True)

        orbit_flag = config['GAMMA_PATH']['orbit_update_method']
        if orbit_flag == "auto":
            program_cmd = [
                "eof",
                "--search-path", raw_data_dir,
                "--save-dir", orbit_dir,
                "--force-asf"
            ]
            # 直接通过subprocess配置输出重定向和后台运行
            with open(f"{step1_log_dir}/orbit_update.log", 'a') as log_file:
                subprocess.Popen(
                    program_cmd,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,  # 把stderr也重定向到日志
                    shell=False,  # 列表形式命令无需shell=True，更安全
                    preexec_fn=os.setsid  # 脱离父进程，后台运行
                )

        s1_zip_pattern = os.path.join(raw_data_dir, "S1*_IW_SLC__*.zip")
        s1_zip_files = glob.glob(s1_zip_pattern)

        date_zip_map = {}
        
        # 解析每个压缩包的文件名，提取日期
        for zip_file in s1_zip_files:
            # 获取纯文件名（去掉路径）
            zip_filename = os.path.basename(zip_file)
            # 按下划线分割文件名，提取日期段（格式：20201231T225656）
            # 文件名结构：S1A_IW_SLC__1SDV_20201231T225656_20201231T225723_035936_04358E_2730.zip
            parts = zip_filename.split('_')
            if len(parts) < 6 or 'T' not in parts[5]:
                raise ValueError(f"文件名格式异常，无法解析日期：{zip_filename}")
            
            # 提取日期（只保留年月日，如20201231）
            date_str = parts[5].split('T')[0]  # 20201231T225656 → 20201231
            # 补全路径（方便后续调用）
            abs_zip_path = os.path.abspath(zip_file)
            
            # 加入日期映射字典
            if date_str not in date_zip_map:
                date_zip_map[date_str] = []
            date_zip_map[date_str].append(abs_zip_path)

        # 对每个日期下的文件列表按时间戳排序（不改变字典结构）
        for date_str in date_zip_map:
            # 按完整时间戳升序排序（时间从早到晚）
            date_zip_map[date_str].sort(key=get_timestamp_from_path)

        json_file_path = os.path.join(log_root, "date.json")
        try:
            with open(json_file_path, 'w', encoding='utf-8') as f:
                # indent=4 格式化输出，便于手动查看；ensure_ascii=False 兼容中文（如果有）
                json.dump(date_zip_map, f, ensure_ascii=False, indent=4)
            
            logger.info(f"date_zip_map 已成功保存到：{json_file_path}")
        except Exception as e:
            logger.error(f"保存 date_zip_map 失败：{str(e)}")
            raise  # 可选：保存失败时终止程序，根据需求调整

        # 按日期创建子目录，并生成ziplist文件
        logger.info("开始按日期创建SLC子目录并生成ziplist...")
        for date_str, zip_list in date_zip_map.items():
            # 创建日期子目录（如SLCs/20201231）
            date_dir = os.path.join(slc_dir, date_str)
            os.makedirs(date_dir, exist_ok=True)
            
            # 定义该日期的日志文件路径（如：LOGs/step1/20201231_read_S1_TOPS_SLC.log）
            log_file_path = os.path.join(step1_log_dir, f"{date_str}_read_S1_TOPS_SLC.log")
            # 生成ziplist文件（记录该日期的所有压缩包路径）
            ziplist_path = os.path.join(date_dir, "ziplist.txt")
            # 写入ziplist（每行一个压缩包路径）
            with open(ziplist_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(zip_list))
            
            logger.info(f"日期 {date_str}：创建目录 {date_dir} | 生成ziplist {ziplist_path} | 包含 {len(zip_list)} 个文件")
        
            # 利用read_S1_TOPS_SLC.py脚本得到每个日期的kml
            logger.info(f"开始为日期 {date_str} 调用read_S1_TOPS_SLC.py生成KML...")
            kml_files = glob.glob(os.path.join(date_dir, f"{date_str}*.kml"))
            png_file = glob.glob(os.path.join(slc_dir, f"{date_str}.png"))
            if kml_files and png_file:
                logger.info(f"日期 {date_str} SLC → {kml_files[:2]}已存在") 
                logger.info(f"日期 {date_str} PNG → {png_file[0]}已存在") 
                continue  # 跳过已存在的SLC文件和PNG图片，避免重复处理

            try:
                with redirect_stdout_stderr(log_file_path):
                    print(f"========== {date_str} read_S1_TOPS_SLC 执行日志 ==========\n")
                    stat = pg.read_S1_TOPS_SLC(
                        input = ziplist_path,
                        root_name = date_str, 
                        pol = 'VV',
                        # burst_sel = kml_path,
                        OPOD_dir = orbit_dir,
                        out_dir = date_dir,
                        kml = True,
                        no_binary = True)
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
        
            # 根据kml，利用plot_IW_kml.bash绘制png图片，保存到SLC文件夹中
            logger.info(f"开始为日期 {date_str} 生成PNG图片...")

            # 定义生成的PNG临时名称（脚本默认输出名）和目标名称
            target_png_name = f"{date_str}.png"    
            try:
                current_dir = os.getcwd()
                os.chdir(date_dir)  # 切换到日期目录执行，确保输入输出在该目录
                plot_kml.main(files=[kml_path],output="../")
                os.chdir(current_dir)  # 恢复原始工作目录
                # 根据返回状态码判断执行结果
                if not os.path.exists(os.path.join(slc_dir, target_png_name)):
                    raise FileNotFoundError(f"未生成PNG文件：{target_png_name}")
                logger.info(f"日期 {date_str}：成功生成PNG文件 → {target_png_name}")                    
            except FileNotFoundError as e:
                logger.error(f"日期 {date_str}：绘图相关文件不存在：{e}")
                raise
            except subprocess.SubprocessError as e:
                logger.error(f"日期 {date_str}：绘图时出错：{e}")
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

def step2_generate_master_image(config):
    """
    步骤2：生成主影像（GAMMA主影像处理）
    """
    logger = logging.getLogger('gamma_s1_processor')
    
    try:
        # 获取配置参数
        bin_dir = config['GAMMA_PATH']['bin_dir']
        range_looks = config['PROCESSING']['multilook']['range_looks']
        azimuth_looks = config['PROCESSING']['multilook']['azimuth_looks']
        orbit_dir = config['GAMMA_PATH']['orbit_dir']
        output_root = config['OUTPUT']['output_root']
        log_root = os.path.join(output_root, "LOGs")
        date_file_path = os.path.join(log_root, "date.json")
        step2_log_dir = os.path.join(log_root, "step2")
        os.makedirs(step2_log_dir, exist_ok=True)

        # GAMMA主影像生成核心命令（示例，需根据实际GAMMA工具调整）
        logger.info("开始生成主影像...")
        
        output_root = config['OUTPUT']['output_root']
        kml_path = config['PROCESSING']['kml_path']
        orbit_dir = config['GAMMA_PATH']['orbit_dir']
        bin_dir = config['GAMMA_PATH']['bin_dir']
        ifgs_dir = os.path.join(output_root, "IFGs")
        master_date = str(config['PROCESSING']['common_master_date'])
        master_dir = os.path.join(ifgs_dir, master_date)
        # print (f"主影像日期：{master_date}")

        slc_file = glob.glob(os.path.join(master_dir, f"{master_date}.slc"))
        if slc_file:
            logger.info(f"主影像 {master_date} SLC → {slc_file[0]}已存在") 
            return  # 主影像已存在，跳过生成步骤
        
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

        try:
            zip_list = date_zip_map[master_date]
            date_dir = os.path.join(ifgs_dir, master_date)
            os.makedirs(date_dir, exist_ok=True)
            
            # 定义该日期的日志文件路径（如：LOGs/step2/20201231_read_S1_TOPS_SLC.log）
            log_file_path = os.path.join(step2_log_dir, f"{master_date}_read_S1_TOPS_SLC.log")
            # 生成ziplist文件（记录该日期的所有压缩包路径）
            ziplist_path = os.path.join(date_dir, "ziplist.txt")
            # 写入ziplist（每行一个压缩包路径）
            with open(ziplist_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(zip_list))
            
            logger.info(f"主影像 {master_date}：创建目录 {date_dir} | 生成ziplist {ziplist_path} | 包含 {len(zip_list)} 个文件")
        

            # 利用read_S1_TOPS_SLC.py脚本读取SLC
            logger.info(f"读取 {master_date} 影像...")
            try:
                with redirect_stdout_stderr(log_file_path):
                    print(f"========== {master_date} read_S1_TOPS_SLC 执行日志 ==========\n")
                    stat = pg.read_S1_TOPS_SLC(
                        input = ziplist_path,
                        root_name = master_date, 
                        pol = 'VV',
                        burst_sel = kml_path,
                        OPOD_dir = orbit_dir,
                        out_dir = date_dir,
                        kml = True,)
                # 根据返回状态码判断执行结果
                if stat == 0:
                    logger.info(f"读取 {master_date}影像成功（状态码：{stat}）")
                    # 验证KML文件是否生成（可选，增强鲁棒性）
                    kml_files = glob.glob(os.path.join(date_dir, f"{master_date}*.kml"))
                    if kml_files:
                        logger.info(f"日期 {master_date}：成功生成 {len(kml_files)} 个KML文件 → {kml_files[:2]}...") 
                    else:
                        logger.warning(f"日期 {master_date}：未检测到生成的KML文件！")
                    logger.info(f"日期 {master_date}：read_S1_TOPS_SLC屏幕输出已保存至：{log_file_path}")
                elif stat == -1:
                    # 状态码-1表示失败，主动抛出异常
                    raise RuntimeError(f"read_S1_TOPS_SLC执行失败（状态码：{stat}）")
                else:
                    # 处理未知状态码（防止函数返回其他值）
                    raise RuntimeError(f"read_S1_TOPS_SLC返回未知状态码：{stat}（预期0/-1）")

            except Exception as e:
                logger.error(f"日期 {master_date}：调用read_S1_TOPS_SLC.py时发生未知错误：{e}")
                raise
        
            # 根据kml，利用plot_IW_kml.bash绘制png图片，保存到SLC文件夹中
            logger.info(f"开始为日期 {master_date} 生成PNG图片...")

            # 复制bash脚本到日期目录
            target_png_name = f"{master_date}.png"
            
            try:
                current_dir = os.getcwd()
                os.chdir(date_dir)  # 切换到日期目录执行，确保输入输出在该目录
                plot_kml.main(files=[kml_path],output="./")
                os.chdir(current_dir)  # 恢复原始工作目录
                # 根据返回状态码判断执行结果
                if not os.path.exists(os.path.join(master_dir, target_png_name)):
                    raise FileNotFoundError(f"未生成PNG文件：{target_png_name}")
                logger.info(f"日期 {master_date}：成功生成PNG文件 → {target_png_name}")                    
            except FileNotFoundError as e:
                logger.error(f"日期 {master_date}：绘图相关文件不存在：{e}")
                raise
            except subprocess.SubprocessError as e:
                logger.error(f"日期 {master_date}：绘图时出错：{e}")
                raise
            
            ## SLC_mosaic_ScanSAR
            logger.info(f"mosaic {master_date} 影像...")
            try:
                with redirect_stdout_stderr(log_file_path):
                    print(f"========== {master_date} SLC_mosaic_ScanSAR 执行日志 ==========\n")
                    current_dir = os.getcwd()
                    os.chdir(date_dir)  # 切换到日期目录执行，确保输入输出在该目录
                    stat = pg.SLC_mosaic_ScanSAR(
                        SLC_tab = master_date + ".vv.SLC_tab",
                        SLC = master_date + ".slc", 
                        SLC_par = master_date + ".slc.par",
                        rlks = 1,
                        azlks = 1,)
                    os.chdir(current_dir)  # 恢复原始工作目录
                # 根据返回状态码判断执行结果
                if stat == 0:
                    logger.info(f"mosaic {master_date}影像成功（状态码：{stat}）")
                    if os.path.exists(os.path.join(master_dir, f"{master_date}.slc")):
                        logger.info(f"日期 {master_date}：成功生成主影像文件 → {os.path.join(master_dir, f'{master_date}.slc')}")
                    else:
                        logger.warning(f"日期 {master_date}：未检测到生成的主影像文件！")
                    logger.info(f"日期 {master_date}：SLC_mosaic_ScanSAR屏幕输出已保存至：{log_file_path}")
                elif stat == -1:
                    # 状态码-1表示失败，主动抛出异常
                    raise RuntimeError(f"SLC_mosaic_ScanSAR执行失败（状态码：{stat}）")
                else:
                    # 处理未知状态码（防止函数返回其他值）
                    raise RuntimeError(f"SLC_mosaic_ScanSAR返回未知状态码：{stat}（预期0/-1）")

            except Exception as e:
                logger.error(f"日期 {master_date}：调用SLC_mosaic_ScanSAR时发生未知错误：{e}")
                raise  

            ## mulit_look2
            logger.info(f"multi_look {master_date} 影像...")
            try:
                with redirect_stdout_stderr(log_file_path):
                    print(f"========== {master_date} multi_look 执行日志 ==========\n")
                    current_dir = os.getcwd()
                    os.chdir(date_dir)  # 切换到日期目录执行，确保输入输出在该目录
                    stat = pg.multi_look2(
                        SLC = master_date + ".slc",
                        SLC_par = master_date + ".slc.par", 
                        MLI = master_date + ".mli",
                        MLI_par = master_date + ".mli.par",
                        r_dec = range_looks,
                        az_dec = azimuth_looks,)
                    os.chdir(current_dir)  # 恢复原始工作目录
                # 根据返回状态码判断执行结果
                if stat == 0:
                    logger.info(f"multi_look {master_date}影像成功（状态码：{stat}）")
                    if os.path.exists(os.path.join(master_dir, f"{master_date}.mli")):
                        logger.info(f"日期 {master_date}：成功生成主影像文件 → {os.path.join(master_dir, f'{master_date}.mli')}")
                    else:
                        logger.warning(f"日期 {master_date}：未检测到生成的主影像文件！")
                    logger.info(f"日期 {master_date}multi_look{log_file_path}")
                elif stat == -1:
                    # 状态码-1表示失败，主动抛出异常
                    raise RuntimeError(f"multi_look执行失败（状态码：{stat}）")
                else:
                    # 处理未知状态码（防止函数返回其他值）
                    raise RuntimeError(f"multi_look返回未知状态码：{stat}（预期0/-1）")

            except Exception as e:
                logger.error(f"日期 {master_date}：调用multi_look时发生未知错误：{e}")
                raise

            ## raspwr
            logger.info(f"raspwr {master_date} 影像...")
            try:
                with redirect_stdout_stderr(log_file_path):
                    print(f"========== {master_date} raspwr 执行日志 ==========\n")
                    current_dir = os.getcwd()
                    os.chdir(date_dir)  # 切换到日期目录执行，确保输入输出在该目录
                    master_mli_param = pg.ParFile(master_date + ".mli.par")
                    stat = pg.raspwr(
                        data = master_date + ".mli",
                        width = master_mli_param.get_value("range_samples"),)
                    os.chdir(current_dir)  # 恢复原始工作目录
                # 根据返回状态码判断执行结果
                if stat == 0:
                    logger.info(f"raspwr {master_date}mli影像成功（状态码：{stat}）")
                    if os.path.exists(os.path.join(master_dir, f"{master_date}.mli")):
                        logger.info(f"日期 {master_date}：成功生成主影像文件 → {os.path.join(master_dir, f'{master_date}.mli.bmp')}")
                    else:
                        logger.warning(f"日期 {master_date}：未检测到生成的主影像文件 {os.path.join(master_dir, f'{master_date}.mli.bmp')}！")
                    logger.info(f"日期 {master_date}raspwr{log_file_path}")
                elif stat == -1:
                    # 状态码-1表示失败，主动抛出异常
                    raise RuntimeError(f"raspwr执行失败（状态码：{stat}）")
                else:
                    # 处理未知状态码（防止函数返回其他值）
                    raise RuntimeError(f"raspwr返回未知状态码：{stat}（预期0/-1）")

            except Exception as e:
                logger.error(f"日期 {master_date}：调用raspwr时发生未知错误：{e}")
                raise



        except KeyError:
            logger.error(f"主影像日期 {master_date} 在 date_zip_map 中未找到对应的压缩包列表！请检查 date.json 文件内容。")
            raise    

        logger.info(f"主影像已生成至：{master_dir}")
        logger.info("步骤2执行完成！")
        
    except Exception as e:
        logger.error(f"步骤2执行异常：{e}", exc_info=True)
        raise

def step3_generate_cropped_map(config):
    """
    步骤3：生成裁剪后的map
    """
    logger = logging.getLogger('gamma_s1_processor')
    
    try:
        # 获取配置参数
        dem_ovr = config['PROCESSING']['geocode']['dem_ovr']
        output_root = config['OUTPUT']['output_root']
        log_root = os.path.join(output_root, "LOGs")
        step3_log_dir = os.path.join(log_root, "step3")
        os.makedirs(step3_log_dir, exist_ok=True)

        # GAMMA地理编码生成核心命令
        logger.info("开始地理编码文件...")
        
        output_root = config['OUTPUT']['output_root']
        ifgs_dir = os.path.join(output_root, "IFGs")
        master_date = str(config['PROCESSING']['common_master_date'])
        master_dir = os.path.join(ifgs_dir, master_date)
        dem = config['PROCESSING']['dem_path']
        dem_par = config['PROCESSING']['dem_path'] + ".par"
        mli = os.path.join(master_dir, f"{master_date}.mli")
        mli_par = os.path.join(master_dir, f"{master_date}.mli.par")
        if not os.path.exists(mli_par):
            raise FileNotFoundError(f"主影像多视处理参数文件不存在：{mli_par}\n"+
                                    "检查第二步是否正确")
        log_file_path = os.path.join(step3_log_dir, f"{master_date}_geocode.log")

        lt_file = glob.glob(os.path.join(master_dir, f"dem_seg.{master_date}.lt_fine"))
        if lt_file:
            logger.info(f"lookup_table_fine → {lt_file[0]}已存在") 
            return  # 主影像已存在，跳过生成步骤

        ## geocoding.py
        logger.info(f"geocoding.py 处理...")
        try:
            with redirect_stdout_stderr(log_file_path):
                print(f"========== geocoding.py 执行日志 ==========\n")
                current_dir = os.getcwd()
                os.chdir(master_dir)  # 切换到日期目录执行，确保输入输出在该目录
                stat = pg.geocoding(
                        MLI = mli,
                        MLI_par = mli_par,
                        DEM = dem,
                        DEM_par = dem_par,
                        root_name = master_date,
                        seg = "dem_seg",
                        lat_ovr = dem_ovr,
                        lon_ovr = dem_ovr, 
                        )
                os.chdir(current_dir)  # 恢复原始工作目录
            # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"geocoding.py 处理成功（状态码：{stat}）")
                if os.path.exists(os.path.join(master_dir, f"dem_seg.{master_date}.lt_fine")):
                    logger.info(f"geocoding.py 处理成功 → {os.path.join(master_dir, f'dem_seg.{master_date}.lt_fine')}")
                else:
                    logger.warning(f"未检测到生成的lookup_table {os.path.join(master_dir, f'dem_seg.{master_date}.lt_fine')}！")
                logger.info(f"geocoding.py {log_file_path}")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"geocoding.py执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"geocoding.py返回未知状态码：{stat}（预期0/-1）")

        except Exception as e:
            logger.error(f"调用geocoding.py时发生未知错误：{e}")
            raise
        logger.info(f"lookup_table已生成至：{os.path.join(master_dir, f'dem_seg.{master_date}.lt_fine')}")

        '''
        dem_seg_width = pg.ParFile(os.path.join(master_dir, f"dem_seg.dem_par")).get_value("width")
        mli_width = pg.ParFile(mli_par).get_value("range_samples")
        lt_fine_path = os.path.join(master_dir, f"dem_seg.{master_date}.lt_fine")
        '''
        logger.info("步骤3执行完成！")
        
    except FileNotFoundError as e:
        logger.error(f"多视参数文件不存在：{e}")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"GAMMA裁剪命令执行失败：{e.stderr}")
        raise
    except Exception as e:
        logger.error(f"步骤3执行异常：{e}", exc_info=True)
        raise

def step4_coregister(config):
    """
    步骤4：配准 + 生成干涉网络
    """
    logger = logging.getLogger('gamma_s1_processor')
    
    try:
        # 获取配置参数
        output_root = config['OUTPUT']['output_root']
        ifgs_dir = os.path.join(output_root, "IFGs")
        master_date = str(config['PROCESSING']['common_master_date'])
        master_dir = os.path.join(ifgs_dir, master_date)
        output_root = config['OUTPUT']['output_root']
        log_root = os.path.join(output_root, "LOGs")
        step4_log_dir = os.path.join(log_root, "step4")
        os.makedirs(step4_log_dir, exist_ok=True)

        lookup_table_fine = os.path.join(master_dir, f'dem_seg.{master_date}.lt_fine')
        if not os.path.exists(lookup_table_fine):
            raise FileNotFoundError(f"geocode文件不存在：{lookup_table_fine}\n"+
                                    "检查第三步是否正确")
        
      
        logger.info("开始执行配准操作...")
        
        # 配准
        step4_coreg.coregister_s1_images(config) 

        # 生成干涉网络
        date_file_path = os.path.join(log_root, "date_keep.json")
        try:
            with open(date_file_path, 'r', encoding='utf-8') as f:
                date_keep = json.load(f)  # 直接得到字典变量
                logger.info(f"成功加载 date_keep，从 {date_file_path} 中读取到 {len(date_keep)} 个日期条目")

            if master_date not in date_keep.keys():
                raise ValueError(f"主影像日期 {master_date} 不在 date_keep 中！请检查配置的 common_master_date 是否正确，或确认 date.json 中包含该日期。")

        except FileNotFoundError:
            logger.error(f"未找到文件：{date_file_path}")
            date_keep = {}  # 兜底：返回空字典
        except ValueError as e:
            logger.error(f"主影像日期错误：{e}")
            raise

        # 如果未指定pair文件或者文件未找到
        if "pairs_file" not in config['PROCESSING']['sbas'] or not os.path.exists(config['PROCESSING']['sbas']['pairs_file']):
            logger.info("未找到有效的pairs_file配置，正在自动生成干涉对列表...")
            log_file_path = os.path.join(step4_log_dir, f"make_sbas_date_pairs.log")
            with redirect_stdout_stderr(log_file_path):
                step4_pair.make_sbas_date_pairs(config, date_keep)
                pairs_file = os.path.join(log_root, "sbas_date_pairs.txt")
        else:
            logger.info(f"已找到有效的pairs_file配置，正在使用 {config['PROCESSING']['sbas']['pairs_file']} 生成干涉对列表...")
            log_file_path = os.path.join(step4_log_dir, f"make_sbas_date_pairs.log")
            with redirect_stdout_stderr(log_file_path):
                pairs_file = config['PROCESSING']['sbas']['pairs_file']
        
        # 计算时空基线，绘制时空基线
        step4_base.calc_base(config, pairs_file, date_keep)
        
        
        logger.info(f"干涉网络已生成")
        logger.info("步骤4执行完成！")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"GAMMA配准/SBAS命令执行失败：{e.stderr}")
        raise
    except Exception as e:
        logger.error(f"步骤4执行异常：{e}", exc_info=True)
        raise

def step5_generate_interferograms(config):
    """
    步骤5：生成干涉图
    """
    logger = logging.getLogger('gamma_s1_processor')
    
    try:
        # 获取配置参数
        bin_dir = config['GAMMA_PATH']['bin_dir']
        output_root = config['OUTPUT']['output_root']
        ifgs_dir = os.path.join(output_root, "IFGs")
     
        logger.info("开始生成干涉图...")
        
        # 干涉图生成
        step5_intf.make_interferogram(config)
        
        logger.info(f"干涉图已生成至：{ifgs_dir}")
        logger.info("步骤5执行完成！")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"GAMMA 干涉图命令执行失败：{e.stderr}")
        raise
    except Exception as e:
        logger.error(f"步骤5执行异常：{e}", exc_info=True)
        raise

def generate_config():
    config_text = """
# GAMMA 软件及数据路径配置
GAMMA_PATH:
  bin_dir: "/mnt/e/TEMP/S1_auto/s1_auto_bin/"
  orbit_update_method: "auto"   # local/auto
  orbit_dir: "/mnt/e/TEMP/S1_auto/orbits/"
  
# 处理核心参数
PROCESSING:
  rawdata_dir: "/mnt/e/TEMP/S1_auto/rawdata"
  kml_path: "/mnt/e/TEMP/S1_auto/poly.kml"
  dem_path: "/mnt/e/TEMP/S1_auto/dem/dem"
  common_master_date: 20201219

  multilook:
    range_looks: 10
    azimuth_looks: 2

  sbas:
    pairs_file: ''
    if_winter_only: 
      option: False
      winter_start: 11
      winter_end: 3
    if_year_pair: 
      option: False
      number_of_year: 1
      baseline_substract: 36
      baseline_add: 36
    temp_baseline: 36
    max_con_ifg: 5

  unwrap:
    unw_thre: 0.4

  geocode:
    dem_ovr: 1

# 输出/临时文件配置
OUTPUT:
  output_root: "/mnt/e/TEMP/S1_auto/"

"""
    with open("gamma_s1_config.yml", "w", encoding="utf-8") as f:
        f.write(config_text.strip())
    print("已生成: gamma_s1_config.yml")
    
def main():
    """主函数：解析参数、加载配置、执行步骤"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Sentinel-1 GAMMA 处理主程序（带日志和错误处理）')
    # 关键修改：加 nargs='?' 让参数可选，加 default=None 给默认值
    parser.add_argument('config_file', nargs='?', default=None, help='YAML配置文件路径（如gamma_s1_config.yml）')
    parser.add_argument('start', type=int, nargs='?', default=1, help='起始步骤（1-5）')
    parser.add_argument('end', type=int, nargs='?', default=5, help='结束步骤（1-5）')
    args = parser.parse_args()

    # 无config_file时生成配置文件（现在能执行到这行了）
    if not args.config_file:
        generate_config()
        print("请先编辑 gamma_s1_config.yml 配置文件，再执行：")
        print("gamma_s1_processor gamma_s1_config.yml 1 5")
        return
    
    # 以下逻辑完全不变
    # 临时初始化基础日志（加载配置前）
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('gamma_s1_processor')
    
    # 校验步骤参数
    try:
        if args.start < 1 or args.end > 5 or args.start > args.end:
            raise ValueError(f"步骤参数无效！需满足 1≤start≤end≤5，当前：start={args.start}, end={args.end}")
    except ValueError as e:
        logger.error(e)
        sys.exit(1)
    
    # 加载配置
    config = load_config(args.config_file)
    
    # 重新初始化完整日志系统
    logger = setup_logger(config)
    logger.info("="*10)
    logger.info(f"启动 Sentinel-1 GAMMA 处理程序 | 执行步骤：{args.start} - {args.end}")
    logger.info("="*10)
    
    # 步骤映射
    steps = {
        1: step1_plot_kml,
        2: step2_generate_master_image,
        3: step3_generate_cropped_map,
        4: step4_coregister,
        5: step5_generate_interferograms
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