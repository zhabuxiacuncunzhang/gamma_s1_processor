#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import os
import sys
import yaml
import logging
from logging.handlers import RotatingFileHandler
import glob
from contextlib import contextmanager
from gamma_s1_processor import load_config, setup_logger, redirect_stdout_stderr
import json
from pathlib import Path
import py_gamma as pg
import numpy as np

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

def step1_generate_cfg(config):
    """
    步骤1：生成配置文件
    """
    logger = logging.getLogger('run_licsbas')

    try:
        # 获取输出根目录
        output_root = config['OUTPUT']['output_root']
        licsbas_work = "licsbas"
        log_root = os.path.join(output_root, "LOGs")
        licsbas_log_dir = os.path.join(log_root, "licsbas")
        os.makedirs(licsbas_log_dir, exist_ok=True)

        licsbas_path = os.path.join(output_root, licsbas_work)
        # 先检查目录是否存在，存在则抛异常
        try:
            if os.path.exists(licsbas_path):
                raise FileExistsError(f"输出目录 {licsbas_path} 已存在")
            # 目录不存在则创建
            os.makedirs(licsbas_path, exist_ok=False)
            logger.info(f"成功创建目录：{licsbas_path}")
        except FileExistsError as e:
            logger.warning(f"创建目录失败：{e}")

        dirs_to_create = ["GEOC"]
        
        # 遍历检查并创建目录
        for dir_name in dirs_to_create:
            dir_path = os.path.join(licsbas_path, dir_name)
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
        GEOC_dir = os.path.join(licsbas_path, "GEOC")

        logger.info("开始准备文件")

        ifgs_dir = os.path.join(output_root, "IFGs")
        master_date = str(config['PROCESSING']['common_master_date'])
        master_dir = os.path.join(ifgs_dir, master_date)
        log_root = os.path.join(output_root, "LOGs")

        create_symlink(os.path.join(log_root, "BASE", "bperp"), 
                               os.path.join(GEOC_dir, "baselines"))

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

        # data2geotiff
        logger.info(f"data2geotiff 处理...")
        try:
            log_file_path = os.path.join(licsbas_log_dir, f"hgt_data2geotiff.log")
            with redirect_stdout_stderr(log_file_path):
                current_dir = os.getcwd()
                os.chdir(master_dir)  # 切换到日期目录执行，确保输入输出在该目录
                output_file = os.path.join(GEOC_dir, f"{master_date}.geo.hgt.tif")
                stat = pg.data2geotiff(
                    DEM_par = os.path.join(master_dir, f"dem_seg.dem_par"),
                    data = os.path.join(master_dir, f"dem_seg.dem"),
                    type = 2,
                    GeoTIFF = output_file
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
            # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"data2geotiff 处理成功（状态码：{stat}）")
                        
                if os.path.exists(output_file):
                    logger.info(f"data2geotiff 处理成功 → {output_file}")
                else:
                    logger.warning(f"未检测到生成的 hgt.tif 文件 {output_file}！")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"data2geotiff 执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"data2geotiff 返回未知状态码：{stat}（预期0/-1）")
        except Exception as e:
            error_msg = f"data2geotiff 处理失败: {str(e)}"
            logger.error(error_msg)
        logger.info(f"hgt.tif 文件已生成至：{output_file}")

        # data2geotiff
        logger.info(f"data2geotiff 处理...")
        try:
            log_file_path = os.path.join(licsbas_log_dir, f"mli_data2geotiff.log")
            with redirect_stdout_stderr(log_file_path):
                current_dir = os.getcwd()
                os.chdir(master_dir)  # 切换到日期目录执行，确保输入输出在该目录
                output_file = os.path.join(GEOC_dir, f"{master_date}.geo.mli.tif")
                stat = pg.data2geotiff(
                    DEM_par = os.path.join(master_dir, f"dem_seg.dem_par"),
                    data = os.path.join(master_dir, f"dem_seg.{master_date}.mli"),
                    type = 2,
                    GeoTIFF = output_file
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
            # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"data2geotiff 处理成功（状态码：{stat}）")
                        
                if os.path.exists(output_file):
                    logger.info(f"data2geotiff 处理成功 → {output_file}")
                else:
                    logger.warning(f"未检测到生成的 mli.tif 文件 {output_file}！")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"data2geotiff 执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"data2geotiff 返回未知状态码：{stat}（预期0/-1）")
        except Exception as e:
            error_msg = f"data2geotiff 处理失败: {str(e)}"
            logger.error(error_msg)
        logger.info(f"mli.tif 文件已生成至：{output_file}")

        # look_vector
        logger.info(f"look_vector 处理...")
        off = glob.glob(os.path.join(ifgs_dir, "*/*.off"))[0]
        try:
            log_file_path = os.path.join(licsbas_log_dir, f"look_vector.log")
            with redirect_stdout_stderr(log_file_path):
                current_dir = os.getcwd()
                os.chdir(master_dir)  # 切换到日期目录执行，确保输入输出在该目录
                output_file1 = os.path.join(GEOC_dir, f"{master_date}.elev")
                output_file2 = os.path.join(GEOC_dir, f"{master_date}.azi")
                stat = pg.look_vector(
                    SLC_par = os.path.join(master_dir, f"{master_date}.slc.par"),
                    OFF_par = off,
                    DEM_par = os.path.join(master_dir, f"dem_seg.dem_par"),
                    DEM = os.path.join(master_dir, f"dem_seg.dem"),
                    lv_theta = output_file1,
                    lv_phi = output_file2,
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
            # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"look_vector 处理成功（状态码：{stat}）")
                        
                if os.path.exists(output_file1):
                    logger.info(f"look_vector 处理成功 → {output_file1}")
                else:
                    logger.warning(f"未检测到生成的 elev 文件 {output_file1}！")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"look_vector 执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"look_vector 返回未知状态码：{stat}（预期0/-1）")
        except Exception as e:
            error_msg = f"look_vector 处理失败: {str(e)}"
            logger.error(error_msg)
        logger.info(f"elev 文件已生成至：{output_file1}")
        logger.info(f"azi 文件已生成至：{output_file2}")    

        mli_par = os.path.join(master_dir, f"{master_date}.mli.par")
        dem_seg_width = pg.ParFile(os.path.join(master_dir, f"dem_seg.dem_par")).get_value("width")
        dem_seg_length = pg.ParFile(os.path.join(master_dir, f"dem_seg.dem_par")).get_value("nlines")
        mli_width = pg.ParFile(mli_par).get_value("range_samples")
        lt_fine_path = os.path.join(master_dir, f"dem_seg.{master_date}.lt_fine")
        # geocode_back
        logger.info(f"geocode_back 处理...")
        try:
            log_file_path = os.path.join(licsbas_log_dir, f"elev_geocode_back.log")
            with redirect_stdout_stderr(log_file_path):
                current_dir = os.getcwd()
                os.chdir(GEOC_dir)  # 切换到日期目录执行，确保输入输出在该目录
                output_file = os.path.join(GEOC_dir, f"{master_date}.geo.elev")
                stat = pg.geocode_back(
                    data_in = os.path.join(GEOC_dir, f"{master_date}.elev"),
                    width_in = mli_width,
                    lookup_table = lt_fine_path,
                    data_out = output_file,
                    width_out = dem_seg_width
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
            # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"geocode_back 处理成功（状态码：{stat}）")
                        
                if os.path.exists(output_file):
                    logger.info(f"geocode_back 处理成功 → {output_file}")
                else:
                    logger.warning(f"未检测到生成的 geo.elev 文件 {output_file}！")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"geocode_back 执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"geocode_back 返回未知状态码：{stat}（预期0/-1）")
        except Exception as e:
            error_msg = f"geocode_back 处理失败: {str(e)}"
            logger.error(error_msg)
        logger.info(f"geo.elev 文件已生成至：{output_file}")

        # geocode_back
        logger.info(f"geocode_back 处理...")
        try:
            log_file_path = os.path.join(licsbas_log_dir, f"azi_geocode_back.log")
            with redirect_stdout_stderr(log_file_path):
                current_dir = os.getcwd()
                os.chdir(GEOC_dir)  # 切换到日期目录执行，确保输入输出在该目录
                output_file = os.path.join(GEOC_dir, f"{master_date}.geo.azi")
                stat = pg.geocode_back(
                    data_in = os.path.join(GEOC_dir, f"{master_date}.azi"),
                    width_in = mli_width,
                    lookup_table = lt_fine_path,
                    data_out = output_file,
                    width_out = dem_seg_width
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
            # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"geocode_back 处理成功（状态码：{stat}）")
                        
                if os.path.exists(output_file):
                    logger.info(f"geocode_back 处理成功 → {output_file}")
                else:
                    logger.warning(f"未检测到生成的 geo.azi 文件 {output_file}！")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"geocode_back 执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"geocode_back 返回未知状态码：{stat}（预期0/-1）")
        except Exception as e:
            error_msg = f"geocode_back 处理失败: {str(e)}"
            logger.error(error_msg)
        logger.info(f"geo.azi 文件已生成至：{output_file}")

        elev_file = os.path.join(GEOC_dir, f"{master_date}.geo.elev")
        azi_file = os.path.join(GEOC_dir, f"{master_date}.geo.azi")
        thetarc = np.fromfile(elev_file,dtype=np.float32).byteswap().reshape((int(dem_seg_length),int(dem_seg_width)))
        nanix = thetarc == 0
        thetarc[nanix] = np.nan
        phirc = np.fromfile(azi_file,dtype=np.float32).byteswap().reshape((int(dem_seg_length),int(dem_seg_width)))
        phirc[nanix] = np.nan
        U = np.sin(thetarc)
        E = np.cos(phirc)*np.cos(thetarc)
        N = np.sin(phirc)*np.cos(thetarc)
        U[nanix] = 0
        E[nanix] = 0
        N[nanix] = 0
        U.byteswap().tofile(os.path.join(GEOC_dir,'U'))
        E.byteswap().tofile(os.path.join(GEOC_dir,'E'))
        N.byteswap().tofile(os.path.join(GEOC_dir,'N'))

        # data2geotiff
        logger.info(f"data2geotiff 处理...")
        try:
            log_file_path = os.path.join(licsbas_log_dir, f"U_data2geotiff.log")
            with redirect_stdout_stderr(log_file_path):
                current_dir = os.getcwd()
                os.chdir(GEOC_dir)  # 切换到日期目录执行，确保输入输出在该目录
                output_file = os.path.join(GEOC_dir, f"{master_date}.geo.U.tif")
                stat = pg.data2geotiff(
                    DEM_par = os.path.join(master_dir, f"dem_seg.dem_par"),
                    data = os.path.join(GEOC_dir, f"U"),
                    type = 2,
                    GeoTIFF = output_file
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
            # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"data2geotiff 处理成功（状态码：{stat}）")
                        
                if os.path.exists(output_file):
                    logger.info(f"data2geotiff 处理成功 → {output_file}")
                else:
                    logger.warning(f"未检测到生成的 U.tif 文件 {output_file}！")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"data2geotiff 执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"data2geotiff 返回未知状态码：{stat}（预期0/-1）")
        except Exception as e:
            error_msg = f"data2geotiff 处理失败: {str(e)}"
            logger.error(error_msg)
        logger.info(f"U.tif 文件已生成至：{output_file}")

        # data2geotiff
        logger.info(f"data2geotiff 处理...")
        try:
            log_file_path = os.path.join(licsbas_log_dir, f"E_data2geotiff.log")
            with redirect_stdout_stderr(log_file_path):
                current_dir = os.getcwd()
                os.chdir(GEOC_dir)  # 切换到日期目录执行，确保输入输出在该目录
                output_file = os.path.join(GEOC_dir, f"{master_date}.geo.E.tif")
                stat = pg.data2geotiff(
                    DEM_par = os.path.join(master_dir, f"dem_seg.dem_par"),
                    data = os.path.join(GEOC_dir, f"E"),
                    type = 2,
                    GeoTIFF = output_file
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
            # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"data2geotiff 处理成功（状态码：{stat}）")
                        
                if os.path.exists(output_file):
                    logger.info(f"data2geotiff 处理成功 → {output_file}")
                else:
                    logger.warning(f"未检测到生成的 E.tif 文件 {output_file}！")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"data2geotiff 执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"data2geotiff 返回未知状态码：{stat}（预期0/-1）")
        except Exception as e:
            error_msg = f"data2geotiff 处理失败: {str(e)}"
            logger.error(error_msg)
        logger.info(f"E.tif 文件已生成至：{output_file}")

        # data2geotiff
        logger.info(f"data2geotiff 处理...")
        try:
            log_file_path = os.path.join(licsbas_log_dir, f"N_data2geotiff.log")
            with redirect_stdout_stderr(log_file_path):
                current_dir = os.getcwd()
                os.chdir(GEOC_dir)  # 切换到日期目录执行，确保输入输出在该目录
                output_file = os.path.join(GEOC_dir, f"{master_date}.geo.N.tif")
                stat = pg.data2geotiff(
                    DEM_par = os.path.join(master_dir, f"dem_seg.dem_par"),
                    data = os.path.join(GEOC_dir, f"N"),
                    type = 2,
                    GeoTIFF = output_file
                    )
                os.chdir(current_dir)  # 恢复原始工作目录
            # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"data2geotiff 处理成功（状态码：{stat}）")
                        
                if os.path.exists(output_file):
                    logger.info(f"data2geotiff 处理成功 → {output_file}")
                else:
                    logger.warning(f"未检测到生成的 N.tif 文件 {output_file}！")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"data2geotiff 执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"data2geotiff 返回未知状态码：{stat}（预期0/-1）")
        except Exception as e:
            error_msg = f"data2geotiff 处理失败: {str(e)}"
            logger.error(error_msg)
        logger.info(f"N.tif 文件已生成至：{output_file}")

        if "pairs_file" not in config['PROCESSING']['sbas'] or not os.path.exists(config['PROCESSING']['sbas']['pairs_file']):
            logger.info("使用sbas_date_pairs.txt进行SBAS干涉处理...")
            pairs_file = os.path.join(log_root, "sbas_date_pairs.txt")
        else:
            logger.info(f"已找到有效的pairs_file配置，正在使用 {config['PROCESSING']['sbas']['pairs_file']} 进行SBAS干涉处理...")
            pairs_file = config['PROCESSING']['sbas']['pairs_file']

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
                
                # 拼接文件路径并查找
                unw_path = os.path.join(ifgs_dir, line, f"{line}.unw.geo.tif")
                cc_path = os.path.join(ifgs_dir, line, f"{line}.cc.geo.tif")
                unw_list = glob.glob(unw_path)
                cc_list = glob.glob(cc_path)
                
                # 标记文件是否存在
                unw_exists = bool(unw_list)
                cc_exists = bool(cc_list)
                
                # 仅当两个文件都存在时，才加入有效列表
                if unw_exists and cc_exists:
                    logger.info(f"第 {intf_num} 对 | {line} 文件正常")
                    valid_pairs.append((intf_num, line))  # 存入有效列表
                else:
                    # 任一文件不存在时，仅输出warning，不终止
                    warning_msg = f"第 {intf_num} 对 | "
                    if not unw_exists:
                        warning_msg += f"unw 文件不存在（路径：{unw_path}）；"
                    if not cc_exists:
                        warning_msg += f"cc 文件不存在（路径：{cc_path}）；"
                    # 移除末尾多余的分号/空格
                    warning_msg = warning_msg.rstrip('； ')
                    logger.warning(warning_msg)

            for intf_num, line in valid_pairs:
                line = line.strip()
                unw_path = os.path.join(ifgs_dir, line, f"{line}.unw.geo.tif")
                cc_path = os.path.join(ifgs_dir, line, f"{line}.cc.geo.tif")

                date_parts = line.split('-')
                start_date, end_date = date_parts
                interf_path = os.path.join(GEOC_dir, f"{start_date}_{end_date}")

                try:
                    if os.path.exists(interf_path):
                        raise FileExistsError(f"输出目录 {interf_path} 已存在")
                    # 目录不存在则创建
                    os.makedirs(interf_path, exist_ok=False)
                    logger.info(f"成功创建目录：{interf_path}")
                except FileExistsError as e:
                    logger.warning(f"创建目录失败：{e}")

                create_symlink(unw_path, 
                               os.path.join(interf_path, f"{start_date}_{end_date}.geo.unw.tif"))
                create_symlink(cc_path, 
                               os.path.join(interf_path, f"{start_date}_{end_date}.geo.cc.tif"))
                

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
    return

def step2_run_licsbas(config):
    """
    步骤2：提示如何执行
    """
    logger = logging.getLogger('run_licsbas')
    logger.info("======= 请执行 LiCSBAS =======")
    output_root = config['OUTPUT']['output_root']
    licsbas_work = "licsbas"
    licsbas_path = os.path.join(output_root, licsbas_work)
    logger.info(f"cd {licsbas_path}")
    logger.info("run LiCSBAS from 02")
    logger.info("LiCSBAS02_ml_prep.py -i ./GEOC")
    return

def main():
    """主函数：解析参数、加载配置、执行步骤"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Sentinel-1 LiCSBAS 处理程序')
    parser.add_argument('config_file', help='YAML配置文件路径（如gamma_s1_config.yml）')
    parser.add_argument('start', type=int, help='起始步骤（1-2）')
    parser.add_argument('end', type=int, help='结束步骤（1-2）')
    args = parser.parse_args()
    
    # 临时初始化基础日志（加载配置前）
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('run_licsbas')
    
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
    logger = setup_logger(config, filename = './run_LiCSBAS.log', name = 'run_licsbas')
    logger.info("="*10)
    logger.info(f"启动 LiCSBAS 处理程序 | 执行步骤：{args.start} - {args.end}")
    logger.info("="*10)
    
    # 步骤映射
    steps = {
        1: step1_generate_cfg,
        2: step2_run_licsbas,
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