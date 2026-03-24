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
from osgeo import gdal, gdalconst
from typing import List, Tuple
import numpy as np
from s1_auto_bin import plot_2Geotif as plot

# 初始化GDAL
gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "YES")
gdal.SetConfigOption("SHAPE_ENCODING", "UTF-8")
gdal.UseExceptions()  # 启用异常捕获
gdal.SetConfigOption("GDALWARP_IGNORE_BAD_CUTLINE", "YES")  # 忽略裁剪边界异常

# ===================== 用GDAL获取tif经纬度范围 =====================
def get_tif_lon_lat_range(tif_path: str) -> Tuple[float, float, float, float]:
    """
    仅用GDAL获取tif文件的经纬度范围（min_lon, max_lon, min_lat, max_lat）
    核心逻辑：GDAL的GetGeoTransform + 像素尺寸直接计算，无需多坐标转换
    """
    if not os.path.exists(tif_path):
        raise FileNotFoundError(f"文件不存在：{tif_path}")

    # 打开tif数据集
    ds = gdal.Open(tif_path)
    if ds is None:
        raise RuntimeError(f"GDAL无法打开文件：{tif_path}")

    # 1. 获取地理变换参数
    # GT = [左上角X, 像素X分辨率, 旋转, 左上角Y, 旋转, 像素Y分辨率]
    gt = ds.GetGeoTransform()
    if gt is None:
        raise RuntimeError(f"无法获取{tif_path}的地理变换信息")

    # 2. 获取影像宽高
    width = ds.RasterXSize  # 列数（X方向像素数）
    height = ds.RasterYSize # 行数（Y方向像素数）

    # 3. 计算地理范围（核心：仅用GDAL的GT参数+宽高，无需四角落计算）
    # 左上角坐标
    min_x = gt[0]
    max_y = gt[3]
    # 右下角坐标（X = 左上角X + 宽度*X分辨率；Y = 左上角Y + 高度*Y分辨率）
    max_x = gt[0] + width * gt[1]
    min_y = gt[3] + height * gt[5]

    # 4. 直接判定：如果是经纬度投影（WGS84/EPSG:4326），X=经度，Y=纬度
    # （如果你的GACOS文件本身就是经纬度投影，这一步直接用；如果是投影坐标，需加投影转换）
    # 先尝试读取投影信息，自动判断是否需要转经纬度
    proj = ds.GetProjection()
    if "GEOGCS[\"WGS 84\"" in proj or "EPSG:4326" in proj:
        # 已是经纬度投影，直接赋值
        min_lon, max_lon = min_x, max_x
        min_lat, max_lat = min_y, max_y
    else:
        # 投影坐标转经纬度
        from osgeo import osr
        src_srs = osr.SpatialReference(wkt=proj)
        dst_srs = osr.SpatialReference()
        dst_srs.ImportFromEPSG(4326)  # 转WGS84经纬度
        transform = osr.CoordinateTransformation(src_srs, dst_srs)
        
        # 仅转换两个对角点（足够计算范围）
        min_lon, min_lat, _ = transform.TransformPoint(min_x, min_y)
        max_lon, max_lat, _ = transform.TransformPoint(max_x, max_y)

    # 确保范围是“小值在前，大值在后”（处理南/北半球、东/西经情况）
    min_lon, max_lon = sorted([min_lon, max_lon])
    min_lat, max_lat = sorted([min_lat, max_lat])

    # 关闭数据集
    ds = None
    return (min_lon, 
            max_lon, 
            min_lat, 
            max_lat, 
            gt[1], 
            gt[5],
            width,
            height)

# ===================== 裁剪+对齐GACOS文件到DEM范围/步长 =====================
def warp_gacos_to_dem(gacos_tif_path, dem_extent, output_tif_path, resample_method=gdalconst.GRA_Bilinear):
    """
    将GACOS文件裁剪到DEM范围，并对齐到DEM的步长（分辨率）
    :param gacos_tif_path: 输入GACOS tif文件路径
    :param dem_extent: DEM范围字典（get_dem_full_extent的返回值）
    :param output_tif_path: 输出对齐后的tif路径
    :param resample_method: 重采样方法（默认双线性，适合连续数据）
    """
    logger = logging.getLogger('run_gacos')

    # 检查输入文件是否存在
    if not os.path.exists(gacos_tif_path):
        raise FileNotFoundError(f"GACOS文件不存在：{gacos_tif_path}")

    # 输出文件夹不存在则创建
    output_dir = os.path.dirname(output_tif_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # ========== 修复核心：显式打开数据集，保留引用 ==========
    # 1. 显式打开输入文件，获取数据类型（避免临时对象回收）
    src_ds = gdal.Open(gacos_tif_path, gdalconst.GA_ReadOnly)
    if src_ds is None:
        raise RuntimeError(f"无法打开GACOS文件：{gacos_tif_path}")
    # 获取原文件的数据类型（显式获取，保留引用）
    src_data_type = src_ds.GetRasterBand(1).DataType
    # 关闭数据集（获取类型后即可关闭，避免占用资源）
    src_ds = None

    # 2. GDAL Warp核心参数（裁剪+重采样+对齐）
    warp_options = gdal.WarpOptions(
        # 1. 裁剪范围（xmin, ymin, xmax, ymax）
        outputBounds=(dem_extent["min_lon"], dem_extent["min_lat"], dem_extent["max_lon"], dem_extent["max_lat"]),
        # 2. 对齐到DEM的步长（分辨率）
        xRes=dem_extent["x_step"],
        yRes=dem_extent["y_step"],
        # 3. 重采样方法（双线性插值，适合ZTD这类连续数据）
        resampleAlg=resample_method,
        # 4. 保持原数据类型（使用显式获取的类型，修复报错）
        # dstNodata=-9999,  # 无数据值（根据你的GACOS文件调整）
        copyMetadata=True,  # 保留原元数据
        outputType=src_data_type,  # ✅ 修复：使用提前获取的有效类型
        # 5. 投影保持一致（默认和输入GACOS一致，如需转WGS84可加dstSRS='EPSG:4326'）
    )

    # 执行裁剪+重采样
    try:
        logger.info(f"开始处理：{os.path.basename(gacos_tif_path)}")
        # 调用GDAL Warp
        ds = gdal.Warp(
            destNameOrDestDS=output_tif_path,
            srcDSOrSrcDSTab=gacos_tif_path,
            options=warp_options
        )
        if ds is None:
            raise RuntimeError(f"裁剪失败：{gacos_tif_path}")
        ds = None  # 关闭数据集，确保文件写入
        logger.info(f"保存对齐后的文件：{output_tif_path}")
    except Exception as e:
        raise RuntimeError(f"处理{os.path.basename(gacos_tif_path)}失败：{str(e)}")
    
def make_correction(phsfilename, ztd1filename, ztd2filename, elevfilename, std_log_path="phase_std_log.txt", plot_log_path="plot.log" ):
    """
    纯GDAL实现相位校正（适配循环调用场景）
    :param phsfilename: 相位TIFF文件路径（带.tif后缀）
    :param ztd1filename: 起始日期GACOS TIFF路径
    :param ztd2filename: 结束日期GACOS TIFF路径
    :param elevfilename: 高程TIFF路径
    :param std_log_path: std日志保存路径（默认当前目录）
    :return: None
    """
    # ===================== 辅助函数：GDAL读取TIFF =====================
    def read_tif(tif_path):
        if not os.path.exists(tif_path):
            raise FileNotFoundError(f"文件不存在: {tif_path}")
        ds = gdal.Open(tif_path, gdal.GA_ReadOnly)
        if ds is None:
            raise RuntimeError(f"GDAL无法打开文件: {tif_path}")
        geotrans = ds.GetGeoTransform()
        proj = ds.GetProjection()
        rows = ds.RasterYSize
        cols = ds.RasterXSize
        data = ds.GetRasterBand(1).ReadAsArray(0, 0, cols, rows).astype(np.float32)
        ds = None
        return data, geotrans, proj, rows, cols

    # ===================== 1. 读取输入数据 =====================
    # 读取相位文件（核心，用于提取输出文件名）
    phase, geotrans, proj, rows, cols = read_tif(phsfilename)
    # 读取其他文件并校验尺寸
    ztd1, _, _, r1, c1 = read_tif(ztd1filename)
    ztd2, _, _, r2, c2 = read_tif(ztd2filename)
    elev, _, _, r3, c3 = read_tif(elevfilename)
    
    if not (rows == r1 == r2 == r3 and cols == c1 == c2 == c3):
        raise ValueError(f"输入文件尺寸不一致！{phsfilename}({rows}x{cols}) vs {ztd1filename}({r1}x{c1})")

    # ===================== 2. 核心计算逻辑 =====================
    # 计算dztd
    dztd = ztd2 - ztd1
    dztd = dztd / 0.0044138251819503
    dztd = dztd / np.sin(elev)

    # 相位校正并记录std
    index = np.where(phase == 0)
    phase[index] = np.nan
    phasemean = np.nanmean(phase)
    std_before = np.nanstd(phase)
    phase = phase - phasemean

    phase = phase - dztd
    phase[index] = np.nan
    phasemean = np.nanmean(phase)
    std_after = np.nanstd(phase)
    phase = phase - phasemean
    phase[index] = 0

    # ===================== 3. 保存std到单独文件 =====================
    # 提取无后缀的相位文件名（用于日志标识）
    phs_basename = os.path.splitext(os.path.basename(phsfilename))[0]
    # 日志内容（包含时间、文件名、前后std）
    log_line = (
        f"文件: {phs_basename} | "
        f"校正前std: {std_before:.6f} | "
        f"校正后std: {std_after:.6f}\n"
    )
    # 追加写入日志（不存在则创建）
    with open(std_log_path, "w", encoding="utf-8") as f:
        f.write(log_line)
    print(f"[{phs_basename}] before {std_before:.6f} | after {std_after:.6f}")

    # ===================== 4. 保存校正结果（去除.tif后缀） =====================
    # 生成输出路径：去掉原文件.tif后缀 + .gacos.tif
    phs_dir = os.path.dirname(phsfilename)
    phs_name_no_ext = os.path.splitext(os.path.basename(phsfilename))[0]
    output_path = os.path.join(phs_dir, f"{phs_name_no_ext}.gacos.tif")

    # GDAL保存TIFF
    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(output_path, cols, rows, 1, gdal.GDT_Float32)
    if out_ds is None:
        raise RuntimeError(f"无法创建输出文件: {output_path}")
    out_ds.SetGeoTransform(geotrans)
    out_ds.SetProjection(proj)
    out_band = out_ds.GetRasterBand(1)
    out_band.WriteArray(phase)
    out_band.SetNoDataValue(np.nan)
    out_band.FlushCache()
    out_ds = None

    logger = logging.getLogger('run_gacos')
    logger.info(f"校正完成：{output_path}")

    # ===================== 5. 绘制校正结果 =====================
    with redirect_stdout_stderr(plot_log_path):
        plot.plot_two_tiffs(phsfilename, output_path, os.path.join(phs_dir, f"{phs_name_no_ext}.gacos"))


def step1_check_GACOS(config, GACOS_path):

    logger = logging.getLogger('run_gacos')

    # 获取配置参数
    output_root = config['OUTPUT']['output_root']
    ifgs_dir = os.path.join(output_root, "IFGs")
    master_date = str(config['PROCESSING']['common_master_date'])
    master_dir = os.path.join(ifgs_dir, master_date)
    log_root = os.path.join(output_root, "LOGs")
    gacos_log_dir = os.path.join(log_root, "GACOS")
    os.makedirs(gacos_log_dir, exist_ok=True)

    # 检查GACOS文件个数与date_keep是否一致
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

    # 获取GACOS目录下所有符合格式的文件（提前缓存，提升效率）
    gacos_files = set()
    gacos_date = set()
    for filename in os.listdir(GACOS_path):
        # 匹配 YYYYMMDD.ztd.tif
        if filename.endswith('.ztd.tif'):
            gacos_files.add(filename)
            gacos_date.add(filename[:-8])
    
    # 检查date_keep中的每个日期是否存在对应文件
    missing_dates = []
    total_dates = len(date_keep.keys())
    for date_str in date_keep.keys():     
        if date_str not in gacos_date:
           missing_dates.append(date_str)

    try:
        if missing_dates:
            raise ValueError(f"缺少 {missing_dates} 日期的GACOS文件")
        logger.info("===== GACOS 文件数检查 =====")
        logger.info(f"总日期数：{total_dates}")
        logger.info(f"存在GACOS文件的日期数：{total_dates - len(missing_dates)}")
        logger.info(f"缺失GACOS文件的日期数：{len(missing_dates)}")
        logger.info("所有日期的GACOS文件均存在，检查通过！")
    except ValueError as e:
        # 捕获异常并输出错误提示
        logger.error(f"===== GACOS 文件数检查 =====")
        logger.error(f"{e}，检查后再试！")
        logger.info(f"总日期数：{total_dates}")
        logger.info(f"存在GACOS文件的日期数：{total_dates - len(missing_dates)}")
        logger.info(f"缺失GACOS文件的日期数：{len(missing_dates)}")
        return  # 终止函数执行
    
    # 检查GACOS文件范围是否大于干涉图范围
    # dem_par_file = glob.glob(os.path.join(master_dir, "dem_seg.dem_par"))
    unw_file = glob.glob(os.path.join(ifgs_dir, "*/*.unw.geo.tif"))
    if not unw_file:
        logger.info(f"unw 不存在") 
        return 
    else:
        [dem_min_lon, 
         dem_max_lon, 
         dem_min_lat, 
         dem_max_lat, 
         dem_x_step,
         dem_y_step,
         dem_width, 
         dem_length]= get_tif_lon_lat_range(unw_file[0])

        logging.info(f"干涉图的经度范围 {dem_min_lon:.7f} - {dem_max_lon:.7f}")
        logging.info(f"干涉图的纬度范围 {dem_min_lat:.7f} - {dem_max_lat:.7f}")

    uncovered_files = []
    for filename in gacos_files:

        tif_full_path = os.path.join(GACOS_path, filename)
        try:
            # 仅调用GDAL核心函数获取范围
            g_min_lon, g_max_lon, g_min_lat, g_max_lat = get_tif_lon_lat_range(tif_full_path)[:4]
            
            # 检查覆盖性（GACOS范围需完全包含DEM范围）
            is_cover = (g_min_lon <= dem_min_lon) and (g_max_lon >= dem_max_lon) and \
                       (g_min_lat <= dem_min_lat) and (g_max_lat >= dem_max_lat)
            
            if not is_cover:
                logger.error(f"未覆盖干涉图范围！")
                uncovered_files.append(filename)

        except Exception as e:
            logger.error(f"处理{filename}失败：{str(e)}")
            uncovered_files.append(filename)

    logger.info(f"===== GACOS 文件范围检查 =====")
    logger.info(f"检查总结：共{len(gacos_files)}个文件，未覆盖{len(uncovered_files)}个")
    if uncovered_files:
        logger.error(f"未覆盖文件列表：{uncovered_files}")

    # 如果没有elev文件，生成
    elev_file = glob.glob(os.path.join(master_dir, f"{master_date}.elev.tif"))
    if elev_file:
        logger.info(f"elev 文件 → {elev_file[0]}已存在") 
    else:
        off = glob.glob(os.path.join(ifgs_dir, "*/*.off"))[0]
        log_file_path = os.path.join(gacos_log_dir, "look_vector.log")
        ## look_vector
        logger.info(f"look_vector 处理...")
        try:
            with redirect_stdout_stderr(log_file_path):
                print(f"========== look_vector 执行日志 ==========\n")
                current_dir = os.getcwd()
                os.chdir(master_dir)  # 切换到日期目录执行，确保输入输出在该目录
                stat = pg.look_vector(
                        SLC_par = os.path.join(master_dir, master_date + ".slc.par"),
                        OFF_par = off,
                        DEM = os.path.join(master_dir, "dem_seg.dem"),
                        DEM_par = os.path.join(master_dir, "dem_seg.dem_par"),
                        lv_theta = os.path.join(master_dir, master_date + ".elev"),
                        lv_phi = os.path.join(master_dir, master_date + ".azi"),
                        )
                os.chdir(current_dir)  # 恢复原始工作目录
            # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"look_vector 处理成功（状态码：{stat}）")
                if os.path.join(master_dir, master_date + ".elev"):
                    logger.info(f"look_vector 处理成功 → {os.path.join(master_dir, master_date + ".elev")}")
                else:
                    logger.warning(f"未检测到生成的elev {os.path.join(master_dir, master_date + ".elev")}！")
                logger.info(f"look_vector {log_file_path}")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"look_vector 执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"look_vector 返回未知状态码：{stat}（预期0/-1）")

        except Exception as e:
            logger.error(f"调用 look_vector 时发生未知错误：{e}")
            raise
        logger.info(f"elev 已生成至：{os.path.join(master_dir, master_date + ".elev")}")

        ## data2geotiff
        logger.info(f"data2geotiff 处理...")
        try:
            with redirect_stdout_stderr(log_file_path):
                print(f"========== data2geotiff 执行日志 ==========\n")
                current_dir = os.getcwd()
                os.chdir(master_dir)  # 切换到日期目录执行，确保输入输出在该目录
                stat = pg.data2geotiff(
                        DEM_par = os.path.join(master_dir, "dem_seg.dem_par"),
                        data = os.path.join(master_dir, master_date + ".elev"),
                        GeoTIFF = os.path.join(master_dir, master_date + ".elev.tif"),
                        type = 2
                        )
                os.chdir(current_dir)  # 恢复原始工作目录
            # 根据返回状态码判断执行结果
            if stat == 0:
                logger.info(f"data2geotiff 处理成功（状态码：{stat}）")
                if os.path.join(master_dir, master_date + ".elev.tif"):
                    logger.info(f"data2geotiff 处理成功 → {os.path.join(master_dir, master_date + ".elev.tif")}")
                else:
                    logger.warning(f"未检测到生成的elev {os.path.join(master_dir, master_date + ".elev.tif")}！")
                logger.info(f"data2geotiff {log_file_path}")
            elif stat == -1:
                # 状态码-1表示失败，主动抛出异常
                raise RuntimeError(f"data2geotiff 执行失败（状态码：{stat}）")
            else:
                # 处理未知状态码（防止函数返回其他值）
                raise RuntimeError(f"data2geotiff 返回未知状态码：{stat}（预期0/-1）")

        except Exception as e:
            logger.error(f"调用 data2geotiff 时发生未知错误：{e}")
            raise
        logger.info(f"elev 已生成至：{os.path.join(master_dir, master_date + ".elev.tif")}")

    # 将GACOS文件裁剪对齐,到干涉图
    failed_files = []
    output_dir = os.path.join(log_root, "GACOS_clip")
    os.makedirs(output_dir, exist_ok=True)
    for filename in gacos_files:
        if not filename.endswith('.tif'):
            logger.warning(f"跳过非TIF文件：{filename}")
            continue

        # 输入输出路径
        input_tif = os.path.join(GACOS_path, filename)
        output_tif = os.path.join(output_dir, filename)  # 输出文件名和原文件一致
        if os.path.exists(output_tif):
            logger.info(f"{output_tif} 已存在，跳过")
            continue

        dem_extent = {
            "min_lon": dem_min_lon,
            "max_lon": dem_max_lon,
            "min_lat": dem_min_lat,
            "max_lat": dem_max_lat,
            "x_step": dem_x_step,
            "y_step": dem_y_step,
            "width": dem_width,
            "length": dem_length
        }

        try:
            # 裁剪+对齐
            warp_gacos_to_dem(input_tif, dem_extent, output_tif)
        except Exception as e:
            logger.error(f" {filename} 处理失败：{str(e)}")
            failed_files.append(filename)

    # 处理总结
    logger.info("=== 处理总结 ===")
    logger.info(f"总文件数：{len(gacos_files)}")
    logger.info(f"成功数：{len(gacos_files) - len(failed_files)}")
    logger.info(f"失败数：{len(failed_files)}")
    if failed_files:
        logger.error(f"失败文件列表：{failed_files}")
        sys.exit(1)
    else:
        logger.info("🎉 所有GACOS文件均对齐成功！")

def step2_run_GACOS(config, GACOS_path):
    logger = logging.getLogger('run_gacos')

    # 获取配置参数
    output_root = config['OUTPUT']['output_root']
    ifgs_dir = os.path.join(output_root, "IFGs")
    master_date = str(config['PROCESSING']['common_master_date'])
    master_dir = os.path.join(ifgs_dir, master_date)
    log_root = os.path.join(output_root, "LOGs")
    gacos_log_dir = os.path.join(log_root, "GACOS")
    gacos_dir = os.path.join(log_root, "GACOS_clip")
    os.makedirs(gacos_log_dir, exist_ok=True)

    # 如果未指定pair文件或者文件未找到
    if "pairs_file" not in config['PROCESSING']['sbas'] or not os.path.exists(config['PROCESSING']['sbas']['pairs_file']):
        pairs_file = os.path.join(log_root, "sbas_date_pairs.txt")
    else:
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
    
    elev = os.path.join(master_dir, master_date + ".elev.tif")
    std_log = os.path.join(gacos_log_dir, "gacos_std.txt")
    plot_log = os.path.join(gacos_log_dir, "gacos_plot.log")
    for intf_num, line in enumerate(intf_pair, 1):
        line = line.strip()
        date_parts = line.split('-')
        start_date, end_date = date_parts

        unw_file = os.path.join(ifgs_dir, line, f"{line}.unw.geo.tif")
        start_gacos = os.path.join(gacos_dir, start_date + ".ztd.tif")
        end_gacos = os.path.join(gacos_dir, end_date + ".ztd.tif")

        print(f"\n===== 处理第{intf_num}个干涉对：{line} =====")
        # 调用校正函数
        try:
            make_correction(
                phsfilename=unw_file,
                ztd1filename=start_gacos,
                ztd2filename=end_gacos,
                elevfilename=elev,
                std_log_path=std_log,  # 指定std日志路径
                plot_log_path = plot_log
            )
        except Exception as e:
            print(f"❌ 处理{line}失败：{str(e)}")
            continue



def main():
    """主函数：解析参数、加载配置、执行步骤"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Sentinel-1 GACOS 处理程序')
    parser.add_argument('config_file', help='YAML配置文件路径（如gamma_s1_config.yml）')
    parser.add_argument('GACOS_path', help='GACOS文件路径（如./IFGS/GACOS）')
    parser.add_argument('start', type=int, help='起始步骤（1-2）')
    parser.add_argument('end', type=int, help='结束步骤（1-2）')
    args = parser.parse_args()
    
    # 临时初始化基础日志（加载配置前）
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('run_gacos')
    
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
    logger = setup_logger(config, filename = './run_GACOS.log', name = 'run_gacos')
    logger.info("="*10)
    logger.info(f"启动 GACOS 处理程序 | 执行步骤：{args.start} - {args.end}")
    logger.info("="*10)
    
    # 步骤映射
    steps = {
        1: step1_check_GACOS,
        2: step2_run_GACOS,
    }
    
    GACOS_path = os.path.abspath(args.GACOS_path)
    if os.path.exists(GACOS_path):
        logger.info(f"已检查 GACOS 文件夹 {GACOS_path} 位置正确")
    else:
        logger.error(f"GACOS 文件夹 {GACOS_path} 不存在！")
        logger.error(f"请检查后重试！")
        sys.exit(1)

    # 执行指定步骤
    try:
        for step_num in range(args.start, args.end + 1):
            logger.info(f"---------- 开始执行步骤 {step_num} ----------")
            steps[step_num](config, GACOS_path)
            logger.info(f"---------- 步骤 {step_num} 执行完成 ----------")
        
        logger.info("所有指定步骤执行完成！程序正常退出")
    except Exception as e:
        logger.critical(f"程序执行失败：{e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()