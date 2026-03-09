import py_gamma as pg
from osgeo import gdal
import os
import random
import string

def generate_random_filename(length=10, start="TEMP_", ext=".txt"):
    chars = string.ascii_letters + string.digits
    return start + ''.join(random.choice(chars) for _ in range(length)) + ext

gdal.UseExceptions()

def compress_tif_image(
    input_path: str,
    output_path: str,
    compress_method: str = "DEFLATE",
    compress_level: int = 6,
    predictor: int = 2
) -> bool:
    """
    使用GDAL压缩TIFF影像
    
    Args:
        input_path: 输入TIFF影像路径
        output_path: 输出压缩后TIFF影像路径
        compress_method: 压缩算法，可选值：
            - NONE: 无压缩
            - DEFLATE: 无损压缩（推荐，平衡压缩率和速度）
            - LZW: 无损压缩
            - JPEG: 有损压缩（仅适用于8位影像）
            - PACKBITS: 无损压缩（适合有大量重复值的影像）
        compress_level: 压缩级别（1-9），数值越大压缩率越高但速度越慢，仅对DEFLATE/LZW有效
        predictor: 预测器（1/2/3），2为差分预测，可提升DEFLATE/LZW对数值型影像的压缩率
    
    Returns:
        压缩成功返回True，失败返回False
    """
    # 验证输入文件是否存在
    if not os.path.exists(input_path):
        print(f"错误：输入文件不存在 - {input_path}")
        return False
    
    # 验证压缩算法合法性
    valid_compress_methods = ["NONE", "DEFLATE", "LZW", "JPEG", "PACKBITS"]
    if compress_method not in valid_compress_methods:
        print(f"错误：不支持的压缩算法 {compress_method}，可选值：{valid_compress_methods}")
        return False
    
    # 验证压缩级别
    if not (1 <= compress_level <= 9):
        print("错误：压缩级别必须为1-9，已自动调整为6")
        compress_level = 6

    try:
        # 1. 打开输入影像
        input_ds = gdal.Open(input_path, gdal.GA_ReadOnly)
        if input_ds is None:
            print(f"错误：无法打开输入影像 - {input_path}")
            return False
        
        # 2. 获取输入影像的基本信息
        driver = gdal.GetDriverByName("GTiff")
        band_count = input_ds.RasterCount  # 波段数
        x_size = input_ds.RasterXSize      # 宽度
        y_size = input_ds.RasterYSize      # 高度
        geo_transform = input_ds.GetGeoTransform()  # 地理变换参数
        projection = input_ds.GetProjection()       # 投影信息
        data_type = input_ds.GetRasterBand(1).DataType  # 数据类型

        # 3. 设置压缩选项
        compress_options = []
        if compress_method != "NONE":
            compress_options = [
                f"COMPRESS={compress_method}",
                f"ZLEVEL={compress_level}",  # DEFLATE/LZW的压缩级别
                f"PREDICTOR={predictor}"     # 预测器，提升数值影像压缩率
            ]
            # JPEG压缩额外参数（质量控制，1-100，越高质量越好）
            if compress_method == "JPEG":
                compress_options.append("JPEG_QUALITY=85")
                # 检查数据类型是否为8位（JPEG仅支持8位）
                if data_type not in [gdal.GDT_Byte, gdal.GDT_UInt16]:
                    print("警告：JPEG压缩仅推荐用于8位影像，可能导致数据异常")
        
        # 4. 创建输出压缩影像
        output_ds = driver.Create(
            output_path,
            x_size,
            y_size,
            band_count,
            data_type,
            options=compress_options
        )
        if output_ds is None:
            print(f"错误：无法创建输出影像 - {output_path}")
            input_ds = None  # 释放资源
            return False
        
        # 5. 拷贝地理信息和投影
        output_ds.SetGeoTransform(geo_transform)
        output_ds.SetProjection(projection)
        
        # 6. 逐波段拷贝影像数据
        for band_idx in range(1, band_count + 1):
            input_band = input_ds.GetRasterBand(band_idx)
            output_band = output_ds.GetRasterBand(band_idx)
            
            # 拷贝波段数据（支持大影像，自动分块）
            output_band.WriteArray(input_band.ReadAsArray())
            
            # 拷贝波段元数据（如NoData值、统计信息等）
            output_band.SetNoDataValue(input_band.GetNoDataValue())
            output_band.SetMetadata(input_band.GetMetadata())
        
        # 7. 刷新并释放资源（关键，避免文件损坏）
        output_ds.FlushCache()
        output_ds = None
        input_ds = None
        
        # 8. 验证输出文件并计算压缩率
        if os.path.exists(output_path):
            # input_size = os.path.getsize(input_path) / (1024 * 1024)  # MB
            # output_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
            # compression_ratio = (1 - output_size / input_size) * 100
            
            # print(f"✅ 压缩成功！")
            # print(f"输入文件大小: {input_size:.2f} MB")
            # print(f"输出文件大小: {output_size:.2f} MB")
            # print(f"压缩率: {compression_ratio:.2f}%")
            return True
        else:
            print("压缩失败：输出文件未生成")
            return False

    except Exception as e:
        print(f"压缩过程出错: {str(e)}")
        # 清理未完成的输出文件
        if os.path.exists(output_path):
            os.remove(output_path)
        return False

def geocode_image(config, data_in, data_out, type="FLOAT", cmap = "rmg.cm"):

    output_root = config['OUTPUT']['output_root']
    ifgs_dir = os.path.join(output_root, "IFGs")
    master_date = str(config['PROCESSING']['common_master_date'])
    master_dir = os.path.join(ifgs_dir, master_date)
    
    mli_par = os.path.join(master_dir, f"{master_date}.mli.par")
    dem_seg_par = os.path.join(master_dir, f"dem_seg.dem_par")

    dem_seg_width = pg.ParFile(dem_seg_par).get_value("width")
    mli_width = pg.ParFile(mli_par).get_value("range_samples")
    lt_fine_path = os.path.join(master_dir, f"dem_seg.{master_date}.lt_fine")


    if type == "FLOAT":
        dtype = 0
    elif type == "FCOMPLEX":
        dtype = 1

    try:
        print(f"========== geocode_back 执行日志 ==========\n")
        stat = pg.geocode_back(
            data_in = data_in,
            width_in = mli_width,
            lookup_table = lt_fine_path,
            data_out = data_out,
            width_out = dem_seg_width,
            dtype = dtype,
            )
            # 根据返回状态码判断执行结果
        if stat == 0:
            print(f"geocode_back 处理成功（状态码：{stat}）")
            if os.path.exists(data_out):
                print(f"geocode_back 处理成功，输出文件已生成：{data_out}")
            else:
                print(f"未检测到生成的输出文件 {data_out}！")
        elif stat == -1:
            # 状态码-1表示失败，主动抛出异常
            raise RuntimeError(f"geocode_back执行失败（状态码：{stat}）")
        else:
            # 处理未知状态码（防止函数返回其他值）
            raise RuntimeError(f"geocode_back返回未知状态码：{stat}（预期0/-1）")

    except Exception as e:
        print(f"调用geocode_back时发生未知错误：{e}")
        raise

    if type == "FLOAT":
        dtype = 2
    elif type == "FCOMPLEX":
        dtype = 4

    try:
        print(f"========== data2geotiff 执行日志 ==========\n")
        stat = pg.data2geotiff(
            DEM_par = dem_seg_par,
            data = data_out,
            type = dtype,
            GeoTIFF = data_out + ".tif",
            )
            # 根据返回状态码判断执行结果
        if stat == 0:
            print(f"data2geotiff 处理成功（状态码：{stat}）")
            if os.path.exists(data_out + ".tif"):
                print(f"data2geotiff 处理成功，输出文件已生成：{data_out + '.tif'}")
            else:
                print(f"未检测到生成的输出文件 {data_out + '.tif'}！")
        elif stat == -1:
            # 状态码-1表示失败，主动抛出异常
            raise RuntimeError(f"data2geotiff执行失败（状态码：{stat}）")
        else:
            # 处理未知状态码（防止函数返回其他值）
            raise RuntimeError(f"data2geotiff返回未知状态码：{stat}（预期0/-1）")

    except Exception as e:
        print(f"调用data2geotiff时发生未知错误：{e}")
        raise
    
    if type == "FLOAT":
        my_rasdt_pwr(data_in=data_out, pwr=None, width=dem_seg_width, cmap=cmap)
    if type == "FCOMPLEX":
        my_rasmph_pwr(data_in=data_out, pwr=None, width=dem_seg_width, cmap=cmap)


def my_rasdt_pwr(data_in, pwr, width, cmap = "rmg.cm"):

    report_name = generate_random_filename(5) 
    report_file = os.path.join(os.path.dirname(data_in), report_name)

    try:
        print(f"========== image_stat 执行日志 ==========\n")
        stat = pg.image_stat(
            image = data_in,
            width = width,
            report = report_file,
            )
            # 根据返回状态码判断执行结果
        if stat == 0:
            print(f"image_stat 处理成功（状态码：{stat}）")
            if os.path.exists(report_file):
                print(f"image_stat 处理成功，输出文件已生成：{report_file}")
            else:
                print(f"未检测到生成的输出文件 {report_file}！")
        elif stat == -1:
            # 状态码-1表示失败，主动抛出异常
            raise RuntimeError(f"image_stat执行失败（状态码：{stat}）")
        else:
            # 处理未知状态码（防止函数返回其他值）
            raise RuntimeError(f"image_stat返回未知状态码{stat}（预期0/-1）")

    except Exception as e:
        print(f"调用image_stat时发生未知错误：{e}")
        raise

    max_value = pg.ParFile(report_file).get_value("max") 
    min_value = pg.ParFile(report_file).get_value("min") 

    if pwr is None:
        pwr = "-"
    # rasdt_pwr
    try:
        print(f"========== rasdt_pwr 执行日志 ==========\n")
        stat = pg.rasdt_pwr(
            data = data_in,
            pwr = pwr,
            width = width,
            max = max_value,
            min = min_value,
            cmap = cmap,
            )
            # 根据返回状态码判断执行结果
        if stat == 0:
            print(f"rasdt_pwr 处理成功（状态码：{stat}）")
            if os.path.exists(data_in + ".bmp"):
                print(f"rasdt_pwr 处理成功，输出文件已生成：{data_in + '.bmp'}")
            else:
                print(f"未检测到生成的输出文件 {data_in + '.bmp'}！")
        elif stat == -1:
            # 状态码-1表示失败，主动抛出异常
            raise RuntimeError(f"rasdt_pwr执行失败（状态码：{stat}）")
        else:
            # 处理未知状态码（防止函数返回其他值）
            raise RuntimeError(f"rasdt_pwr返回未知状态码{stat}（预期0/-1）")

    except Exception as e:
        print(f"调用rasdt_pwr时发生未知错误：{e}")
        raise
    
    if os.path.isfile(report_file):
        os.remove(report_file)

def my_rasmph_pwr(data_in, pwr, width, cmap = "rmg.cm"):

    if pwr is None:
        pwr = "-"
    # rasmph_pwr
    try:
        print(f"========== rasmph_pwr 执行日志 ==========\n")
        stat = pg.rasmph_pwr(
            data = data_in,
            pwr = pwr,
            width = width,
            # max = max_value,
            # min = min_value,
            cmap = cmap,
            )
            # 根据返回状态码判断执行结果
        if stat == 0:
            print(f"rasmph_pwr 处理成功（状态码：{stat}）")
            if os.path.exists(data_in + ".bmp"):
                print(f"rasmph_pwr 处理成功，输出文件已生成：{data_in + '.bmp'}")
            else:
                print(f"未检测到生成的输出文件 {data_in + '.bmp'}！")
        elif stat == -1:
            # 状态码-1表示失败，主动抛出异常
            raise RuntimeError(f"rasmph_pwr执行失败（状态码：{stat}）")
        else:
            # 处理未知状态码（防止函数返回其他值）
            raise RuntimeError(f"rasmph_pwr返回未知状态码{stat}（预期0/-1）")

    except Exception as e:
        print(f"调用rasmph_pwr时发生未知错误：{e}")
        raise


    