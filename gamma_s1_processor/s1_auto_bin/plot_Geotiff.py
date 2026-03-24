from osgeo import gdal
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import cm
import matplotlib.colors as colors


def savefig(filename, vmin=None, vmax=1):
    """
    读取GeoTIFF文件并绘图，自动计算vmin/vmax（平均值±2倍标准差）
    背景设置为白色/透明，NaN区域填充为白色
    :param filename: TIFF文件路径
    :param vmin: 手动指定最小值（None则自动计算）
    :param vmax: 手动指定最大值（None则自动计算）
    """
    # 读取TIF图像
    try:
        gdal.UseExceptions()  # 启用异常处理
        dataset = gdal.Open(filename)
        if dataset is None:
            raise Exception("File cannot be opened")
    except Exception as e:
        print(f"Error opening file {filename}: {e}")
        return
    
    band = dataset.GetRasterBand(1)
    data = band.ReadAsArray()
    nodata_value = band.GetNoDataValue()

    # 处理无效值（NaN）- 修复原代码顺序问题（先处理NoData，再处理0值）
    if nodata_value is not None:
        data[data == nodata_value] = np.nan
        print(f"Masked {np.sum(np.isnan(data))} pixels as NaN (NoData value)")
    else:
        print("No NoData value found in TIFF file")
        # 使用容差匹配近似0的值为NaN
        mask = np.isclose(data, 0.0, atol=1e-6)
        data[mask] = np.nan
        print(f"Masked {np.sum(mask)} pixels as NaN (zero tolerance)")
    # 补充处理0值（原逻辑保留，避免漏处理）
    data[data == 0] = np.nan

    # 计算有效数据的平均值和标准差（排除NaN）
    valid_data = data[~np.isnan(data)]
    if len(valid_data) == 0:
        print("Error: No valid data found in the file!")
        return
    
    # 自动计算vmin/vmax（平均值±2倍标准差）
    if vmin is None:
        mean_val = np.mean(valid_data)
        std_val = np.std(valid_data)
        vmin = mean_val - 2 * std_val
        vmax = mean_val + 2 * std_val
        print(f"Auto-calculated range: vmin={vmin:.4f}, vmax={vmax:.4f} (mean±2*std)")

    # 计算经纬度网格
    x0, dx, dxdy, y0, dydx, dy = dataset.GetGeoTransform()
    nrows, ncols = data.shape
    lon = np.linspace(x0, x0 + dx * ncols, ncols)
    lat = np.linspace(y0, y0 + dy * nrows, nrows)

    # 设置绘图样式
    plt.rcParams['font.family'] = 'Arial'
    plt.rcParams['font.size'] = 12              # 默认字体大小
    plt.rcParams['font.weight'] = 'normal'
    plt.rcParams['axes.labelweight'] = 'bold'   # 坐标轴标签加粗
    plt.rcParams['axes.titleweight'] = 'bold'   # 标题加粗

    # ========== 核心修改：设置背景为白色/透明 ==========
    fig, ax = plt.subplots(facecolor='white')  # 画布背景设为白色
    ax.set_facecolor('white')                 # 坐标轴背景设为白色

    cmap = cm.coolwarm
    # 关键：设置NaN区域的填充色为白色（消除灰色）
    cmap.set_bad(color='white', alpha=1)       # alpha=1表示不透明，透明则设为0
    norm = colors.TwoSlopeNorm(vmin=vmin, vcenter=0, vmax=vmax)
    
    # 绘制图像
    pc = ax.imshow(data, norm=norm, cmap=cmap, 
                extent=[lon.min(), lon.max(), lat.min(), lat.max()])

    # 添加colorbar
    cbar = fig.colorbar(pc, ax=ax, shrink=0.3, extend='both')
    cbar.set_ticks([vmin, 0, vmax])  # 自定义刻度
    cbar.ax.set_yticklabels([f"{vmin:.2f}", "0", f"{vmax:.2f}"])  # 格式化刻度标签

    # 隐藏坐标轴（可选，如需显示则注释）
    # ax.set_xticks([])
    # ax.set_yticks([])
    # ax.spines['top'].set_visible(False)
    # ax.spines['right'].set_visible(False)
    # ax.spines['bottom'].set_visible(False)
    # ax.spines['left'].set_visible(False)

    # 确保所有元素都能显示
    plt.tight_layout()

    # 保存图像（如需透明背景，添加参数 transparent=True）
    png_filename = filename.rsplit('.', 1)[0] + ".png"
    pdf_filename = filename.rsplit('.', 1)[0] + ".pdf"
    # 白色背景保存（默认）
    plt.savefig(png_filename, dpi=300, bbox_inches='tight', facecolor='white')
    plt.savefig(pdf_filename, dpi=300, bbox_inches='tight', facecolor='white')
    # 如需透明背景，替换为下面两行：
    # plt.savefig(png_filename, dpi=300, bbox_inches='tight', transparent=True)
    # plt.savefig(pdf_filename, dpi=300, bbox_inches='tight', transparent=True)
    
    plt.close()  # 关闭画布释放内存
    print(f"Saved figure as {png_filename} and {pdf_filename}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage:")
        print("  自动计算范围: python plotgeotiff.py <filename>")
        print("  手动指定范围: python plotgeotiff.py <filename> <vmin> <vmax>")
        sys.exit(1)
    
    filename = sys.argv[1]
    vmin_input = None
    vmax_input = None
    
    # 处理命令行参数
    if len(sys.argv) >= 4:
        try:
            vmin_input = float(sys.argv[2])
            vmax_input = float(sys.argv[3])
            print(f"Using manually specified range: vmin={vmin_input}, vmax={vmax_input}")
        except ValueError:
            print("Error: vmin and vmax must be numeric values!")
            sys.exit(1)
    
    savefig(filename, vmin=vmin_input, vmax=vmax_input)