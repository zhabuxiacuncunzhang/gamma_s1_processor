from osgeo import gdal
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import cm
import matplotlib.colors as colors
import platform

def load_tiff_data(filename):
    """
    加载TIFF文件数据，返回数据数组、有效数据、地理范围
    :param filename: TIFF文件路径
    :return: data(含NaN)、valid_data(无NaN)、extent(地理范围)
    """
    try:
        gdal.UseExceptions()
        dataset = gdal.Open(filename)
        if dataset is None:
            raise Exception("文件无法打开")
        
        band = dataset.GetRasterBand(1)
        data = band.ReadAsArray()
        nodata_value = band.GetNoDataValue()

        # 处理无效值
        if nodata_value is not None:
            data[data == nodata_value] = np.nan
        else:
            mask = np.isclose(data, 0.0, atol=1e-6)
            data[mask] = np.nan
        data[data == 0] = np.nan

        # 提取有效数据
        valid_data = data[~np.isnan(data)]
        if len(valid_data) == 0:
            raise Exception("文件中无有效数据")

        # 计算地理范围
        x0, dx, _, y0, _, dy = dataset.GetGeoTransform()
        nrows, ncols = data.shape
        lon_min, lon_max = x0, x0 + dx * ncols
        lat_min, lat_max = y0, y0 + dy * nrows
        extent = (lon_min, lon_max, lat_min, lat_max)

        return data, valid_data, extent

    except Exception as e:
        print(f"加载文件 {filename} 出错: {e}")
        return None, None, None


def plot_two_tiffs(file1, file2, output_filename="output"):
    """
    绘制两个TIFF文件，用第二个文件计算vmin/vmax，标注统计信息
    :param file1: 第一个TIFF文件路径
    :param file2: 第二个TIFF文件路径（用于计算全局vmin/vmax）
    :param output_prefix: 输出图片前缀
    """
    # 加载两个文件的数据
    data1, valid1, extent1 = load_tiff_data(file1)
    data2, valid2, extent2 = load_tiff_data(file2)
    
    # if None in [data1, data2]:
    #     return

    # ========== 核心：用第二个文件计算全局vmin/vmax ==========
    mean2 = np.mean(valid2)
    std2 = np.std(valid2)
    vmin = mean2 - 2 * std2
    vmax = mean2 + 2 * std2
    # print(f"全局绘图范围（基于第二个文件）:")
    # print(f"  平均值: {mean2:.4f}, 标准差: {std2:.4f}")
    # print(f"  vmin: {vmin:.4f}, vmax: {vmax:.4f}")

    # 计算第一个文件的统计信息（用于标注）
    mean1 = np.mean(valid1)
    std1 = np.std(valid1)
    # print(f"\n第一个文件统计信息:")
    # print(f"  平均值: {mean1:.4f}, 标准差: {std1:.4f}")

    # 设置绘图样式
    system = platform.system()
    if system == 'Windows':
        plt.rcParams['font.family'] = 'Arial'
    elif system == 'Linux':
        plt.rcParams['font.family'] = 'DejaVu Sans'
    elif system == 'Darwin':  # MacOS
        plt.rcParams['font.family'] = 'Arial'
    else:
        plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.size'] = 10
    plt.rcParams['axes.labelweight'] = 'bold'
    plt.rcParams['axes.titleweight'] = 'bold'

    # ========== 修复布局警告：使用GridSpec自定义布局 ==========
    from matplotlib.gridspec import GridSpec
    
    # 创建网格布局：3行2列，colorbar占第三行整行
    fig = plt.figure(figsize=(12, 6), facecolor='white')
    gs = GridSpec(3, 2, figure=fig, 
                  height_ratios=[1, 1, 0.1],  # 前两行绘图，第三行colorbar
                  hspace=0.3, wspace=0.2)     # 调整子图间距
    
    # 创建两个子图
    ax1 = fig.add_subplot(gs[0:2, 0])  # 占据前两行第一列
    ax2 = fig.add_subplot(gs[0:2, 1])  # 占据前两行第二列
    ax1.set_facecolor('white')
    ax2.set_facecolor('white')

    # 配置colormap（白色背景，NaN区域白色）
    cmap = cm.coolwarm
    cmap.set_bad(color='white', alpha=1)
    norm = colors.TwoSlopeNorm(vmin=vmin, vcenter=0, vmax=vmax)

    # ========== 绘制第一个图 ==========
    im1 = ax1.imshow(data1, norm=norm, cmap=cmap, extent=extent1)
    ax1.set_title(f"File 1: {file1.split('/')[-1]}")
    # 标注统计信息（左上角，白色背景防止遮挡）
    text1 = f"Mean: {mean1:.4f}\nStd: {std1:.4f}"
    ax1.text(0.05, 0.95, text1, transform=ax1.transAxes, 
             verticalalignment='top', fontsize=8,
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    # ========== 绘制第二个图 ==========
    im2 = ax2.imshow(data2, norm=norm, cmap=cmap, extent=extent2)
    ax2.set_title(f"File 2: {file2.split('/')[-1]}")
    # 标注统计信息（左上角）
    text2 = f"Mean: {mean2:.4f}\nStd: {std2:.4f}"
    ax2.text(0.05, 0.95, text2, transform=ax2.transAxes, 
             verticalalignment='top', fontsize=8,
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))


    # ========== 修复colorbar布局 ==========
    # 创建colorbar轴（占据第三行整行）
    # cbar_ax = fig.add_subplot(gs[2, :])
    # cbar_ax.set_position([0.25, 0.05, 0.5, 0.03])  # [左, 下, 宽, 高]，核心调整
    cbar_ax = fig.add_axes([0.25, 0.03, 0.5, 0.03])
    cbar = fig.colorbar(im2, cax=cbar_ax, extend='both', orientation='horizontal', shrink=1, pad=0.0)
    cbar.set_ticks([vmin, 0, vmax])
    cbar.set_ticklabels([f"{vmin:.2f}", "0", f"{vmax:.2f}"])
    cbar.set_label('Value', fontweight='bold')

    # ========== 替代tight_layout：使用subplots_adjust手动调整布局 ==========
    fig.subplots_adjust(left=0.05, right=0.95, top=0.9, bottom=0.1)

    # 保存图片
    png_file = f"{output_filename}.png"
    pdf_file = f"{output_filename}.pdf"
    plt.savefig(png_file, dpi=300, bbox_inches='tight', facecolor='white')
    plt.savefig(pdf_file, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"\n图片已保存为: {png_file}, {pdf_file}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("使用方法:")
        print("  python plot_two_tiffs.py <第一个TIFF文件路径> <第二个TIFF文件路径>")
        sys.exit(1)
    
    file1_path = sys.argv[1]
    file2_path = sys.argv[2]
    plot_two_tiffs(file1_path, file2_path)