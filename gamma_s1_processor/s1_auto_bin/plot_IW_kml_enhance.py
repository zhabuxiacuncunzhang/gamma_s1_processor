# -*- coding: utf-8 -*-
import argparse
import re
from pathlib import Path
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from shapely.geometry import Polygon
import xml.etree.ElementTree as ET
import warnings
warnings.filterwarnings("ignore")

# ======================
# 配置
# ======================
LINE_COLORS = ["red", "blue", "darkgreen", "purple", "orange", "brown"]
FILL_COLORS = ["lightcoral", "lightblue", "lightgreen", "mediumorchid", "orange", "sandybrown"]
TRANSPARENCY = 0.5
PADDING = 0.1
FONT_LABEL = 8

# ======================
# 命令行解析
# ======================
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", action="append", default=[], help="额外KML文件")
    parser.add_argument("-o", "--output", default=".", help="输出目录")
    return parser.parse_args()

# ======================
# 读取 KML
# ======================
def read_kml_native(kml_path):
    geoms = []
    labels = []
    tree = ET.parse(kml_path)
    root = tree.getroot()
    ns = {"kml": "http://www.opengis.net/kml/2.2"}

    for pm in root.findall(".//kml:Placemark", ns):
        name_elem = pm.find("kml:name", ns)
        label = name_elem.text.strip() if (name_elem is not None and name_elem.text) else ""
        coords_elem = pm.find(".//kml:coordinates", ns)
        if coords_elem is None:
            continue

        coords_str = coords_elem.text.strip()
        coords = []
        for part in coords_str.split():
            try:
                lon, lat, _ = part.split(",")
                coords.append((float(lon), float(lat)))
            except:
                continue

        if len(coords) >= 3:
            geom = Polygon(coords)
            geoms.append(geom)
            if label:
                cx, cy = geom.centroid.x, geom.centroid.y
                labels.append((cx, cy, label))
    return geoms, labels

# ======================
# 计算范围
# ======================
def get_bounds(all_geoms):
    bounds = [g.bounds for g in all_geoms]
    x1 = min(b[0] for b in bounds)
    y1 = min(b[1] for b in bounds)
    x2 = max(b[2] for b in bounds)
    y2 = max(b[3] for b in bounds)
    dx, dy = (x2 - x1) * PADDING, (y2 - y1) * PADDING
    return [x1 - dx, x2 + dx, y1 - dy, y2 + dy]

# ======================
# 标签去重
# ======================
def dedup(labels):
    seen = set()
    out = []
    for x, y, t in labels:
        key = t
        b = re.search(r"Burst[:\s]*(\d+)", t)
        iw = re.search(r"IW(\d+)", t)
        if b and iw:
            key = f"IW{iw.group(1)}_Burst{b.group(1)}"
        elif b:
            key = f"Burst{b.group(1)}"
        if key not in seen:
            seen.add(key)
            out.append((x, y, t))
    return out

# ======================
# ✅ 核心：完全对齐 bash 逻辑
# 1. 默认加载当前目录所有 .kml
# 2. -f / files 追加额外文件
# ======================
def main(files=None, output=None):
    # ===== 命令行模式 =====
    if files is None and output is None:
        args = parse_args()
        extra_files = args.file
        output = args.output
    else:
        extra_files = files or []

    # ===== 1. 加载当前目录所有 kml（和bash一样）=====
    current_kml = list(Path(".").glob("*.kml"))
    current_kml = [str(f) for f in current_kml]

    # ===== 2. 追加 -f 指定的文件（和bash一样）=====
    all_kml = current_kml + extra_files

    # ===== 去重、过滤 =====
    all_kml = list(dict.fromkeys(all_kml))
    all_kml = [f for f in all_kml if Path(f).exists() and f.endswith(".kml")]

    if not all_kml:
        print("❌ 未找到KML文件")
        return

    # ===== 绘图逻辑不变 =====
    all_geoms = []
    all_labels = []
    file_geoms = []

    for f in all_kml:
        g, l = read_kml_native(f)
        file_geoms.append(g)
        all_geoms.extend(g)
        all_labels.extend(l)

    all_labels = dedup(all_labels)
    bounds = get_bounds(all_geoms)

    plt.figure(figsize=(15, 10))
    proj = ccrs.Mercator()
    ax = plt.axes(projection=proj)
    ax.set_extent(bounds, crs=ccrs.PlateCarree())

    gl = ax.gridlines(draw_labels=True, linewidth=0.3, color='gray', alpha=0.5, linestyle='-')
    gl.top_labels = False
    gl.right_labels = False
    gl.bottom_labels = True
    gl.left_labels = True
    gl.xlabel_style = {'size': 10}
    gl.ylabel_style = {'size': 10}

    ax.add_feature(cfeature.LAND)
    ax.add_feature(cfeature.OCEAN)
    ax.add_feature(cfeature.COASTLINE.with_scale("10m"), linewidth=0.25, edgecolor="gray")
    ax.add_feature(cfeature.RIVERS.with_scale("10m"), linewidth=1, color="black")

    for idx, geoms in enumerate(file_geoms):
        c = LINE_COLORS[idx % len(LINE_COLORS)]
        f = FILL_COLORS[idx % len(FILL_COLORS)]
        ax.add_geometries(geoms, ccrs.PlateCarree(), facecolor=f, edgecolor=c, linewidth=1, alpha=TRANSPARENCY)

    for x, y, t in all_labels:
        ax.text(x, y, t, transform=ccrs.PlateCarree(), fontsize=FONT_LABEL, ha="center", va="center",
                bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor="gray", linewidth=0.5))

    Path(output).mkdir(exist_ok=True)
    name = Path(all_kml[0]).stem[:8]
    plt.savefig(Path(output) / f"{name}.pdf", dpi=300, bbox_inches="tight")
    plt.savefig(Path(output) / f"{name}.png", dpi=300, bbox_inches="tight")
    #print(f"✅ 绘图完成！输出目录：{output}")

if __name__ == "__main__":
    main()