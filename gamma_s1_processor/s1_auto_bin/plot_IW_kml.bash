#!/bin/bash
# =============================================================================
# 脚本名称: plot_kml.sh
# 作    者: Xuesong Zhang
# 创建日期: 2024-03-21
# 最后修改: 2024-03-21
# 功    能: 自动批量转换KML文件为GMT格式并绘制地图，支持多边形填充和标签标注
#           
# 使用方法: ./plot_kml.sh
# 依赖工具: GMT, awk, bc
#           
# 输入文件: 当前目录下所有 .kml 文件
# 输出文件: [第一个KML文件名前8字符]_[日期].pdf 和 .png
#           
# 功能说明:
#   1. 自动搜索当前目录下所有KML文件
#   2. 使用kml2gmt转换为GMT格式
#   3. 提取每个多边形的标签信息（如 "Subswath: IW1 Burst: 1"）
#   4. 基于burst序号进行全局去重，避免重复标注
#   5. 用不同颜色区分不同KML文件的多边形
#   6. 添加海岸线和图框
#   7. 输出高分辨率PDF和PNG图片
#           
# 修改历史:
#   v1.0 - 2024-03-21 - 初始版本
#   - 实现基本功能
#   - 添加全局去重机制
#   - 优化标签提取速度
# =============================================================================

# 检查GMT是否安装
if ! command -v gmt &> /dev/null; then
    echo "错误: GMT未安装"
    exit 1
fi

# 获取当前目录下所有.kml文件
kml_files=(*.kml)
file_count=${#kml_files[@]}

if [ $file_count -eq 0 ]; then
    echo "错误: 当前目录下没有找到KML文件"
    exit 1
fi

echo "找到 $file_count 个KML文件:"
for file in "${kml_files[@]}"; do
    echo "  - $file"
done

# 设置临时文件前缀
TMP_PREFIX="gmt_all_$$"

# 预定义颜色
colors=("red" "blue" "darkgreen" "purple" "orange" "brown")
fill_colors=("lightred" "lightblue" "lightgreen" "mediumorchid" "orange" "brown")

# 处理每个KML文件
file_index=1
all_gmt_files=""
declare -A file_names

# 全局去重数组（跨文件）
declare -A global_seen

for kml_file in "${kml_files[@]}"; do
    echo "处理文件 $file_index: $kml_file"
    
    # 转换为GMT格式
    gmt kml2gmt "$kml_file" > "${TMP_PREFIX}_file${file_index}.gmt" 2>/dev/null
    
    if [ -s "${TMP_PREFIX}_file${file_index}.gmt" ]; then
        all_gmt_files="$all_gmt_files ${TMP_PREFIX}_file${file_index}.gmt"
        file_names[$file_index]=$(basename "$kml_file" .kml)
        
        # 提取
        awk '
        /^> -L"/ {
            # 提取 -L"..." 中的内容
            match($0, /-L"([^"]*)"/)
            label = substr($0, RSTART+3, RLENGTH-4)
            getline coord
            if (coord !~ /^>/) {
                print coord, label
            }
        }' "${TMP_PREFIX}_file${file_index}.gmt" > "${TMP_PREFIX}_raw_labels${file_index}.txt"
        
        # 基于burst序号去重
        if [ -s "${TMP_PREFIX}_raw_labels${file_index}.txt" ]; then
            
            # 先对当前文件去重（基于burst序号）
            sort -u "${TMP_PREFIX}_raw_labels${file_index}.txt" > "${TMP_PREFIX}_file_unique.txt"
            
            # 然后基于全局去重
            > "${TMP_PREFIX}_labels${file_index}.txt"
            while IFS= read -r line; do
                if [[ $line =~ Burst:\ ([0-9]+) ]]; then
                    burst_num="${BASH_REMATCH[1]}"
                    if [[ $line =~ IW([0-9]+) ]]; then
                        iw_num="${BASH_REMATCH[1]}"
                        key="IW${iw_num}_Burst_${burst_num}"
                    else
                        key="Burst_${burst_num}"
                    fi
                    
                    if [[ -z ${global_seen[$key]} ]]; then
                        global_seen[$key]=1
                        echo "$line" >> "${TMP_PREFIX}_labels${file_index}.txt"
                    fi
                else
                    # 如果没有burst信息，默认保留（但也要全局去重）
                    if [[ -z ${global_seen[$line]} ]]; then
                        global_seen[$line]=1
                        echo "$line" >> "${TMP_PREFIX}_labels${file_index}.txt"
                    fi
                fi
            done < "${TMP_PREFIX}_file_unique.txt"
            
            raw_count=$(wc -l < "${TMP_PREFIX}_raw_labels${file_index}.txt")
            unique_count=$(wc -l < "${TMP_PREFIX}_labels${file_index}.txt")
            echo "  文件 $file_index: 提取了 $raw_count 个标签，去重后 $unique_count 个"
            
            rm -f "${TMP_PREFIX}_file_unique.txt"
        else
            echo "  文件 $file_index: 没有提取到标签"
        fi
        
        file_index=$((file_index + 1))
    else
        echo "  警告: $kml_file 转换失败或为空"
        rm -f "${TMP_PREFIX}_file${file_index}.gmt"
    fi
done

actual_count=$((file_index - 1))

if [ $actual_count -eq 0 ]; then
    echo "错误: 没有成功转换的KML文件"
    rm -f ${TMP_PREFIX}_*
    exit 1
fi

echo "成功转换 $actual_count 个文件"

# 计算地图范围
echo "计算地图范围..."
gmt info $all_gmt_files -C > "${TMP_PREFIX}_bounds.txt"
xmin=$(awk '{print $1}' "${TMP_PREFIX}_bounds.txt")
xmax=$(awk '{print $2}' "${TMP_PREFIX}_bounds.txt")
ymin=$(awk '{print $3}' "${TMP_PREFIX}_bounds.txt")
ymax=$(awk '{print $4}' "${TMP_PREFIX}_bounds.txt")

# 添加边界余量（10%）
xrange=$(echo "$xmax - $xmin" | bc -l)
yrange=$(echo "$ymax - $ymin" | bc -l)
xmin=$(echo "$xmin - $xrange * 0.1" | bc -l)
xmax=$(echo "$xmax + $xrange * 0.1" | bc -l)
ymin=$(echo "$ymin - $yrange * 0.1" | bc -l)
ymax=$(echo "$ymax + $yrange * 0.1" | bc -l)

output_base="${kml_files[0]:0:8}"
echo "输出文件: ${output_base}.pdf 和 ${output_base}.png"

# 开始绘图
echo "开始绘图..."

gmt begin ${output_base} PNG,pdf
# 创建底图
gmt basemap -R$xmin/$xmax/$ymin/$ymax -JM15c -BWSen -Bxa1 -Bya1 --FONT_ANNOT_PRIMARY=10p

# 添加海岸线
gmt coast -W0.25p,gray50 -Glightgray -Slightblue -Da -Ia/1p,black,solid 

# 绘制每个文件的多边形
for ((i=1; i<=actual_count; i++)); do
    if [ -f "${TMP_PREFIX}_file${i}.gmt" ]; then
        color_idx=$(( (i-1) % ${#colors[@]} ))
        
        # 绘制多边形（填充+边框）
        echo "  绘制文件 $i: ${file_names[$i]} (颜色: ${colors[$color_idx]})"
        gmt plot "${TMP_PREFIX}_file${i}.gmt" -G${fill_colors[$color_idx]} -W1p,${colors[$color_idx]} -t50 
    fi
done

total_labels=0
for ((i=1; i<=actual_count; i++)); do
    if [ -f "${TMP_PREFIX}_labels${i}.txt" ] && [ -s "${TMP_PREFIX}_labels${i}.txt" ]; then
        count=$(wc -l < "${TMP_PREFIX}_labels${i}.txt")
        total_labels=$((total_labels + count))
        
        # 绘制标签，带白色背景框
        gmt text "${TMP_PREFIX}_labels${i}.txt" -F+f8p,Helvetica,black+jCM -Gwhite -W0.5p,gray30 -C2p -D0/0c
    fi
done

gmt end

# 清理临时文件
# echo "清理临时文件..."
rm -f ${TMP_PREFIX}_* gmt.history

# echo ""
echo "================================="
echo "绘图完成!"
echo "================================="
# echo "处理的KML文件: $actual_count 个"
# echo "绘制的标签数: $total_labels 个"
# echo "生成的文件:"
# echo "  - ${output_base}.pdf"
# echo "  - ${output_base}.png"
# echo "================================="