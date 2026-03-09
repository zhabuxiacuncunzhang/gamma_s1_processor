#!/bin/bash
# =============================================================================
# 脚本名称: plot_IW_kml_enhance.bash
# 作    者: Modified based on original version
# 功能说明: 绘制IW KML文件，支持：
#           1. 自动读取目录内所有.kml文件
#           2. 命令行指定额外的KML文件（-f 参数）
#           3. 指定输出目录（-o 参数）
#           4. 完全保留原标签处理逻辑（去重、提取、标注）
# 使用方法: 
#   基础用法（同原版）: ./plot_IW_kml_enhance.bash
#   指定额外文件+输出目录: ./plot_IW_kml_enhance.bash -f /path/extra.kml -o /output/dir
#   仅指定输出目录: ./plot_IW_kml_enhance.bash -o ./output
#   仅指定额外文件: ./plot_IW_kml_enhance.bash -f file1.kml -f file2.kml
# =============================================================================

# 初始化变量
extra_kml_files=()  # 存储命令行指定的额外KML文件
output_dir="."      # 默认输出目录为当前目录
declare -A seen_bursts  # 保留原全局去重字典（Burst/IW）

# 显示帮助信息
show_help() {
    echo "用法: $0 [选项]"
    echo "选项:"
    echo "  -f <file>   指定额外的KML文件（可多次使用，补充目录内文件）"
    echo "  -o <dir>    指定输出目录（不存在则自动创建）"
    echo "  -h          显示帮助信息"
    exit 0
}

# 解析命令行参数
while getopts "f:o:h" opt; do
    case $opt in
        f)
            # 检查指定的文件是否存在且是.kml文件
            if [ -f "$OPTARG" ] && [[ "$OPTARG" == *.kml ]]; then
                extra_kml_files+=("$OPTARG")
                echo "已添加命令行指定的KML文件: $OPTARG"
            else
                echo "警告: 文件 $OPTARG 不存在或不是.kml文件，已忽略"
            fi
            ;;
        o)
            output_dir="$OPTARG"
            # 创建输出目录（递归创建，忽略已存在）
            # mkdir -p "$output_dir"
            # if [ ! -d "$output_dir" ]; then
            #     echo "错误: 无法创建输出目录 $output_dir"
            #     exit 1
            # fi
            echo "输出目录已设置为: $output_dir"
            ;;
        h)
            show_help
            ;;
        \?)
            echo "错误: 无效的选项 -$OPTARG" >&2
            show_help
            ;;
        :)
            echo "错误: 选项 -$OPTARG 需要指定参数" >&2
            show_help
            ;;
    esac
done

# 1. 收集所有待处理的KML文件（目录内 + 命令行指定）
# 读取当前目录下的.kml文件
dir_kml_files=($(ls *.kml 2>/dev/null))
# 初始化all_kml_files为目录内文件（保证排在最前面）
all_kml_files=("${dir_kml_files[@]}")
# 遍历命令行指定的文件：只添加「不在目录文件中」的文件，追加到列表最后
for extra_file in "${extra_kml_files[@]}"; do
    # 检查文件是否已在目录列表中，不在则追加到最后
    if [[ ! " ${dir_kml_files[@]} " =~ " ${extra_file} " ]]; then
        all_kml_files+=("$extra_file")
    fi
done

# 检查是否有可用的KML文件
if [ ${#all_kml_files[@]} -eq 0 ]; then
    echo "错误: 未找到任何有效的KML文件（目录内无.kml，且命令行未指定有效文件）"
    exit 1
fi

echo "========================================"
echo "共找到 ${#all_kml_files[@]} 个KML文件待处理:"
for kml in "${all_kml_files[@]}"; do
    echo "  - $kml"
done
echo "========================================"

# 2. 转换KML为GMT格式（临时文件）
temp_gmt_files=()
for idx in "${!all_kml_files[@]}"; do
    kml_file="${all_kml_files[$idx]}"
    temp_gmt="tmp_${idx}.gmt"

    # 转换KML到GMT
    gmt kml2gmt "$kml_file" > "$temp_gmt" 2>/dev/null
    if [ -s "$temp_gmt" ]; then
        temp_gmt_files+=("$temp_gmt")
        echo "已转换: $kml_file -> $temp_gmt"
    else
        echo "警告: $kml_file 转换失败或为空，跳过该文件"
        rm -f "$temp_gmt"
    fi
done

# 检查转换后的GMT文件是否有效
if [ ${#temp_gmt_files[@]} -eq 0 ]; then
    echo "错误: 所有KML文件转换失败，无有效数据可绘图"
    exit 1
fi

# 3. 计算地图范围（添加10%余量）
bounds=$(gmt info "${temp_gmt_files[@]}" -C)
xmin=$(echo $bounds | awk '{print $1}')
xmax=$(echo $bounds | awk '{print $2}')
ymin=$(echo $bounds | awk '{print $3}')
ymax=$(echo $bounds | awk '{print $4}')

# 计算10%余量
xrange=$(echo "$xmax - $xmin" | bc)
yrange=$(echo "$ymax - $ymin" | bc)
xmin_new=$(echo "$xmin - 0.1 * $xrange" | bc)
xmax_new=$(echo "$xmax + 0.1 * $xrange" | bc)
ymin_new=$(echo "$ymin - 0.1 * $yrange" | bc)
ymax_new=$(echo "$ymax + 0.1 * $yrange" | bc)

# 4. 设置输出文件名（保留原命名规则，调整输出路径）
base_name=$(basename "${dir_kml_files[0]}" .kml | cut -c1-8)
output_base="${output_dir}/${base_name}"

# 5. 开始绘图（GMT命令行）
echo "开始绘图，输出文件: ${output_base}.pdf / ${output_base}.png"

# 初始化GMT会话
gmt begin "${output_base}" pdf,png
    # 设置底图
    gmt basemap -R${xmin_new}/${xmax_new}/${ymin_new}/${ymax_new} -JM15c -BWSen -Bxa1 -Bya1 --FONT_ANNOT_PRIMARY=10p
    
    # 添加海岸线和地理要素（保留原参数）
    gmt coast -W0.25p,gray50 -Glightgray -Slightblue -Da -Ia/1p,black
    
    # 预定义颜色列表（与原脚本一致）
    colors=("red" "blue" "darkgreen" "purple" "orange" "brown")
    fill_colors=("lightred" "lightblue" "lightgreen" "mediumorchid" "orange" "brown")
    
    # 绘制每个GMT文件的多边形（保留原标签处理逻辑）
    for idx in "${!temp_gmt_files[@]}"; do
        gmt_file="${temp_gmt_files[$idx]}"
        color_idx=$((idx % ${#colors[@]}))
        line_color=${colors[$color_idx]}
        fill_color=${fill_colors[$color_idx]}
        
        # 绘制多边形（填充+边框，50%透明度）
        gmt plot "$gmt_file" -G$fill_color -W1p,$line_color -t50
        
        # ========== 完全保留你原有的标签处理逻辑 ==========
        # 提取标签（-L"..."中的内容 + 对应坐标）
        awk -F'"' '
            /^> -L"/ {
                label = $2
                getline
                if ($0 !~ /^>/) {
                    print $0, label
                }
            }
        ' "$gmt_file" > tmp_raw_labels.txt
        
        # 基于Burst/IW编号全局去重（核心保留逻辑）
        while IFS= read -r line; do
            # 提取Burst和IW编号
            burst=$(echo "$line" | grep -o 'Burst: [0-9]*' | awk '{print $2}')
            iw=$(echo "$line" | grep -o 'IW[0-9]*' | head -1 | sed 's/IW//')
            
            if [ -n "$burst" ] && [ -n "$iw" ]; then
                key="IW${iw}_Burst_${burst}"
            elif [ -n "$burst" ]; then
                key="Burst_${burst}"
            else
                key="$line"
            fi
            
            # 全局去重，只保留首次出现的标签
            if [ -z "${seen_bursts[$key]}" ]; then
                seen_bursts[$key]=1
                echo "$line" >> tmp_deduped_labels.txt
            fi
        done < tmp_raw_labels.txt
        
        # 绘制去重后的标签（保留原样式）
        if [ -s tmp_deduped_labels.txt ]; then
            gmt text tmp_deduped_labels.txt -F+f8p,Helvetica,black+jCM -Gwhite -W0.5p,gray30 -C2p
        fi
        
        # 清理临时标签文件
        rm -f tmp_raw_labels.txt tmp_deduped_labels.txt
    done
gmt end

# 6. 清理临时文件
rm -f "${temp_gmt_files[@]}"
echo "绘图完成！临时文件已清理"
echo "最终文件路径:"
echo "  - PDF: ${output_base}.pdf"
echo "  - PNG: ${output_base}.png"