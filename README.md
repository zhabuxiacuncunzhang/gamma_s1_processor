# s1-gamma-processor

[![Python Version](https://img.shields.io/badge/python-3.6%2B-blue)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Sentinel-1（哨兵1号）SAR数据自动化GAMMA处理工具，支持从S1影像数据到干涉图生成的全流程自动化，无需手动编写GAMMA命令，通过配置文件即可完成批量处理。

## 功能特点
- 🚀 全流程自动化：支持S1数据解压、配准、干涉处理等核心步骤
- ⚙️ 配置驱动：通过YAML配置文件统一管理路径和处理参数
- 📝 完善日志：记录每一步处理过程，便于问题排查
- 📊 可视化输出：自动生成KML区域图、SAR数据预览图
- 📦 可安装部署：支持pip安装，可作为命令行工具使用

## 环境要求
### 基础环境
- Python 3.7+
- Linux系统
- GAMMA软件 2024以上
- 足够的磁盘空间（建议≥100GB）

### 核心依赖
- **GAMMA Remote Sensing**：商业SAR处理软件（需自行购买授权，版本≥2024）
- Python依赖：见`setup.py`

## 安装方法

### 1. 安装GAMMA软件
#### 版本要求202412以上
### 2. pip安装gamma_s1_processor
cd gamma_s1_processor
pip install .
### 3. 建议先安装GDAL 
mamba install gdal
### 4. 生成配置文件
gamma_s1_processor 会自动在当前目录生成配置文件
在 templates 文件夹中也有配置文件的模板
### 5. 执行方式
gamma_s1_processor gamma_s1_config.yml start_step end_step  
gamma_s1_processor gamma_s1_config.yml 1 1  
gamma_s1_processor gamma_s1_config.yml 1 5  

