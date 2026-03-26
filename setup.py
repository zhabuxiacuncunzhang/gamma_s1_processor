from setuptools import setup, find_packages
import os
import sys

# 检查Python版本
if sys.version_info < (3, 7):
    sys.exit("本包需要Python 3.7及以上版本")

# 读取README
with open(os.path.join(os.path.dirname(__file__), 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="gamma_s1_processor",  # PyPI包名（小写、短横线）
    version="0.1.0",            # 版本号
    author="Xuesong Zhang",
    author_email="940280613@qq.com",
    description="Sentinel-1数据GAMMA处理自动化工具",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/zhabuxiacuncunzhang/gamma_s1_processor",  # 项目地址
    packages=find_packages(),
    include_package_data=True,  # 包含非代码文件（需配合MANIFEST.in）
    install_requires=[          # 依赖包
        "pyyaml",
        "matplotlib",
        "Pillow",
        "argparse",
        "glob2",
        "gdal",
        "numpy",
        "pandas",
        "psutil",
        "shapely",
        "scipy",
        "sentineleof",
        "cartopy"
    ],
    entry_points={              # 命令行入口（可选，方便终端调用）
        'console_scripts': [
            'gamma_s1_processor = gamma_s1_processor:main',
        ]
    },
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix"
    ],
)