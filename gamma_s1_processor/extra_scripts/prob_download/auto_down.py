#!/usr/bin/env python3
import os
import sys
import time
import subprocess
from pathlib import Path
import requests
from bs4 import BeautifulSoup

def load_file_list(filename):
    """加载文件列表"""
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def get_online_files(base_url):
        """获取在线文件列表"""
        print("正在获取文件列表...")
        try:
            session = requests.Session()
            response = session.get(base_url, timeout=30)
            response.raise_for_status()
            
            # 解析HTML获取文件链接
            soup = BeautifulSoup(response.content, 'html.parser')
            files = []
            
            # 查找所有链接，过滤出EOF文件
            for link in soup.find_all('a'):
                href = link.get('href', '')
                if href and 'S1' in href and '.EOF' in href:
                    files.append(href.strip())
            
            return sorted(set(files))  # 去重并排序
            
        except requests.exceptions.RequestException as e:
            print(f"获取文件列表失败: {e}")
            return []

def download_file(base_url, filename, download_dir, username, password):
    """下载单个文件"""
    url = f"{base_url}{filename}"
    output_path = os.path.join(download_dir, filename)
    
    cmd = [
        'wget', '--no-check-certificate',
        '--auth-no-challenge',
        '--user', username,
        '--password', password,
        '--output-document', output_path,
        url
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            # 检查文件大小
            if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
                return True
        return False
    except:
        return False

def main():
    # 配置
    BASE_URL = "https://s1qc.asf.alaska.edu/aux_poeorb/"
    USERNAME = "Zhang_Xuesong"  # 替换为你的用户名
    PASSWORD = "Snzzbyc123"  # 替换为你的密码
    LOCAL_LIST = "files"
    DOWNLOAD_DIR = "./prob/"
    
    # 创建目录
    Path(DOWNLOAD_DIR).mkdir(exist_ok=True)
    
    # 获取文件列表
    local_files = load_file_list(LOCAL_LIST)
    online_files = get_online_files(BASE_URL)
    
    if not online_files:
        print("无法获取在线文件列表")
        return
    
    # 计算缺失文件
    missing = [f for f in online_files if f not in local_files]
    
    if not missing:
        print("没有需要下载的新文件")
        return
    
    print(f"需要下载 {len(missing)} 个文件")
    
    # 下载
    success_count = 0
    for i, filename in enumerate(missing, 1):
        print(f"[{i}/{len(missing)}] 下载 {filename}")
        
        if download_file(BASE_URL, filename, DOWNLOAD_DIR, USERNAME, PASSWORD):
            # 更新本地列表
            with open(LOCAL_LIST, 'a') as f:
                f.write(filename + '\n')
            success_count += 1
            print(f"  成功")
        else:
            print(f"  失败")
        
        time.sleep(1)  # 礼貌间隔
    
    print(f"\n下载完成！成功: {success_count}/{len(missing)}")

if __name__ == "__main__":
    main()