import os
import json
import requests
import cloudscraper
import yaml
import gzip
import logging
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path='config.yaml'):
    with open(config_path) as f:
        return yaml.safe_load(f)

def process_sitemap(url, visited=None, max_depth=10):
    """
    处理 sitemap URL，支持递归处理 sitemap 索引文件
    
    Args:
        url: sitemap URL
        visited: 已访问的 URL 集合，用于防止循环
        max_depth: 最大递归深度，防止无限递归
    
    Returns:
        所有页面 URL 列表
    """
    if visited is None:
        visited = set()
    
    if max_depth <= 0:
        logging.warning(f"达到最大递归深度，停止处理: {url}")
        return []
    
    if url in visited:
        logging.warning(f"检测到循环引用，跳过: {url}")
        return []
    
    visited.add(url)
    
    try:
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url, timeout=10)
        response.raise_for_status()

        content = response.content
        # 智能检测gzip格式
        if content[:2] == b'\x1f\x8b':  # gzip magic number
            content = gzip.decompress(content)

        # 检测是否是 sitemap 索引文件
        if b'<sitemapindex' in content or b'<sitemapindex>' in content:
            logging.info(f"检测到 sitemap 索引文件: {url}")
            sitemap_urls = parse_sitemap_index(content)
            all_urls = []
            for sitemap_url in sitemap_urls:
                # 递归处理每个 sitemap URL
                urls = process_sitemap(sitemap_url, visited, max_depth - 1)
                all_urls.extend(urls)
            return all_urls
        elif b'<urlset' in content:
            # 普通的 sitemap 文件，包含实际页面链接
            return parse_xml_urlset(content)
        else:
            # 文本格式的 sitemap
            return parse_txt(content.decode('utf-8'))
    except requests.RequestException as e:
        logging.error(f"Error processing {url}: {str(e)}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error processing {url}: {str(e)}")
        return []

def parse_sitemap_index(content):
    """
    解析 sitemap 索引文件，提取其中的 sitemap URL
    
    Args:
        content: XML 内容（bytes）
    
    Returns:
        sitemap URL 列表
    """
    sitemap_urls = []
    soup = BeautifulSoup(content, 'xml')
    # 查找 sitemapindex 中的 sitemap 标签
    for sitemap in soup.find_all('sitemap'):
        loc = sitemap.find('loc')
        if loc:
            url = loc.get_text().strip()
            if url:
                sitemap_urls.append(url)
                logging.debug(f"发现 sitemap: {url}")
    return sitemap_urls

def parse_xml_urlset(content):
    """
    解析包含实际页面链接的 sitemap 文件（urlset）
    
    Args:
        content: XML 内容（bytes）
    
    Returns:
        页面 URL 列表
    """
    urls = []
    soup = BeautifulSoup(content, 'xml')
    for loc in soup.find_all('loc'):
        url = loc.get_text().strip()
        if url:
            urls.append(url)
    return urls

def parse_txt(content):
    return [line.strip() for line in content.splitlines() if line.strip()]

def save_latest(site_name, new_urls):
    base_dir = Path('latest')
    
    # 创建latest目录（与日期目录同级）
    latest_dir = base_dir
    latest_dir.mkdir(parents=True, exist_ok=True)
    
    # 保存latest.json
    latest_file = latest_dir / f'{site_name}.json'
    with open(latest_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(new_urls))

def save_diff(site_name, new_urls):
    base_dir = Path('diff')
        
    # 创建日期目录
    today = datetime.now().strftime('%Y%m%d')
    date_dir = base_dir / today
    date_dir.mkdir(parents=True, exist_ok=True)
    
    # 保存当日新增数据
    file_path = date_dir / f'{site_name}.json'
    mode = 'a' if file_path.exists() else 'w'
    with open(file_path, mode, encoding='utf-8') as f:
        if mode == 'a':
            f.write('\n--------------------------------\n')  # 添加分隔符
        f.write('\n'.join(new_urls) + '\n')  # 确保每个URL后都有换行

def compare_data(site_name, new_urls):
    latest_file = Path('latest') / f'{site_name}.json'
    
    if not latest_file.exists():
        return []
        
    with open(latest_file) as f:
        last_urls = set(f.read().splitlines())
    
    return [url for url in new_urls if url not in last_urls]

def send_feishu_notification(new_urls, config, site_name):
    if not new_urls:
        return
    
    webhook_url = config['feishu']['webhook_url']
    secret = config['feishu'].get('secret')
    
    message = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"🎮 {site_name} 游戏上新通知"},
                "template": "green"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**今日新增 {len(new_urls)} 款游戏**\n\n" + "\n".join(f"• {url}" for url in new_urls[:10])
                    }
                }
            ]
        }
    }
    
    for attempt in range(3):  # 重试机制
        try:
            resp = requests.post(webhook_url, json=message)
            resp.raise_for_status()
            logging.info("飞书通知发送成功")
            return
        except requests.RequestException as e:
            logging.error(f"飞书通知发送失败: {str(e)}")
            if attempt < 2:
                logging.info("重试发送通知...")

def main(config_path='config.yaml'):
    config = load_config(config_path)
    
    for site in config['sites']:
        if not site['active']:
            continue
            
        logging.info(f"处理站点: {site['name']}")
        all_urls = []
        for sitemap_url in site['sitemap_urls']:
            logging.info(f"  处理 sitemap: {sitemap_url}")
            urls = process_sitemap(sitemap_url)
            logging.info(f"  获取到 {len(urls)} 个链接")
            all_urls.extend(urls)
            
        # 去重处理
        unique_urls = list({url: None for url in all_urls}.keys())
        logging.info(f"站点 {site['name']} 共获取 {len(unique_urls)} 个唯一链接")
        new_urls = compare_data(site['name'], unique_urls)
        if new_urls:
            logging.info(f"站点 {site['name']} 发现 {len(new_urls)} 个新增链接")
        
        save_latest(site['name'], unique_urls)
        if new_urls:
            save_diff(site['name'], new_urls)
            send_feishu_notification(new_urls, config, site['name'])
            
        # 清理旧数据
        cleanup_old_data(site['name'], config)

def cleanup_old_data(site_name, config):
    data_dir = Path('diff')
    if not data_dir.exists():
        return
        
    # 获取配置中的保留天数
    retention_days = config.get('retention_days', 7)
    cutoff = datetime.now() - timedelta(days=retention_days)
    
    # 遍历所有日期文件夹
    for date_dir in data_dir.glob('*'):
        if not date_dir.is_dir():
            continue
            
        try:
            # 解析文件夹名称为日期
            dir_date = datetime.strptime(date_dir.name, '%Y%m%d')
            if dir_date < cutoff:
                # 删除整个日期文件夹
                for f in date_dir.glob('*.json'):
                    f.unlink()
                date_dir.rmdir()
                logging.info(f"已删除过期文件夹: {date_dir.name}")
        except ValueError:
            # 忽略非日期格式的文件夹
            continue
        except Exception as e:
            logging.error(f"删除文件夹时出错: {str(e)}")

if __name__ == '__main__':
    main()
