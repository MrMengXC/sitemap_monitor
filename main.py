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
from urllib.parse import urlparse, urljoin

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path='config.yaml'):
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Prefer GitHub Actions env vars for sensitive config
    # Repository Variables: FEISHU_WEBHOOK
    feishu_webhook = os.getenv("FEISHU_WEBHOOK")
    if feishu_webhook:
        config.setdefault("feishu", {})
        config["feishu"]["webhook_url"] = feishu_webhook.strip()

    return config

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

# 常见域名列表（需要过滤的域名及其子域名）
COMMON_DOMAINS = {
    'facebook.com', 'fb.com', 'facebook.net',
    'google.com', 'google.co.jp', 'google.co.uk', 'google.fr', 'google.de', 
    'google.it', 'google.es', 'google.ca', 'google.com.au', 'google.com.br',
    'googletagmanager.com', 'googleapis.com', 'googleusercontent.com',
    'x.com', 'twitter.com', 't.co',
    'youtube.com', 'youtu.be',
    'instagram.com',
    'linkedin.com',
    'pinterest.com',
    'tumblr.com',
    'reddit.com',
    'amazon.com', 'amazon.co.jp', 'amazon.co.uk',
    'microsoft.com', 'live.com', 'outlook.com', 'hotmail.com',
    'apple.com',
    'wikipedia.org',
    'yahoo.com',
    'baidu.com',
    'qq.com',
    'weibo.com',
    'tiktok.com',
    'snapchat.com',
    'discord.com',
    'telegram.org',
    'whatsapp.com',
    'line.me',
    'vk.com',
    'ok.ru',
    'mail.ru',
    'naver.com',
    'kakao.com',
    'paypal.com',
    'stripe.com',
    'adobe.com',
    'cloudflare.com',
    'akamai.net',
    'cdnjs.cloudflare.com',
    'jsdelivr.net',
    'unpkg.com',
    'jquery.com',
    'bootstrap.com',
    'fontawesome.com',
    'github.com', 'github.io',
    'stackoverflow.com',
    'medium.com',
    'wordpress.com',
    'blogger.com',
}

def extract_domain(url):
    """从 URL 中提取域名"""
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower()
    except Exception:
        return None

def extract_base_domain(domain):
    """
    提取主域名（去除子域名前缀）
    例如：www.facebook.com -> facebook.com
         api.google.com -> google.com
         www.google.co.uk -> google.co.uk
    """
    if not domain:
        return None
    
    domain = domain.lower().replace('www.', '')
    
    # 分割域名部分
    parts = domain.split('.')
    
    if len(parts) >= 2:
        # 尝试匹配主域名（最后两部分，如 facebook.com）
        base = '.'.join(parts[-2:])
        
        # 检查是否是常见域名的子域名
        if base in COMMON_DOMAINS:
            return base
        
        # 尝试匹配三级域名（最后三部分，如 google.co.uk）
        if len(parts) >= 3:
            base = '.'.join(parts[-3:])
            if base in COMMON_DOMAINS:
                return base
    
    return domain

def is_common_domain(domain):
    """
    检查域名是否是常见域名（或其子域名）
    
    Args:
        domain: 域名字符串
    
    Returns:
        bool: 如果是常见域名返回 True，否则返回 False
    """
    if not domain:
        return False
    
    domain = domain.lower().replace('www.', '')
    base_domain = extract_base_domain(domain)
    
    # 检查主域名是否在常见域名列表中
    if base_domain in COMMON_DOMAINS:
        return True
    
    # 检查完整域名是否在常见域名列表中（处理子域名情况）
    parts = domain.split('.')
    for i in range(len(parts)):
        check_domain = '.'.join(parts[i:])
        if check_domain in COMMON_DOMAINS:
            return True
    
    return False

def process_backlinks(page_url, target_domain):
    """
    解析网页中的 <a> 标签链接，提取所有非目标域名的外链
    
    Args:
        page_url: 要解析的网页 URL
        target_domain: 目标域名（如 labo-party.jp），同域名的链接会被过滤
    
    Returns:
        外链 URL 列表
    """
    try:
        scraper = cloudscraper.create_scraper()
        response = scraper.get(page_url, timeout=10)
        response.raise_for_status()
        
        # 解析 HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # 提取目标域名（去除 www. 前缀）
        target_domain_clean = target_domain.lower().replace('www.', '')
        target_base_domain = extract_base_domain(target_domain_clean)
        
        external_links = set()
        domain_counts = {}  # 统计每个域名已收集的链接数量
        
        # 查找所有 <a> 标签
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').strip()
            if not href:
                continue
            
            # 处理相对链接，转换为绝对链接
            absolute_url = urljoin(page_url, href)
            
            # 提取链接的域名
            link_domain = extract_domain(absolute_url)
            if not link_domain:
                continue
            
            # 去除 www. 前缀进行比较
            link_domain_clean = link_domain.replace('www.', '')
            link_base_domain = extract_base_domain(link_domain_clean)
            
            # 跳过目标域名
            if link_domain_clean == target_domain_clean or link_base_domain == target_base_domain:
                continue
            
            # 跳过常见域名（社交媒体、搜索引擎等）及其子域名
            if is_common_domain(link_domain):
                continue
            
            # 只保留有效的外链，并限制同一域名最多 2 条
            if absolute_url.startswith(('http://', 'https://')):
                count = domain_counts.get(link_domain_clean, 0)
                if count >= 2:
                    continue
                domain_counts[link_domain_clean] = count + 1
                external_links.add(absolute_url)
        
        logging.info(f"从 {page_url} 提取到 {len(external_links)} 个外链（按域名最多 2 条）")
        return list(external_links)
        
    except requests.RequestException as e:
        logging.error(f"Error processing backlinks from {page_url}: {str(e)}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error processing backlinks from {page_url}: {str(e)}")
        return []

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

def save_back_link_diff(site_name, new_urls):
    """保存外链差异到 back_link_diff 文件夹"""
    base_dir = Path('back_link_diff')
        
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

def compare_back_link_data(site_name, new_urls):
    """对比外链数据，返回新增的外链"""
    latest_file = Path('latest') / f'{site_name}_backlinks.json'
    
    if not latest_file.exists():
        return []
        
    with open(latest_file) as f:
        last_urls = set(f.read().splitlines())
    
    return [url for url in new_urls if url not in last_urls]

def send_feishu_notification(new_urls, config, site_name, category_label=None):
    if not new_urls:
        return
    
    webhook_url = config['feishu']['webhook_url']
    secret = config['feishu'].get('secret')
    
    message = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"🎮 {site_name} 上新通知" if not category_label else f"🎮 [{category_label}] {site_name} 上新通知"
                },
                "template": "green"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**今日新增 {len(new_urls)} {'条外链' if category_label == '外链监控' else '款游戏'}**\n\n" + "\n".join(f"• {url}" for url in new_urls[:10])
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
    
    def iter_all_sites(cfg):
        for site in cfg.get('sites', []):
            yield site, '游戏'
        for site in cfg.get('high_traffic_sites', []):
            yield site, '大流量'
        for site in cfg.get('nav_sites', []):
            yield site, '导航'
    
    # 处理 sitemap 站点
    for site, category in iter_all_sites(config):
        if not site.get('active', True):
            continue
            
        logging.info(f"处理站点: {site['name']}（分类: {category}）")
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
            send_feishu_notification(new_urls, config, site['name'], category_label=category)
            
        # 清理旧数据
        cleanup_old_data(site['name'], config)
    
    # 处理外链监控站点
    for site in config.get('back_link_sites', []):
        if not site.get('active', True):
            continue
        
        site_name = site['name']
        page_url = site['page_url']
        target_domain = site.get('target_domain', extract_domain(page_url))
        
        logging.info(f"处理外链监控站点: {site_name}")
        logging.info(f"  监控页面: {page_url}")
        logging.info(f"  目标域名: {target_domain}")
        
        # 提取外链
        external_links = process_backlinks(page_url, target_domain)
        
        if not external_links:
            logging.warning(f"站点 {site_name} 未提取到外链")
            continue
        
        # 去重处理
        unique_links = list({url: None for url in external_links}.keys())
        logging.info(f"站点 {site_name} 共获取 {len(unique_links)} 个唯一外链")
        
        # 对比数据
        new_links = compare_back_link_data(site_name, unique_links)
        logging.info(f"站点 {site_name} 差异链接数量: {len(new_links)} 个")
        
        if new_links:
            logging.info(f"站点 {site_name} 发现 {len(new_links)} 个新增外链，将保存到 back_link_diff")
        else:
            logging.info(f"站点 {site_name} 无新增外链")
        
        # 保存最新数据
        latest_file = Path('latest') / f'{site_name}_backlinks.json'
        latest_file.parent.mkdir(parents=True, exist_ok=True)
        with open(latest_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(unique_links))
        
        # 保存差异数据到 back_link_diff
        if new_links:
            save_back_link_diff(site_name, new_links)
            logging.info(f"站点 {site_name} 已保存 {len(new_links)} 个差异链接到 back_link_diff")
            send_feishu_notification(new_links, config, site_name, category_label='外链监控')
        
        # 清理旧数据（back_link_diff）
        cleanup_back_link_diff(site_name, config)

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

def cleanup_back_link_diff(site_name, config):
    """清理 back_link_diff 文件夹中的旧数据"""
    data_dir = Path('back_link_diff')
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
                logging.info(f"已删除过期外链文件夹: {date_dir.name}")
        except ValueError:
            # 忽略非日期格式的文件夹
            continue
        except Exception as e:
            logging.error(f"删除外链文件夹时出错: {str(e)}")

if __name__ == '__main__':
    main()
