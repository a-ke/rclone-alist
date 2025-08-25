#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import logging
import tempfile
import shutil
import fcntl
import requests
import fnmatch
import time
from datetime import datetime, timedelta, timezone

# =================================================================
# rclone + Alist 增量备份脚本 (Python 重写版)
# 版本：2.2 (功能对等)
#
# 功能：
#  - 使用 rclone 实现高效增量备份
#  - 支持本地目录 → 远端目录一对一映射
#  - 可配置保护模式 (不删除远端文件) / 回收站模式 (删除时移入)
#  - 自动清理远端回收站
#  - 定期全量校验
#  - 日志文件自动清理
#  - 本地日志无乱码 (UTF-8)
#  - 使用文件锁确保单例执行
# =================================================================


# === 基础配置 ===
# 配置格式: "本地目录::远端目录"
SYNC_PAIRS = [
    "/volume2/xxx::115/nas备份/xxx",
]

# === 内置忽略规则（支持通配符，语法与 rclone exclude 相同）===
EXCLUDES = [
    "node_modules/**",
    ".DS_Store",
    "*.log",
    "tmp/",
]

REMOTE_NAME = "alist"
LOG_DIR = "~/rclone-alist/logs"
CACHE_DIR = "~/rclone-alist/cache"
RCLONE_BIN = "/usr/local/bin/rclone" # 请确保 rclone 在此路径或系统 PATH 中

# --- 脚本执行配置 ---
LOCK_FILE = os.path.join(CACHE_DIR, "rclone_alist_backup.lock")

# === 操作间隔配置 ===
OPERATION_INTERVAL_SECONDS = 2  # 每次文件操作（上传/删除）后的间隔时间（秒），设为 0 则无间隔

# === 同步策略 ===
PROTECT_MODE = "off"  # 'on' 或 'off'。'on' 则不删除任何远端文件
REMOTE_TRASH = "off"  # 'on' 或 'off'。'on' 则将删除的文件移入回收站
TRASH_RETENTION_DAYS = 30  # 回收站文件保留天数

# === 全量校验配置 ===
ENABLE_FULL_CHECK = "off"  # 'on' 或 'off'
FULL_CHECK_DAYS = 30  # 每隔多少天执行一次全量校验
FULL_CHECK_ACTION = "warn"  # 'warn' (仅警告) 或 'resync' (自动修复)

# === 日志清理配置 ===
LOG_RETENTION_DAYS = 30

# === 通知配置 ===
ENABLE_NOTIFY = "off"  # 'on' 或 'off'
NOTIFY_MODE = "telegram"  # 'telegram' 或 'mail'
# --- 邮件配置 ---
MAIL_TO = "user@example.com"
MAIL_SUBJECT = "[rclone-backup] 任务结果"
MAIL_BIN = "/usr/sbin/sendmail"
# --- Telegram 配置 ---
TG_BOT_TOKEN = "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
TG_CHAT_ID = "123456789"
# === 代理配置  ===
PROXY_URL = "http://192.168.1.1:7890"

# ================= rclone 参数（列表形式） =================
RCLONE_OPTS = [
    "--progress",
    "--transfers=2",
    "--checkers=4",
    "--bwlimit=100M",
    "--retries=5",
    "--low-level-retries=10",
    "--timeout=2h",
    "--contimeout=60s",
    "--max-duration=24h",
    "--stats=30s",
    "--log-level=INFO",
]

# ================= 脚本核心，一般无需修改 =================

# --- 初始化 ---
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)
TODAY = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = os.path.join(LOG_DIR, f"backup-{TODAY}.log")

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, 'a', 'utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# --- 预定义回收站目录 ---
TRASH_ROOT_DIR = f"{REMOTE_NAME}:115/trash"
TRASH_TODAY_DIR = f"{TRASH_ROOT_DIR}/{TODAY}"

class Stats:
    """全局统计"""
    def __init__(self):
        self.upload_count = 0
        self.upload_size = 0
        self.delete_count = 0
        self.trash_count = 0
        self.fail_count = 0
        self.start_time = datetime.now()

stats = Stats()

def format_bytes(size_bytes):
    """将字节数格式化为易读的单位"""
    if size_bytes == 0:
        return "0B"
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size_bytes >= power and n < len(power_labels):
        size_bytes /= power
        n += 1
    return f"{size_bytes:.2f}{power_labels[n]}B"

def run_command(cmd_args, check=True):
    """执行外部命令并返回结果"""
    try:
        result = subprocess.run(cmd_args, check=check, capture_output=True, text=True, encoding='utf-8')
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"命令执行失败: {' '.join(cmd_args)}")
        logging.error(f"返回码: {e.returncode}")
        logging.error(f"标准输出: {e.stdout}")
        logging.error(f"标准错误: {e.stderr}")
        return None
    except FileNotFoundError:
        logging.error(f"命令未找到: {cmd_args[0]}，请检查路径配置。")
        sys.exit(1)


def notify(message):
    """发送通知"""
    if ENABLE_NOTIFY != "on":
        return
    
    # 构造 proxies 字典 (如果配置了全局代理)
    proxies = None
    try:
        # 检查 PROXY_URL 是否已定义、非空且不只包含空格
        if PROXY_URL and PROXY_URL.strip():
            # .strip() 用于去除首尾可能存在的空格
            effective_proxy_url = PROXY_URL.strip()
            proxies = {
               'http': effective_proxy_url,
               'https': effective_proxy_url,
            }
            logging.info(f"检测到代理配置，将通过 {effective_proxy_url} 发送通知。")
            
    except NameError:
        # 如果 PROXY_URL 变量未定义 (被删除或注释)，则静默处理，proxies 保持为 None
        # 脚本将继续尝试直接连接
        pass
    try:
        if NOTIFY_MODE == "mail":
            msg = f"Subject: {MAIL_SUBJECT}\n\n{message}"
            with subprocess.Popen([MAIL_BIN, MAIL_TO], stdin=subprocess.PIPE) as p:
                p.communicate(input=msg.encode('utf-8'))
        elif NOTIFY_MODE == "telegram":
            url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
            payload = {'chat_id': TG_CHAT_ID, 'text': message}
            requests.post(url, params=payload, timeout=10, proxies=proxies)
        logging.info("通知发送成功。")
    except Exception as e:
        logging.error(f"通知发送失败: {e}")


def get_local_files(local_dir):
    """
    生成本地文件快照信息，使用 fnmatch 实现忽略规则。
    格式: {relative_path: "size|mtime"}
    """
    local_files = {}
    
    # 为了统一匹配，将EXCLUDES中的Windows路径分隔符替换为/
    normalized_excludes = [p.replace('\\', '/') for p in EXCLUDES]

    for root, dirs, files in os.walk(local_dir, topdown=True):
        # --- 目录排除 (Pruning) ---
        # 遍历目录列表的副本，因为我们会在循环中修改它
        for d in list(dirs):
            is_excluded = False
            for pattern in normalized_excludes:
                # 提取模式的基础部分用于匹配目录名
                base_pattern = pattern
                if pattern.endswith('/**'):
                    base_pattern = pattern[:-3]
                elif pattern.endswith('/'):
                    base_pattern = pattern[:-1]
                
                # 核心修正：直接用目录名 `d` 和模式的基底部分匹配
                # 这使得规则可以在任意路径深度生效。
                # 例如 d='@eaDir', base_pattern='@eaDir' -> 匹配成功
                if fnmatch.fnmatch(d, base_pattern):
                    is_excluded = True
                    break
            
            if is_excluded:
                # 从os.walk的后续遍历中移除此目录及其所有子目录
                dirs.remove(d)
                # print(f"Pruned directory: {os.path.join(root, d)}") # 调试时可以取消注释

        # --- 文件排除 ---
        for file in files:
            full_path = os.path.join(root, file)
            # 构造相对路径并统一使用 / 分隔符
            rel_path = os.path.relpath(full_path, local_dir).replace('\\', '/')
            basename = os.path.basename(rel_path)
            
            is_excluded = False
            for pattern in normalized_excludes:
                # 规则不含'/'，是简单的文件名规则 (如: '*.log', '.DS_Store')
                if '/' not in pattern:
                    if fnmatch.fnmatch(basename, pattern):
                        is_excluded = True
                        break
                # 规则含'/'，是路径规则 (如: '@eaDir/**', 'node_modules/**')
                else:
                    # 核心修正：在模式前加通配符'*'，以匹配任意深度的路径
                    if fnmatch.fnmatch(rel_path, '*' + pattern):
                        is_excluded = True
                        break
            
            if is_excluded:
                continue
            
            # 如果文件未被忽略，则获取其信息
            try:
                stat = os.stat(full_path)
                info_str = f"{stat.st_size}|{stat.st_mtime}"
                local_files[rel_path] = info_str
            except FileNotFoundError:
                continue # 文件在扫描过程中被删除
            
    return local_files


def copy_file(local_dir, remote_sub, rel_path, size, snapshot_work_file, line_info):
    """上传单个文件"""
    src_path = os.path.join(local_dir, rel_path)
    dest_path = f"{REMOTE_NAME}:{os.path.join(remote_sub, rel_path)}"
    
    logging.info(f"上传: {rel_path} ({format_bytes(size)})")
    cmd = [RCLONE_BIN, "copyto", src_path, dest_path] + RCLONE_OPTS + [f"--log-file={LOG_FILE}"]
    
    if run_command(cmd, check=False).returncode == 0:
        stats.upload_count += 1
        stats.upload_size += size
        logging.debug(f"上传成功，更新工作快照: {rel_path}")
        update_snapshot_file(snapshot_work_file, "upsert", line_info)
    else:
        logging.error(f"上传失败: {src_path}")
        stats.fail_count += 1
    
    time.sleep(OPERATION_INTERVAL_SECONDS)

def delete_or_trash_file(remote_sub, rel_path, snapshot_work_file, line_info):
    """删除或移动远端文件"""
    remote_file_path = f"{REMOTE_NAME}:{os.path.join(remote_sub, rel_path)}"
    
    if REMOTE_TRASH == "on":
        trash_path = f"{TRASH_TODAY_DIR}/{os.path.join(remote_sub, rel_path)}"
        logging.info(f"移动到回收站: {rel_path}")
        cmd = [RCLONE_BIN, "moveto", remote_file_path, trash_path, f"--log-file={LOG_FILE}"]
        if run_command(cmd, check=False).returncode == 0:
            stats.trash_count += 1
            logging.debug(f"移至回收站成功，更新工作快照: {rel_path}")
            update_snapshot_file(snapshot_work_file, "delete", line_info)
        else:
            logging.error(f"移至回收站失败: {remote_file_path}")
            stats.fail_count += 1
    else:
        logging.info(f"删除远端文件: {rel_path}")
        cmd = [RCLONE_BIN, "deletefile", remote_file_path, f"--log-file={LOG_FILE}"]
        if run_command(cmd, check=False).returncode == 0:
            stats.delete_count += 1
            logging.debug(f"删除成功，更新工作快照: {rel_path}")
            update_snapshot_file(snapshot_work_file, "delete", line_info)
        else:
            logging.error(f"删除远端文件失败: {remote_file_path}")
            stats.fail_count += 1
    
    time.sleep(OPERATION_INTERVAL_SECONDS)


def update_snapshot_file(snapshot_file, operation, line_to_process):
    """使用临时文件安全地更新快照文件"""
    rel_path = line_to_process.split('|', 1)[0]
    
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_f:
            temp_filename = temp_f.name
            if os.path.exists(snapshot_file):
                with open(snapshot_file, 'r', encoding='utf-8') as current_f:
                    for line in current_f:
                        if not line.startswith(f"{rel_path}|"):
                            temp_f.write(line)
            
            if operation == "upsert":
                temp_f.write(line_to_process + '\n')
        
        shutil.move(temp_filename, snapshot_file)
    except Exception as e:
        logging.error(f"更新快照文件 {snapshot_file} 失败: {e}")
        if 'temp_filename' in locals() and os.path.exists(temp_filename):
            os.remove(temp_filename)


def sync_dir_incremental(local_dir, remote_sub):
    """核心增量同步函数"""
    safe_name = remote_sub.replace('/', '_').replace(':', '_')
    snapshot_file = os.path.join(CACHE_DIR, f"{safe_name}.snapshot")
    snapshot_work_file = f"{snapshot_file}.tmp"

    logging.info("=" * 64)
    logging.info(f"开始同步目录: {local_dir} → {REMOTE_NAME}:{remote_sub}")
    logging.info("=" * 64)

    # 1. 准备工作快照
    if os.path.exists(snapshot_work_file):
        logging.warning("发现上次未完成的工作快照，将基于此继续同步。")
    elif os.path.exists(snapshot_file):
        shutil.copy(snapshot_file, snapshot_work_file)
    else:
        logging.info("未找到任何快照，将进行首次全量同步。")
        open(snapshot_work_file, 'w').close()

    # 2. 将工作快照读入内存
    old_files = {}
    if os.path.exists(snapshot_work_file):
        with open(snapshot_work_file, 'r', encoding='utf-8') as f:
            for line in f:
                if '|' in line:
                    parts = line.strip().split('|', 2)
                    if len(parts) == 3:
                        old_files[parts[0]] = f"{parts[1]}|{parts[2]}"
    logging.info(f"内存快照加载完成，包含 {len(old_files)} 个文件记录。")

    # 3. 扫描本地文件，处理新增和修改
    logging.info("开始扫描本地文件并处理新增/修改...")
    current_files = get_local_files(local_dir)
    logging.info(f"本地扫描到 {len(current_files)} 个文件。")
    
    for rel_path, info_str in current_files.items():
        size, mtime = map(float, info_str.split('|', 1))
        line_info = f"{rel_path}|{int(size)}|{mtime}"

        if rel_path not in old_files:
            logging.info(f"发现新增文件: {rel_path}")
            copy_file(local_dir, remote_sub, rel_path, int(size), snapshot_work_file, line_info)
        elif old_files[rel_path] != f"{int(size)}|{mtime}":
            logging.info(f"发现修改文件: {rel_path}")
            copy_file(local_dir, remote_sub, rel_path, int(size), snapshot_work_file, line_info)
        
        # 从待删除列表中移除
        if rel_path in old_files:
            del old_files[rel_path]

    # 4. 处理删除
    if PROTECT_MODE == "off":
        if old_files:
            logging.info(f"开始处理 {len(old_files)} 个已删除文件...")
            for rel_path, info_str in old_files.items():
                line_info = f"{rel_path}|{info_str}"
                delete_or_trash_file(remote_sub, rel_path, snapshot_work_file, line_info)
        else:
            logging.info("未发现需要删除的文件。")
    else:
        logging.warning("保护模式已开启，跳过删除文件检查。")

    # 5. 完成同步，用工作快照覆盖基准快照
    logging.info("目录同步完成，更新基准快照文件。")
    shutil.move(snapshot_work_file, snapshot_file)


def cleanup_trash():
    """清理远端回收站"""
    if REMOTE_TRASH == "on" and TRASH_RETENTION_DAYS > 0:
        logging.info(f"开始清理远端回收站，保留最近 {TRASH_RETENTION_DAYS} 天...")
        cutoff_date = datetime.now() - timedelta(days=TRASH_RETENTION_DAYS)
        
        cmd = [RCLONE_BIN, "lsd", TRASH_ROOT_DIR]
        result = run_command(cmd)
        if not result:
            return

        for line in result.stdout.strip().split('\n'):
            try:
                # rclone lsd 输出格式: "  -1 2023-01-01 10:00:00.000        -1 2023-01-01"
                dir_date_str = line.strip().split()[-1]
                dir_date = datetime.strptime(dir_date_str, "%Y-%m-%d")
                if dir_date < cutoff_date:
                    dir_to_purge = f"{TRASH_ROOT_DIR}/{dir_date_str}"
                    logging.info(f"回收站目录 [{dir_date_str}] 已过期，正在执行清理...")
                    purge_cmd = [RCLONE_BIN, "purge", dir_to_purge, f"--log-file={LOG_FILE}"]
                    run_command(purge_cmd)
            except (ValueError, IndexError):
                continue
        logging.info("回收站清理完成。")


def full_check():
    """定期全量校验"""
    if ENABLE_FULL_CHECK != "on":
        return
        
    last_check_file = os.path.join(CACHE_DIR, "last_full_check")
    now = datetime.now()
    needs_check = False
    
    if not os.path.exists(last_check_file):
        needs_check = True
    else:
        with open(last_check_file, 'r') as f:
            last_check_ts = float(f.read())
        if (now - datetime.fromtimestamp(last_check_ts)).days >= FULL_CHECK_DAYS:
            needs_check = True

    if needs_check:
        logging.info("开始执行全量校验...")
        for pair in SYNC_PAIRS:
            local_dir, remote_sub = pair.split("::", 1)
            logging.info(f"校验: {local_dir}")
            
            check_opts = [item for pattern in EXCLUDES for item in ("--exclude", pattern)]
            check_cmd = [RCLONE_BIN, "check", local_dir, f"{REMOTE_NAME}:{remote_sub}", "--one-way", f"--log-file={LOG_FILE}"] + check_opts
            
            if run_command(check_cmd, check=False).returncode != 0:
                logging.error(f"校验发现差异: {local_dir}")
                notify_msg = f"【rclone备份】校验发现差异，请检查: {local_dir}"
                if FULL_CHECK_ACTION == "resync":
                    logging.warning(f"将自动重新同步: {local_dir}")
                    sync_cmd = [RCLONE_BIN, "sync", local_dir, f"{REMOTE_NAME}:{remote_sub}"] + RCLONE_OPTS + check_opts + [f"--log-file={LOG_FILE}"]
                    run_command(sync_cmd)
                    notify_msg = f"【rclone备份】校验发现差异并已自动修复: {local_dir}"
                notify(notify_msg)
            else:
                logging.info(f"校验通过: {local_dir}")

        with open(last_check_file, 'w') as f:
            f.write(str(now.timestamp()))
    else:
        logging.info("未到全量校验周期，跳过。")


def cleanup_logs():
    """清理本地旧日志和临时文件"""
    if LOG_RETENTION_DAYS > 0:
        logging.info(f"开始清理本地旧日志，保留最近 {LOG_RETENTION_DAYS} 天...")
        cutoff_time = datetime.now() - timedelta(days=LOG_RETENTION_DAYS)
        
        for dir_path, file_prefix, file_suffix in [(LOG_DIR, "backup-", ".log"), (CACHE_DIR, "", ".snapshot.tmp")]:
            for filename in os.listdir(dir_path):
                if filename.startswith(file_prefix) and filename.endswith(file_suffix):
                    file_path = os.path.join(dir_path, filename)
                    try:
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                        if file_mtime < cutoff_time:
                            os.remove(file_path)
                            logging.info(f"已删除旧文件: {file_path}")
                    except OSError as e:
                        logging.error(f"删除文件 {file_path} 失败: {e}")
        logging.info("本地日志清理完成。")


def main():
    """主流程函数"""
    logging.info("########## rclone 备份任务开始 ##########")
    
    for pair in SYNC_PAIRS:
        try:
            local_dir, remote_sub = pair.split("::", 1)
            if not local_dir or not remote_sub:
                raise ValueError("配置项格式错误")
            if not os.path.isdir(local_dir):
                logging.error(f"本地目录不存在，跳过: {local_dir}")
                continue
            sync_dir_incremental(local_dir, remote_sub)
        except ValueError:
            logging.error(f"配置项格式错误，跳过: {pair}")
            continue

    cleanup_trash()
    full_check()
    cleanup_logs()
    
    duration = datetime.now() - stats.start_time
    duration_str = str(timedelta(seconds=int(duration.total_seconds())))
    upload_size_str = format_bytes(stats.upload_size)

    summary = f"""
########## rclone 备份任务完成 ##########
- 本次上传/更新: {stats.upload_count} 个文件, 共 {upload_size_str}
- 本次移到回收站: {stats.trash_count} 个文件
- 本次永久删除: {stats.delete_count} 个文件
- 本次操作失败: {stats.fail_count} 次 (失败的操作将在下次运行时自动重试)
- 总耗时: {duration_str}
详情请查看日志: {LOG_FILE}
"""
    logging.info(summary)
    notify(summary)
    logging.info("########## 任务结束 ##########")


if __name__ == "__main__":
    lock_f = open(LOCK_FILE, 'w')
    try:
        # 尝试获取非阻塞排他锁
        fcntl.flock(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        main()
    except (IOError, BlockingIOError):
        logging.warning("检测到已有任务在运行，本次任务自动跳过。")
    finally:
        # 释放锁并关闭文件
        fcntl.flock(lock_f, fcntl.LOCK_UN)
        lock_f.close()
