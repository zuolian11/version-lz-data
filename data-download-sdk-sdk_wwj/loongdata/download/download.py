import asyncio
import contextlib
import os
import sys
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import requests
from obs import ObsClient
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
    MofNCompleteColumn
)
from rich.progress import TaskProgressColumn


def _first_env(*names):
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _header_value(resp, key):
    headers = getattr(resp, "header", None) or []
    target = key.lower()
    for header_key, header_value in headers:
        if str(header_key).lower() == target:
            return header_value
    return None


class LoongObsClient:
    def __init__(self, access_key_id=None, secret_access_key=None, server=None, security_token=None, credential_provider=None):
        self.credential_provider = credential_provider
        self._cred_lock = threading.Lock()
        self._expire_at = float('inf')
        self.platform = os.name
        access_key_id = access_key_id or _first_env(
            "LOONGDATA_OBS_AK",
            "OBS_ACCESS_KEY",
            "S3_ACCESS_KEY",
            "ACCESS_KEY_ID",
        )
        secret_access_key = secret_access_key or _first_env(
            "LOONGDATA_OBS_SK",
            "OBS_SECRET_KEY",
            "S3_SECRET_KEY",
            "SECRET_ACCESS_KEY",
        )
        security_token = security_token or _first_env(
            "LOONGDATA_OBS_TOKEN",
            "OBS_SECURITY_TOKEN",
            "S3_SECURITY_TOKEN",
            "SECURITY_TOKEN",
        )
        server = server or _first_env(
            "LOONGDATA_OBS_ENDPOINT",
            "OBS_ENDPOINT",
            "S3_DEFAULT_ENDPOINT",
        ) or 'http://obs.cn-east-3.myhuaweicloud.com'
        if not access_key_id or not secret_access_key:
            raise ValueError("OBS credentials are missing. Please fetch signature first or provide credentials explicitly.")
        self.obs_client = ObsClient(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            security_token=security_token,
            server=server
        )
        self.access_key_id = access_key_id
        self.server = server
        self.security_token = security_token

        if credential_provider:
            self._refresh_client()
            self._start_refresh_loop()

    def _refresh_client(self):
        ak, sk, token, expire = self.credential_provider()
        self._expire_at = expire if expire else time.time() + 28800
        self.access_key_id = ak
        self.security_token = token
        self.obs_client = ObsClient(
            access_key_id=ak,
            secret_access_key=sk,
            security_token=token,
            server=self.server,
        )

    def _start_refresh_loop(self):
        def loop():
            while True:
                time.sleep(21600)
                with self._cred_lock:
                    try:
                        self._refresh_client()
                    except Exception as e:
                        print(f"Credential refresh failed: {e}")
        t = threading.Thread(target=loop, daemon=True)
        t.start()

    def _ensure_valid(self):
        if time.time() + 7200 >= self._expire_at:
            with self._cred_lock:
                if time.time() + 7200 >= self._expire_at:
                    self._refresh_client()

    def list_objects(self, bucket_name, prefix=None, max_keys=1000, delimiter=None, marker=None):
        resp = self.obs_client.listObjects(bucket_name, prefix, max_keys=max_keys,
                                           delimiter=delimiter, marker=marker, encoding_type='url')

        result = []
        if resp.status < 300:
            for content in resp.body.contents:
                result.append({
                    'key': content.key,
                    'size': content.size,
                    'lastModified': content.lastModified,
                })
        return result

    def download_file(self, bucket_name, object_key, local_file_path, callback=None):
        self._ensure_valid()
        return self.obs_client.getObject(
            bucket_name,
            object_key,
            downloadPath=local_file_path,
            progressCallback=callback,
        )

class RichMultiThreadDownloader:
    def __init__(
        self,
        max_workers=5,
        obs_access_key=None,
        obs_secret_key=None,
        obs_endpoint=None,
        obs_security_token=None,
        trust_env_proxy=False,
    ):
        self.host = ''
        self.dataset = ''
        self.session = ''
        self.max_workers = max_workers
        self.q = deque()
        self.max_size = max_workers * 2
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.futures = []
        self.lock = threading.Lock()
        self.console = Console()
        self.trust_env_proxy = trust_env_proxy
        self.obs_access_key = obs_access_key
        self.obs_secret_key = obs_secret_key
        self.obs_endpoint = obs_endpoint
        self.obs_security_token = obs_security_token
        self.client = None
        if self.obs_access_key and self.obs_secret_key:
            self.configure_obs_client(
                access_key_id=self.obs_access_key,
                secret_access_key=self.obs_secret_key,
                security_token=self.obs_security_token,
                server=self.obs_endpoint,
            )
        self.requests_session = requests.Session()
        self.requests_session.trust_env = trust_env_proxy

        # 创建进度显示
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(binary_units=True),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=self.console
        )
        self.overall_progress = Progress(
            TextColumn("[bold red]{task.description}"),
            BarColumn(bar_width=None),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=self.console
        )
        self.overall_cnt_task_id = self.overall_progress.add_task("[red]Overall Counts", total=0)
        # self.overall_byte_task_id = self.overall_progress.add_task("[red]Overall Bytes", total=0)
        self.layout = self.make_layout()

    def configure_obs_client(self, access_key_id, secret_access_key, security_token=None, server=None, credential_provider=None):
        self.obs_access_key = access_key_id
        self.obs_secret_key = secret_access_key
        self.obs_security_token = security_token
        self.obs_endpoint = server or self.obs_endpoint
        self.client = LoongObsClient(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            security_token=security_token,
            server=self.obs_endpoint,
            credential_provider=credential_provider,
        )

    def make_layout(self) -> Layout:
        """生成页面布局"""
        layout = Layout()
        # 将屏幕分为上下两部分：主区域和页脚
        layout.split_column(
            Layout(name="main", ratio=1),
            Layout(name="footer", size=3)  # 固定页脚高度为 5
        )
        return layout

    @contextlib.contextmanager
    def live(self, host, dataset, session, screen=True, output=''):
        self.layout["main"].update(Panel(self.progress, title="[bold blue]Downloading Progress", border_style="blue"))
        self.layout["footer"].update(Panel(self.overall_progress, title="[bold red]Overall Progress", border_style="red"))
        self.update_overall(complete_cnt=0, total_cnt=1)
        with Live(self.layout, console=self.console, screen=screen) as live:
            try:
                self.host, self.dataset, self.session = host, dataset, session
                self.q.clear()
                yield live
            finally:
                self.wait_finish()
                # 任务结束，更新底部提示框
                self.layout["footer"].update(
                    Panel(f"[bold green]数据已下载至 {output} 目录，请按 [Enter] 键退出程序...",
                          border_style="green", title_align="center")
                )
                if sys.stdin is not None and sys.stdin.isatty():
                    try:
                        input("")
                    except EOFError:
                        pass

    def update_overall(self, complete_cnt=-1, advance_cnt=0, advance_bytes=0, advance_duration=0, total_cnt=-1,
                       upload_progress=False):
        """更新整体进度"""
        with self.lock:
            if complete_cnt >= 0:
                self.overall_progress.update(self.overall_cnt_task_id, completed=complete_cnt)
            if advance_cnt > 0:
                self.overall_progress.advance(self.overall_cnt_task_id, advance=advance_cnt)
            if total_cnt >= 0:
                self.overall_progress.update(self.overall_cnt_task_id, total=total_cnt)
            if upload_progress:
                response = self.requests_session.post(f"{self.host}/data-miner/dataset/download/session/update", json={
                    "sessionId": self.session,
                    "datasetId": self.dataset,
                    "downloadCount": advance_cnt,
                    "downloadSize": advance_bytes,
                    "downloadDuration": advance_duration
                })
                if response.status_code != 200:
                    print(f"更新进度失败，状态码: {response.status_code}")

    def download_with_progress(self, bucket_name, url, filename, save_dir: Path, duration, file_size):
        """使用rich进度条下载文件"""
        if self.client is None:
            raise RuntimeError("OBS client is not configured.")
        tmp_path = save_dir / (filename + '.tmp')
        save_path = save_dir / filename

        with self.progress:
            if len(self.q) >= self.max_size:
                leave_id = self.q.popleft()
                self.progress.remove_task(leave_id)
            task_id = self.progress.add_task(
                f"[cyan]Download {filename}",
                filename=filename,
                start=True
            )
            self.q.append(task_id)
            solid_download = False
            if save_path.exists():
                total = save_path.stat().st_size
                self.progress.update(task_id, completed=total, total=total)
            else:
                def _progress_callback(bytes_transferred, bytes_total, seconds):
                    self.progress.update(task_id, completed=bytes_transferred, total=bytes_total)
                resp = self.client.download_file(bucket_name, url, tmp_path, _progress_callback)
                success = resp.status < 300
                if success and tmp_path.exists():
                    tmp_path.rename(save_path)
                    solid_download = True
                else:
                    error_code = getattr(resp, "errorCode", None) or _header_value(resp, "error-code")
                    error_message = getattr(resp, "errorMessage", None) or _header_value(resp, "error-message")
                    request_id = getattr(resp, "requestId", None) or _header_value(resp, "request-id")
                    hint = ""
                    if error_code == "AccessDenied" and self.client and self.client.security_token:
                        hint = " createSignature 返回的临时凭证当前无法 GetObject，请检查服务端签发策略或 OBS endpoint；如果内网可访问私网 OBS，可尝试 --obs-endpoint http://obs-private.cn-east-3.myhuaicloud.com。"
                    self.console.print(
                        f"[red]Download {url} failed: status={resp.status}, "
                        f"code={error_code}, message={error_message}, request_id={request_id}.{hint}[/red]"
                    )
            self.update_overall(advance_cnt=1, advance_bytes=file_size, advance_duration=duration, upload_progress=solid_download)

        return filename, True

    def download_item(self, bucket_name, obs_path, output_dir, filename, duration, file_size):
        future = self.executor.submit(
            self.download_with_progress,
            bucket_name, obs_path, filename, output_dir, duration, file_size
        )
        self.futures.append(future)

    def wait_finish(self):
        results = []
        for future in self.futures:
            try:
                result = future.result()
                results.append(result)
            except asyncio.CancelledError:
                self.console.print("[red]✗ Download interrupted by user")
            except Exception as e:
                self.console.print(f"[red]✗ 下载失败: {e}")
                results.append((None, False))
        return results
