import argparse
import asyncio
import os

from .config import load_local_env
from .download import DownloadClient

def main():
    # 先加载本地配置文件，再读取 argparse 默认值。
    load_local_env()
    parser = argparse.ArgumentParser(description="Loongdata Open Source Data Client")
    parser.add_argument("action", default="download", help="data action", choices=["download"])
    parser.add_argument("--dataset", required=True, help="Dataset id in loong data platform")
    parser.add_argument("--session", required=True, help="Loongdata download session")
    parser.add_argument("--output", help="Output directory", default='')
    parser.add_argument("--host", help="Loongdata server host",
                        default=os.getenv("LOONGDATA_HOST", "http://dojo-api.openloong.org.cn"))
    parser.add_argument("--max-worker", help="Max download worker threads", type=int, default=5)
    parser.add_argument("--obs-ak", help=argparse.SUPPRESS)
    parser.add_argument("--obs-sk", help=argparse.SUPPRESS)
    parser.add_argument("--obs-token", help=argparse.SUPPRESS)
    parser.add_argument(
        "--obs-endpoint",
        help="Override OBS endpoint",
        default=(
            os.getenv("LOONGDATA_OBS_ENDPOINT")
            or os.getenv("OBS_ENDPOINT")
            or os.getenv("S3_DEFAULT_ENDPOINT")
            or "http://obs.cn-east-3.myhuaweicloud.com"
        ),
    )
    parser.add_argument(
        "--trust-env-proxy",
        action="store_true",
        help="Use proxy settings from the shell environment",
    )

    args = parser.parse_args()

    if args.action == "download":
        output_dir = args.output if args.output else f"./{args.dataset}"
        client = DownloadClient(
            args.host,
            args.max_worker,
            obs_access_key=args.obs_ak,
            obs_secret_key=args.obs_sk,
            obs_security_token=args.obs_token,
            obs_endpoint=args.obs_endpoint,
            trust_env_proxy=args.trust_env_proxy,
        )
        asyncio.run(client.download(args.dataset, args.session, output_dir))
