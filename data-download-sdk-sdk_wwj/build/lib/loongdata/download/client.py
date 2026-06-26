from pathlib import Path
from datetime import datetime

from .download import RichMultiThreadDownloader
from .stream import DatasetStreamClient

class DownloadClient:
    def __init__(
        self,
        host,
        max_workers=4,
        obs_access_key=None,
        obs_secret_key=None,
        obs_endpoint=None,
        obs_security_token=None,
        trust_env_proxy=False,
    ):
        self.host = host
        self.max_workers = max_workers
        self.trust_env_proxy = trust_env_proxy
        self.obs_access_key = obs_access_key
        self.obs_secret_key = obs_secret_key
        self.obs_security_token = obs_security_token
        self.obs_endpoint = obs_endpoint
        self.downloader = RichMultiThreadDownloader(
            max_workers=max_workers,
            obs_access_key=obs_access_key,
            obs_secret_key=obs_secret_key,
            obs_endpoint=obs_endpoint,
            obs_security_token=obs_security_token,
            trust_env_proxy=trust_env_proxy,
        )

    async def download(self, dataset, session, output_dir):
        credentials, provider = await self.get_obs_credentials(dataset, session)
        self.downloader.configure_obs_client(
            access_key_id=credentials["access_key_id"],
            secret_access_key=credentials["secret_access_key"],
            security_token=credentials.get("security_token"),
            server=credentials.get("endpoint") or self.obs_endpoint,
            credential_provider=provider,
        )
        with self.downloader.live(self.host, dataset, session, output=output_dir):
            def download_item(bucket, obs_path, task_id, episode_id, duration, file_size):
                task_dir = Path(output_dir) / task_id
                filename = f'{episode_id}.h5'
                if not task_dir.exists():
                    task_dir.mkdir(parents=True)
                self.downloader.download_item(bucket, obs_path, task_dir, filename, duration, file_size)
            cnt = await self.get_data_info(dataset, session, download_item)
            self.downloader.update_overall(total_cnt=cnt)

    async def get_data_info(self, dataset, session, callback):
        async with DatasetStreamClient(self.host, trust_env=self.trust_env_proxy).connect() as client:
            async def process_data(data):
                bucket, task_id, episode_id = data.get('bucket'), data.get('taskId'), data.get('episodeId')
                duration, file_size = int(data.get('duration')), int(data.get('fileSize'))
                obs_path = f'data-collector-svc/align/{task_id}/{episode_id}/{episode_id}.h5'
                callback(bucket, obs_path, task_id, episode_id, duration, file_size)
            return await client.stream_episode(
                dataset_id=dataset,
                session_id=session,
                callback=process_data
            )

    async def get_obs_credentials(self, dataset, session):
        if self.obs_access_key and self.obs_secret_key:
            return {
                "access_key_id": self.obs_access_key,
                "secret_access_key": self.obs_secret_key,
                "security_token": self.obs_security_token,
                "endpoint": self.obs_endpoint,
            }, None

        async with DatasetStreamClient(self.host, trust_env=self.trust_env_proxy).connect() as client:
            payload = await client.create_signature(dataset_id=dataset, session_id=session)
            data = payload.get("data") or {}
            access_key_id = data.get("accessKeyId")
            secret_access_key = data.get("secretAccessKey")
            security_token = data.get("securityToken")
            if not access_key_id or not secret_access_key:
                raise RuntimeError(f"createSignature 返回缺少凭证字段: {payload}")
            return {
                "access_key_id": access_key_id,
                "secret_access_key": secret_access_key,
                "security_token": security_token,
                "endpoint": self.obs_endpoint,
            }, self._build_provider(dataset, session)

    def _build_provider(self, dataset, session):
        """构建凭证刷新回调"""
        import requests
        host = self.host

        def provider():
            resp = requests.post(
                f"{host}/data-miner/ak/createSignature",
                json={"datasetId": dataset, "sessionId": session},
                timeout=30,
            )
            cred = resp.json()['data']
            expire_ts = datetime.fromisoformat(
                cred['expiredTime'].replace('Z', '+00:00')
            ).timestamp()
            return cred['accessKeyId'], cred['secretAccessKey'], cred['securityToken'], expire_ts

        return provider
