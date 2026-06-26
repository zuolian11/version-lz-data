import time

import httpx
import asyncio
import json
from contextlib import asynccontextmanager

class DatasetStreamClient:
    """使用HTTPX的流式客户端"""

    def __init__(self, base_url: str, timeout: float = 30.0, trust_env: bool = False):
        self.base_url = base_url
        self.timeout = timeout
        self.trust_env = trust_env
        self.client = None

    @asynccontextmanager
    async def connect(self):
        """连接上下文管理器"""
        limits = httpx.Limits(
            max_keepalive_connections=5,
            max_connections=10
        )

        async with httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
                limits=limits,
                follow_redirects=True,
                trust_env=self.trust_env,
        ) as client:
            self.client = client
            try:
                yield self
            finally:
                self.client = None

    async def stream_episode(self, dataset_id: str, session_id: str, callback=None):
        """流式接收数据集的episode数据"""
        endpoint = f"/data-miner/dataset/stream/episode"
        params = {'datasetId': dataset_id, 'sessionId': session_id}
        cnt = await self.stream_json(endpoint, callback, params)
        return cnt

    async def create_signature(self, dataset_id: str, session_id: str):
        """获取临时 OBS 凭证。"""
        endpoint = "/data-miner/ak/createSignature"
        payload = {"datasetId": dataset_id, "sessionId": session_id}
        return await self.request_json("POST", endpoint, json_body=payload)

    async def request_json(self, method: str, endpoint: str, params: dict = None, json_body: dict = None):
        """普通 JSON 请求。"""
        if not self.client:
            raise RuntimeError("客户端未连接")

        headers = {
            "Accept": "application/json",
            "User-Agent": "Python-Httpx-Stream-Client",
        }
        response = await self.client.request(
            method,
            endpoint,
            params=params,
            json=json_body,
            headers=headers,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and payload.get("code") not in (None, 200):
            raise RuntimeError(f"请求失败: code={payload.get('code')}, msg={payload.get('msg')}")
        return payload

    async def stream_json(self,
                          endpoint: str,
                          callback=None,
                          params: dict = None) -> int:
        """流式接收JSON"""
        if not self.client:
            raise RuntimeError("客户端未连接")

        url = endpoint
        headers = {
            'Accept': 'application/x-ndjson',
            'User-Agent': 'Python-Httpx-Stream-Client'
        }
        cnt = 0

        try:
            async with self.client.stream(
                    'GET',
                    url,
                    params=params,
                    headers=headers
            ) as response:

                if response.status_code != 200:
                    error = await response.aread()
                    print(f"请求失败: {response.status_code}, {error}")
                    return

                buffer = ""
                async for chunk in response.aiter_bytes():
                    chunk_str = chunk.decode('utf-8')
                    buffer += chunk_str

                    # 分割处理
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        line = line.strip()

                        if line:
                            try:
                                data = json.loads(line)
                                if callback:
                                    await callback(data)
                                else:
                                    print(f"收到: {data}")
                                cnt += 1
                            except json.JSONDecodeError:
                                print(f"无效JSON: {line}")

        except httpx.RequestError as e:
            print(f"请求错误: {e}")
        except Exception as e:
            print(f"未知错误: {e}")
        return cnt


async def httpx_example():
    """HTTPX示例"""
    async with DatasetStreamClient("http://localhost:8999").connect() as client:
        async def process_data(data):
            bucket, task_id, episode_id = data.get('bucket'), data.get('taskId'), data.get('episodeId')
            obs_path = f'data-collector-svc/align/{task_id}/{episode_id}/{episode_id}.h5'
            print(f"处理: {bucket}, {obs_path}")
        start = time.time()
        await client.stream_episode(
            dataset_id='48986607a6724b43b02ebd3b96c59a64',
            session_id='f2ee5264237b46cea1abce1ebe9ebfa2',
            callback=process_data
        )
        print(f"Cost: {time.time() - start}")


if __name__ == "__main__":
    asyncio.run(httpx_example())
