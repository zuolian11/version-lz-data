import os
from pathlib import Path


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _parse_env_line(line: str):
    line = line.strip()
    if not line or line.startswith("#"):
        return None, None
    if line.startswith("export "):
        line = line[len("export "):].strip()
    if "=" not in line:
        return None, None
    key, value = line.split("=", 1)
    key = key.strip()
    value = _strip_quotes(value.strip())
    if not key:
        return None, None
    return key, value


def load_local_env():
    # 优先读取用户级配置文件，让 loongdata 原始命令不必额外拼环境变量。
    env_file = os.getenv("LOONGDATA_ENV_FILE")
    candidates = []
    if env_file:
        candidates.append(Path(env_file).expanduser())
    candidates.append(Path.home() / ".loongdata.env")

    for candidate in candidates:
        if not candidate.is_file():
            continue
        for raw_line in candidate.read_text(encoding="utf-8").splitlines():
            key, value = _parse_env_line(raw_line)
            if key and value and key not in os.environ:
                # 显式传入的环境变量优先级更高，本地配置只补缺省值。
                os.environ[key] = value
        return str(candidate)
    return None
