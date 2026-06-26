# Windows 下载说明

本文档已简化，最新流程请直接参考：

- [Loongdata Windows 快速使用](/Users/Zhuanz/rx/code/data-download-sdk/docs/internal_distribution_guide.md)

当前版本的下载逻辑会在执行 `loongdata download ...` 前自动调用：

- `/data-miner/ak/createSignature`

根据 `dataset` 和 `session` 获取临时 `AK/SK/TOKEN`，因此用户侧不再需要手工准备 OBS AK/SK 配置文件。

用户只需要准备：

- `install_loongdata.ps1`
- `loongdata.cmd`
- `pip.ini`

安装后执行：

```powershell
.\loongdata.cmd download --dataset <dataset> --session <session> --host http://dev-dojo-api.openloong.org.cn
```
