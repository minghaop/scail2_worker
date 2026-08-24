# SCAIL-2 Dispatcher Worker

`scail2_worker_service.py` 是一个从头实现的 Dispatcher WebSocket Worker。
它只参考 `scail2_fastapi_service.py` 中的 SDK 生命周期，不复用其中的 HTTP API、
任务队列、健康检查或 Backend 实现。

## 固定运行合同

- 对外协议：`dispatcher_worker_protocol.md`
- workflow：`scail2_video`
- 模型目录：`/models`
- DiT checkpoint：
  `/models/derived/SCAIL-2-lightx2v-r128-dpo-alpha1-full-bf16.safetensors`
- profile：`scail2-512p-bf16-v1`
- 两个 torchrun rank、两张 GPU，共同串行执行一个任务
- 音频模式：`driving`，因此 driving video 必须带音轨
- 无等待队列；Worker 忙碌时新任务立即失败
- 每个任务必须提供四个 S3 下载和恰好一个 S3 上传 URL
- rank 0 的任务目录：`/dev/shm/<pid>`，每次任务准备前清空

服务只在 WebSocket `/` 上处理 `connect` 和 `submitTask`，不提供健康检查接口。

## Python 依赖

容器中需要：

- `scail2-inference==0.1.3` 及其交付镜像规定的 CUDA/PyTorch 依赖
- `fastapi`
- `uvicorn[standard]`

对象存储 GET/PUT 使用 Python 标准库，不需要 boto3 或 httpx。

## 启动

为 Podman 容器提供两张 GPU、只读 `/models` 映射和至少 2 GiB `/dev/shm`，然后执行：

```bash
torchrun \
  --standalone \
  --nnodes=1 \
  --nproc-per-node=2 \
  --max-restarts=0 \
  -m scail2_worker_service \
  --port 3000
```

`--port` 是唯一的业务命令行配置，默认值为 `3000`。torchrun 提供的 rank/world-size
环境变量仍由推理 SDK 使用。

## 构建 Worker 镜像

Dockerfile 使用 `localhost/scail2-inference:0.1.3` 基础镜像，该镜像需要已经包含
SCAIL-2 wheel、CUDA、PyTorch、FlashAttention 和 FFmpeg。构建过程会先安装 Git，
然后 shallow clone `minghaop/scail2_worker` 的最新 `main` 分支：

```bash
podman build --no-cache -t scail2-worker:latest .
```

`--no-cache` 确保每次构建都重新下载最新代码。Dockerfile 已将双 rank `torchrun`
设置为入口，并固定传入 `--port 3000`。镜像不使用 `EXPOSE` 声明端口，也不要求
把端口映射到宿主机；Dispatcher 通过可达的容器网络地址访问 Worker：

```bash
podman run <GPU、模型目录、共享内存和容器网络参数> \
  scail2-worker:latest
```

## workflow 参数

`params` 必须包含：

```text
reference_image
reference_mask
driving_video
driving_mask
prompt
```

前四个字段必须同时出现在 `s3.relative_path_fields` 中，其值必须分别匹配一个
`s3.downloads[].local_file`。`reference_mask` 必须是 PNG。

SDK 成功后，服务使用 `InferenceResult.output_path` 向 `s3.uploads[0]` 执行 HTTP PUT；
只有上传成功才发送 `completed/succeed`。

## 验证

当前目录中的协议和 Backend 单元测试不需要真实 SDK 或 GPU：

```bash
python3 -m unittest -v test_scail2_worker_service.py
```

真实推理必须在交付容器中使用上述双 rank 命令验证。
