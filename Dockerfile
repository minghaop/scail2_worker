FROM localhost/scail2-inference:0.1.3

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        iproute2 \
        libgl1 \
        libglib2.0-0 \
        lsof \
        nano \
        unzip \
        wget \
    && rm -rf /var/lib/apt/lists/*

RUN git clone \
        --depth 1 \
        --branch main \
        https://github.com/minghaop/scail2_worker.git \
        /opt/scail2_worker \
    && rm -rf /opt/scail2_worker/.git \
    && python3.10 -m pip install --no-cache-dir \
        -r /opt/scail2_worker/requirements.txt

WORKDIR /opt/scail2_worker

ENTRYPOINT ["torchrun", "--standalone", "--nnodes=1", "--nproc-per-node=2", "--max-restarts=0", "-m", "scail2_worker_service"]
CMD ["--port", "3000"]
