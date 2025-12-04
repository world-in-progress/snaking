FROM docker.io/library/golang:1.25-bookworm

ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY

ENV http_proxy=${HTTP_PROXY} \
    https_proxy=${HTTPS_PROXY} \
    no_proxy=${NO_PROXY} \
    HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=${NO_PROXY}

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    swig \
    curl \
    ca-certificates \
    git \
    protobuf-compiler \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

ARG GIT_USER_NAME
ARG GIT_USER_EMAIL

RUN git config --global http.proxy ${HTTP_PROXY} && \
    git config --global https.proxy ${HTTPS_PROXY} && \
    git config --global user.name "${GIT_USER_NAME}" && \
    git config --global user.email "${GIT_USER_EMAIL}"

# Install Go tools
RUN go install -v golang.org/x/tools/gopls@latest && \
    go install -v github.com/go-delve/delve/cmd/dlv@latest && \
    go install -v honnef.co/go/tools/cmd/staticcheck@latest && \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Install uv (Universal Version Manager) and add to PATH
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# Create a uv python environment with Python 3.13
RUN uv python install 3.13 && \
    uv python pin 3.13