# hysteria-agent 构建
#
# 常用目标：
#   make build          构建 agent（含版本注入）
#   make UPGRADE_PUBKEY=<base64> build
#                       生产构建：把自升级验签公钥编进二进制（agent-sign pubkey 导出）
#   make tools          构建 mockcontrol / mockhysteria / agent-sign
#   make proto          重新生成 protobuf Go 代码
#   make test           单元测试
#   make e2e            端到端联调（需要 root 可用、openssl、python3）
#   make dist           产出 release 目录（二进制 + 校验和）

SHELL      := /bin/bash
MODULE     := github.com/getthelostcode/hysteria-agent
BIN        := hysteria-agent
VERSION    ?= 0.1.0
COMMIT     := $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GO         ?= /usr/local/go/bin/go
DIST       := dist
UPGRADE_PUBKEY ?=

LDFLAGS := -s -w \
  -X $(MODULE)/internal/version.Version=$(VERSION) \
  -X $(MODULE)/internal/version.Commit=$(COMMIT) \
  -X $(MODULE)/internal/version.BuildTime=$(BUILD_TIME)

ifneq ($(UPGRADE_PUBKEY),)
LDFLAGS += -X $(MODULE)/internal/version.UpgradePublicKey=$(UPGRADE_PUBKEY)
endif

.PHONY: all build tools proto test vet e2e dist clean install

all: build tools

build:
	mkdir -p bin
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/$(BIN) ./cmd/hysteria-agent
	@echo "--- 版本信息 ---"
	@./bin/$(BIN) version
	@if [ -z "$(UPGRADE_PUBKEY)" ]; then \
	  echo "提示: 未指定 UPGRADE_PUBKEY，自升级将只能依赖配置文件里的 upgrade.pubkeys"; \
	fi

tools:
	mkdir -p bin
	$(GO) build -trimpath -o bin/mockcontrol  ./cmd/mockcontrol
	$(GO) build -trimpath -o bin/mockhysteria ./cmd/mockhysteria
	$(GO) build -trimpath -o bin/agent-sign   ./cmd/agent-sign

proto:
	protoc -I proto --go_out=. --go_opt=module=$(MODULE) proto/agent.proto

test:
	$(GO) test ./... -count=1

vet:
	$(GO) vet ./...

e2e: build tools
	bash tests/e2e.sh

dist: build
	mkdir -p $(DIST)
	cp bin/$(BIN) $(DIST)/$(BIN)-linux-amd64
	cd $(DIST) && sha256sum $(BIN)-linux-amd64 > $(BIN)-linux-amd64.sha256
	@echo "--- 产物 ---"
	@ls -l $(DIST)

clean:
	rm -rf bin $(DIST)
