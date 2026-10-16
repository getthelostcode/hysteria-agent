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
# 版本标签取「最后改动 agent 源码的提交」，不是 HEAD：打包/文档提交不改代码，
# 却会让二进制自报另一个 commit —— 同一份代码两个身份（与 install.sh 的 agent_source_commit 一致）。
COMMIT     := $(shell git log -1 --format=%h -- cmd internal '*.go' 2>/dev/null || git rev-parse --short HEAD 2>/dev/null || echo dev)$(shell git status --porcelain -- cmd internal '*.go' 2>/dev/null | grep -q . && echo -dirty)
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GO         ?= /usr/local/go/bin/go
DIST       := dist
RELEASE    := release
VERSION    ?= 0.1.0
UPGRADE_PUBKEY ?=

TARBALL_AMD64 := $(RELEASE)/hysteria-agent-$(VERSION)-linux-amd64.tar.gz
TARBALL_ARM64 := $(RELEASE)/hysteria-agent-$(VERSION)-linux-arm64.tar.gz

LDFLAGS := -s -w \
  -X $(MODULE)/internal/version.Version=$(VERSION) \
  -X $(MODULE)/internal/version.Commit=$(COMMIT) \
  -X $(MODULE)/internal/version.BuildTime=$(BUILD_TIME)

ifneq ($(UPGRADE_PUBKEY),)
LDFLAGS += -X $(MODULE)/internal/version.UpgradePublicKey=$(UPGRADE_PUBKEY)
endif

.PHONY: all build tools proto test vet e2e dist release clean install smoke-release smoke-cp

all: build tools

build:
	mkdir -p bin
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/$(BIN) ./cmd/hysteria-agent
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/controlplane ./cmd/controlplane
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
	printf '%s\n' '$(COMMIT)' > $(DIST)/$(BIN)-linux-amd64.commit   # install.sh 靠它判断发布产物过期没有
	@echo "--- 产物 ---"
	@ls -l $(DIST)

# release 产出"解压即可用"的完整包：
#   hysteria-agent-<版本>-linux-<架构>/
#     ├── bin/          agent + mockcontrol/mockhysteria/agent-sign（静态、无动态依赖）
#     ├── install.sh    一键安装（会自动发现同包内的 bin/hysteria-agent）
#     ├── deploy/       config.yaml + systemd 单元
#     ├── README.md / DESIGN.md
#     └── SHA256SUMS
# 用法：make release            （amd64 + arm64）
#      make UPGRADE_PUBKEY=xxx release   （把自升级验签公钥编进二进制）
release:
	rm -rf $(RELEASE)
	@for arch in amd64 arm64; do \
	  name=hysteria-agent-$(VERSION)-linux-$$arch; \
	  dir=$(RELEASE)/$$name; \
	  echo ">>> 构建 $$name"; \
	  mkdir -p $$dir/bin; \
	  CGO_ENABLED=0 GOOS=linux GOARCH=$$arch $(GO) build -trimpath \
	    -ldflags '$(LDFLAGS) -X $(MODULE)/internal/version.Version=$(VERSION)' \
	    -o $$dir/bin/$(BIN) ./cmd/hysteria-agent || exit 1; \
	  CGO_ENABLED=0 GOOS=linux GOARCH=$$arch $(GO) build -trimpath -o $$dir/bin/mockcontrol ./cmd/mockcontrol || exit 1; \
	  CGO_ENABLED=0 GOOS=linux GOARCH=$$arch $(GO) build -trimpath -o $$dir/bin/mockhysteria ./cmd/mockhysteria || exit 1; \
	  CGO_ENABLED=0 GOOS=linux GOARCH=$$arch $(GO) build -trimpath -o $$dir/bin/agent-sign ./cmd/agent-sign || exit 1; \
	  CGO_ENABLED=0 GOOS=linux GOARCH=$$arch $(GO) build -trimpath \
	    -ldflags '$(LDFLAGS) -X $(MODULE)/internal/version.Version=$(VERSION)' \
	    -o $$dir/bin/controlplane ./cmd/controlplane || exit 1; \
	  cp install.sh $$dir/install.sh; \
	  mkdir -p $$dir/deploy; cp deploy/config.yaml deploy/hysteria-agent.service \
	    deploy/controlplane.yaml deploy/controlplane.service $$dir/deploy/ 2>/dev/null || true; \
	  cp README.md DESIGN.md $$dir/; \
	  ( cd $$dir && sha256sum bin/* > SHA256SUMS ); \
	  tar -czf $(RELEASE)/$$name.tar.gz -C $(RELEASE) $$name; \
	done
	( cd $(RELEASE) && sha256sum *.tar.gz > SHA256SUMS )
	@echo "--- 发布产物 ---"
	@ls -lh $(RELEASE)/*.tar.gz
	@echo "--- 校验和（$(RELEASE)/SHA256SUMS）---"
	@cat $(RELEASE)/SHA256SUMS
	@echo "--- 每个包内的 bin 校验和见 <包>/SHA256SUMS ---"
	@cat $(RELEASE)/hysteria-agent-$(VERSION)-linux-amd64/SHA256SUMS

# smoke-release：用发布包里的二进制真跑一遍（enroll → 长连接 → /readyz 200）
smoke-release:
	bash tests/smoke_release.sh

# smoke-cp：控制面端到端（init → serve → enroll → 下发配置 → 指令 → 事件 → 优雅停止）
smoke-cp: build
	bash tests/controlplane_smoke.sh

clean:
	rm -rf bin $(DIST)
