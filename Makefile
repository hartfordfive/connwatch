BINARY   ?= connwatch
# The version lives in version.txt (edit it to release). Override with: make build VERSION=1.2.3
VERSION  ?= $(shell tr -d ' \t\r\n' < version.txt)
LDFLAGS  := -s -w -X main.version=$(VERSION)
DIST     := dist
DATA     := data
ARGS     ?=

# DB-IP Lite databases: free, no account, CC BY 4.0 (attribution required),
# published monthly. Falls back to last month's file early in the month.
DBIP_URL   := https://download.db-ip.com/free
THIS_MONTH := $(shell date -u +%Y-%m)
LAST_MONTH := $(shell date -u -v-1m +%Y-%m 2>/dev/null || date -u -d '1 month ago' +%Y-%m)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@awk 'BEGIN{FS=":.*## "} /^[a-zA-Z_-]+:.*## /{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: version
version: ## Print the version from version.txt
	@echo $(VERSION)

.PHONY: build
build: ## Build for this machine -> ./connwatch
	go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) .

.PHONY: build-all
build-all: ## Build macOS arm64 + amd64 and a universal binary into dist/
	@mkdir -p $(DIST)
	GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags '$(LDFLAGS)' -o $(DIST)/$(BINARY)-darwin-arm64 .
	GOOS=darwin GOARCH=amd64 go build -trimpath -ldflags '$(LDFLAGS)' -o $(DIST)/$(BINARY)-darwin-amd64 .
	@if command -v lipo >/dev/null; then \
		lipo -create -output $(DIST)/$(BINARY)-darwin-universal $(DIST)/$(BINARY)-darwin-arm64 $(DIST)/$(BINARY)-darwin-amd64 && \
		echo "built $(DIST)/$(BINARY)-darwin-universal"; fi

.PHONY: test
test: ## Run unit tests
	go test -race ./...

.PHONY: vet
vet: ## gofmt check + go vet
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)
	go vet ./...

.PHONY: check
check: vet test ## vet + test

.PHONY: run
run: build ## Build and run (sees every process, no sudo needed). Extra flags: make run ARGS="-interval 1s"
	./$(BINARY) -log-file ./connections.jsonl $(ARGS)

.PHONY: run-sudo
run-sudo: build ## Build and run as root, if some connections seem to be missing
	sudo ./$(BINARY) -log-file ./connections.jsonl $(ARGS)

.PHONY: run-headless
run-headless: build ## Run without the web UI: only /metrics and /healthz, fixed interval
	./$(BINARY) --headless -log-file ./connections.jsonl $(ARGS)

.PHONY: geoip
geoip: ## Download DB-IP Lite City + ASN databases into ./data (no account needed)
	@mkdir -p $(DATA)
	@for db in city asn; do \
		ok=; for m in $(THIS_MONTH) $(LAST_MONTH); do \
			url="$(DBIP_URL)/dbip-$$db-lite-$$m.mmdb.gz"; \
			if curl -fsSL "$$url" -o "$(DATA)/dbip-$$db-lite.mmdb.gz.tmp"; then \
				gunzip -c "$(DATA)/dbip-$$db-lite.mmdb.gz.tmp" > "$(DATA)/dbip-$$db-lite.mmdb.tmp" && \
				mv "$(DATA)/dbip-$$db-lite.mmdb.tmp" "$(DATA)/dbip-$$db-lite.mmdb" && \
				echo "downloaded dbip-$$db-lite ($$m)"; ok=1; break; \
			fi; \
		done; \
		rm -f "$(DATA)/dbip-$$db-lite.mmdb.gz.tmp"; \
		test -n "$$ok" || { echo "failed to download dbip-$$db-lite"; exit 1; }; \
	done

.PHONY: geoip-maxmind
geoip-maxmind: ## Download MaxMind GeoLite2 City + ASN via geoipupdate (needs GeoIP.conf with your free license key)
	@command -v geoipupdate >/dev/null || { echo "install it first: brew install geoipupdate"; exit 1; }
	@test -f GeoIP.conf || { echo "create GeoIP.conf (see README) with AccountID, LicenseKey and EditionIDs GeoLite2-City GeoLite2-ASN"; exit 1; }
	@mkdir -p $(DATA)
	geoipupdate -f GeoIP.conf -d $(DATA)

.PHONY: clear-cache
clear-cache: ## Delete the persisted DNS cache
	rm -f "$$HOME/Library/Caches/connwatch/dns.json"

.PHONY: clean
clean: ## Remove build output (keeps ./data)
	rm -rf $(BINARY) $(DIST)
