APP?=storage
BIN?=./bin/$(APP)
GO?=go
LOCAL_BIN:=$(CURDIR)/bin

.PHONY: build
build:
	$(GO) build -o $(BIN) ./cmd/$(APP)

.PHONY: run
run: build
	$(BIN)
