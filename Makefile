export GO111MODULE:=auto
export GOPROXY:=direct
export GOSUMDB:=off
export CGO_ENABLED:=0
export GOOS:=linux
export GOARCH:=amd64

COMMIT_NUMBER ?= latest

.DEFAULT_GOAL := build

run:
	docker run -it --rm \
	--volume "$$HOME/.cache/go-build:/root/.cache/go-build" \
	--volume "$$GOPATH:/go" \
	--workdir "/go/src/github.com/hasansino/kafka-replicator" \
	--env-file .env \
	hasansino/golang:latest build "$$@"

build:
	@[ -d .build ] || mkdir -p .build
	go build  -ldflags="-s -w " -o .build/app main.go
	file  .build/app
	du -h .build/app

image: build
	cp Dockerfile .build
	cd .build && docker build -t hasansino/kafka-replicator:${COMMIT_NUMBER} .
	rm -rf .build
