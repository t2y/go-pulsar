# for makefile
SHELL := /bin/bash
export PATH := ${GOPATH}/bin:$(PATH)

# pulsar info
PULSAR_API_DIR = proto
PULSAR_API_PROTO = PulsarApi.proto
PULSAR_API_URL = https://raw.githubusercontent.com/yahoo/pulsar/master/pulsar-common/src/main/proto/${PULSAR_API_PROTO}
PULSAR_API_PROTO_PATH = ${PULSAR_API_DIR}/${PULSAR_API_PROTO}


all: build

_gen:
	mkdir -p ${PULSAR_API_DIR}
	curl -L ${PULSAR_API_URL} -o ${PULSAR_API_PROTO_PATH}
	protoc --version

install-pb:
	go get -u github.com/golang/protobuf/protoc-gen-go

gen-pb: _gen
	@echo "# use pb"
	mkdir -p ${PULSAR_API_DIR}/pb
	protoc --go_out=${PULSAR_API_DIR}/pb --proto_path=${PULSAR_API_DIR} ${PULSAR_API_PROTO_PATH}

gen-gogopb: _gen
	@echo "# use gogo-pb"
	mkdir -p ${PULSAR_API_DIR}/gogo-pb
	protoc --gogo_out=${PULSAR_API_DIR}/gogo-pb --proto_path=${PULSAR_API_DIR} ${PULSAR_API_PROTO_PATH}

install-glide:
	go get github.com/Masterminds/glide

.PHONY: deps
deps:
	@echo "deps"
	glide cache-clear
	glide update

.PHONY: build
build:
	@mkdir -p bin
	go build -o ./bin/pulsar-client ./cli/pulsar-client

.PHONY: clean
clean:
	rm -f .bin/pulsar-client

.PHONY: test
test:
	go test .
