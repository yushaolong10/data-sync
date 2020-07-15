.PHONY: all

ModuleName=iot-data-sync

all:  build

build:
	go build  -o ${ModuleName} ./src/main

fmt:
	gofmt -w ./src

clean:
	rm -rf ${ModuleName}
