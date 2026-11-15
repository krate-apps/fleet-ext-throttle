.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/throttle-worker ./cmd/throttle-worker

pack:
	bash scripts/pack.sh dist
