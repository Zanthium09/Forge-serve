# Every target's command is runnable directly if make is not installed locally.
MODEL_URL  = https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct-GGUF/resolve/main/qwen2.5-1.5b-instruct-q4_k_m.gguf
MODEL_FILE = models/qwen2.5-1.5b-instruct-q4_k_m.gguf

.PHONY: run test lint model up bench plots

run:            ## run gateway locally
	cd gateway && go run ./cmd/gateway

test:           ## unit tests
	cd gateway && go test ./...

lint:           ## requires golangci-lint (CI always runs it)
	cd gateway && golangci-lint run

model: $(MODEL_FILE)  ## download quantized GGUF (~1.1 GB, not gated)

$(MODEL_FILE):
	mkdir -p models
	curl -L --fail -o $(MODEL_FILE) $(MODEL_URL)

up:             ## full stack
	docker compose up --build

bench:          ## sequential vs batched matrix (~15 min, local hardware only)
	cd loadgen && go build -o loadgen.exe .
	bash benchmarks/run_bench.sh

plots:          ## turn benchmarks/raw/*.csv into tables and plots
	python benchmarks/plot.py
