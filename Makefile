IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
PLATFORM ?= linux/amd64
DEV   ?= dev
ANSWER ?= dev-answer
PYTHON ?= python3
GENERATE ?= generate
FIXTURES ?= fixtures
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures dev run-dev check-dev test verify

build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

fixtures: build
	$(PYTHON) scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

dev:
	$(GENERATE) --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

check-dev:
	$(PYTHON) scripts/compare.py out $(ANSWER)

test:
	cargo test --release

verify: test
	cargo build --release
	$(PYTHON) scripts/run_fixtures.py $(FIXTURES) -- $(BIN)
	$(PYTHON) scripts/verify_seeds.py --bin $(BIN) --generate $(GENERATE) --dev $(DEV) --answer $(ANSWER)
