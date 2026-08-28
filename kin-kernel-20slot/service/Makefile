.PHONY: static-check fmt test-rust test-go verify compose-up compose-down smoke package

static-check:
	python3 scripts/validate.py

fmt:
	cd kernel && cargo fmt --check
	cd control && gofmt -l . | tee /tmp/kin-gofmt.out && test ! -s /tmp/kin-gofmt.out

test-rust:
	cd kernel && cargo test --all-targets

test-go:
	cd control && go test ./...

verify: static-check fmt test-rust test-go
	cd kernel && cargo clippy --all-targets -- -D warnings
	cd control && go vet ./...

compose-up:
	docker compose -f deploy/compose.yaml up --build -d

compose-down:
	docker compose -f deploy/compose.yaml down

smoke:
	bash scripts/smoke.sh

package:
	cd .. && zip -qr kin-gateway-v2.zip kin-gateway-v2 -x '*/target/*' '*/.git/*'
