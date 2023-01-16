bench:
	go test -bench=./... | tee /tmp/bench.txt
.PHONY: bench

bench-report:
	cat /tmp/bench.txt | go run github.com/boreq/db_benchmark/cmd/report
.PHONY: bench-report

tools:
	go install github.com/rinchsan/gosimports/cmd/gosimports@latest # https://github.com/golang/go/issues/20818
.PHONY: tools

fmt:
	gosimports -l -w ./
.PHONY: fmt
