bench:
	go test -bench=./... | tee /tmp/bench.txt
.PHONY: bench

bench-report:
	cat /tmp/bench.txt | go run github.com/boreq/db_benchmark/cmd/report
.PHONY: bench-report

