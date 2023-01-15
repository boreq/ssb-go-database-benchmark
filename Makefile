bench:
	go test -bench=./... | tee /tmp/bench.txt
.PHONY: bench

bench-chart:
	cat /tmp/bench.txt | go run github.com/boreq/db_benchmark/cmd/chart
.PHONY: bench-chart

