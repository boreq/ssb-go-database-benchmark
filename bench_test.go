package db_benchmark

import (
	"fmt"
	"github.com/boreq/db_benchmark/fixtures"
	"github.com/boreq/errors"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkDatabaseSystems(b *testing.B) {
	testedDatabaseSystems := getDatabaseSystems()
	benchmarks := getBenchmarks()

	for i := 0; i < b.N; i++ {
		for _, testedDatabaseSystem := range testedDatabaseSystems {
			b.Run(testedDatabaseSystem.Name, func(b *testing.B) {
				for _, benchmark := range benchmarks {
					b.Run(benchmark.Name, func(b *testing.B) {
						dir := fixtures.Directory(b)

						system, err := testedDatabaseSystem.DatabaseSystemConstructor(dir)
						if err != nil {
							b.Fatal(err)
						}

						if benchmark.SetupFunc != nil {
							if err := benchmark.SetupFunc(b, system); err != nil {
								b.Fatal(err)
							}
						}

						b.ResetTimer()
						b.StartTimer()

						for i := 0; i < b.N; i++ {
							if err := benchmark.Func(b, system); err != nil {
								b.Fatal(err)
							}
						}

						b.StopTimer()

						if err := system.Close(); err != nil {
							b.Fatal(err)
						}

						size, err := dirSize(dir)
						if err != nil {
							b.Fatal(err)
						}

						if b.N == 1 {
							b.Logf("Run with n=%d directory size: %d (%d MB)", b.N, size, size/1000/1000)
						}
					})
				}
			})
		}
	}
}

type TestedDatabaseSystem struct {
	Name                      string
	DatabaseSystemConstructor DatabaseSystemConstructor
}

type DatabaseSystemConstructor func(dir string) (DatabaseSystem, error)

func getDatabaseSystems() []TestedDatabaseSystem {
	return []TestedDatabaseSystem{
		{
			Name: "bolt",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewBoltDatabaseSystem(dir)
			},
		},
		{
			Name: "badger",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewBadgerDatabaseSystem(dir)
			},
		},
		{
			Name: "margaret",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewMargaretDatabaseSystem(dir)
			},
		},
	}
}

type Benchmark struct {
	Name      string
	SetupFunc BenchmarkFunc
	Func      BenchmarkFunc
}

type BenchmarkFunc func(b *testing.B, databaseSystem DatabaseSystem) error

func getBenchmarks() []Benchmark {
	var benchmarks []Benchmark

	const defaultSize = 1000

	for _, n := range []int{1, 10, 100, 1000} {
		numberOfAppendsToPerform := n
		benchmarks = append(benchmarks, []Benchmark{
			{
				Name: fmt.Sprintf("append_%d_random_values_separately", numberOfAppendsToPerform),
				Func: func(b *testing.B, databaseSystem DatabaseSystem) error {
					for i := 0; i < numberOfAppendsToPerform; i++ {
						if err := databaseSystem.Update(func(updater Updater) error {
							return updater.Append(fixtures.RandomBytes(defaultSize))
						}); err != nil {
							return errors.Wrap(err, "error calling update")
						}
					}

					return nil
				},
			},
			{
				Name: fmt.Sprintf("append_%d_random_values_together", numberOfAppendsToPerform),
				Func: func(b *testing.B, databaseSystem DatabaseSystem) error {
					if err := databaseSystem.Update(func(updater Updater) error {
						for i := 0; i < numberOfAppendsToPerform; i++ {
							if err := updater.Append(fixtures.RandomBytes(defaultSize)); err != nil {
								return errors.Wrap(err, "error calling set")
							}
						}
						return nil
					}); err != nil {
						return errors.Wrap(err, "error calling update")
					}

					return nil
				},
			},
		}...)
	}

	const readRandomSequencesMaxSequence = 100000
	const readRandomSequencesBatchSize = 1000

	benchmarks = append(benchmarks, []Benchmark{
		{
			Name: "read_random_sequences",
			SetupFunc: func(b *testing.B, databaseSystem DatabaseSystem) error {
				for i := 0; i < readRandomSequencesMaxSequence/readRandomSequencesBatchSize; i++ {
					if err := databaseSystem.Update(func(updater Updater) error {
						for i := 0; i <= readRandomSequencesBatchSize; i++ {
							if err := updater.Append(fixtures.RandomBytes(defaultSize)); err != nil {
								return errors.Wrap(err, "error calling set")
							}
						}
						return nil
					}); err != nil {
						return errors.Wrap(err, "error calling update")
					}
				}

				return nil
			},
			Func: func(b *testing.B, databaseSystem DatabaseSystem) error {
				if err := databaseSystem.Read(func(reader Reader) error {
					value, err := reader.Get(Sequence(rand.Intn(readRandomSequencesMaxSequence + 1)))
					if err != nil {
						return errors.Wrap(err, "error calling get")
					}

					require.NotEmpty(b, value)
					return nil
				}); err != nil {
					return errors.Wrap(err, "error calling read")
				}

				return nil
			},
		},
	}...)

	return benchmarks
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
