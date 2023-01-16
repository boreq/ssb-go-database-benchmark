package db_benchmark

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/boreq/db_benchmark/fixtures"
	"github.com/boreq/errors"
	"github.com/dgraph-io/badger/v3"
	badgeroptions "github.com/dgraph-io/badger/v3/options"
	"github.com/stretchr/testify/require"
)

func BenchmarkPerformance(b *testing.B) {
	testedDatabaseSystems := getDatabaseSystems()
	benchmarks := getBenchmarks()
	storageSystems, err := getStorageSystems()
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		for _, testedDatabaseSystem := range testedDatabaseSystems {
			b.Run(testedDatabaseSystem.Name, func(b *testing.B) {
				for _, storageSystem := range storageSystems {
					b.Run(storageSystem.Name, func(b *testing.B) {
						for _, benchmark := range benchmarks {
							b.Run(benchmark.Name, func(b *testing.B) {
								dir := fixtures.Directory(b, storageSystem.Path)

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

								if err := system.Sync(); err != nil {
									b.Fatal(err)
								}

								b.StopTimer()

								if err := system.Close(); err != nil {
									b.Fatal(err)
								}
							})
						}
					})
				}
			})
		}
	}
}

func BenchmarkSize(b *testing.B) {
	testedDatabaseSystems := getDatabaseSystems()
	dataConstructors := getDataConstructors()

	for i := 0; i < b.N; i++ {
		for _, testedDatabaseSystem := range testedDatabaseSystems {
			b.Run(testedDatabaseSystem.Name, func(b *testing.B) {
				for _, dataConstructor := range dataConstructors {
					b.Run(dataConstructor.Name, func(b *testing.B) {
						dir := fixtures.Directory(b, "")

						system, err := testedDatabaseSystem.DatabaseSystemConstructor(dir)
						if err != nil {
							b.Fatal(err)
						}

						const valuesPerN = 1000

						for i := 0; i < b.N; i++ {
							if err := system.Update(func(updater Updater) error {
								for j := 0; j < valuesPerN; j++ {
									if err := updater.Append(dataConstructor.Fn()); err != nil {
										return errors.Wrap(err, "error calling set")
									}
								}
								return nil
							}); err != nil {
								b.Fatal(err)
							}
						}

						if err := system.Sync(); err != nil {
							b.Fatal(err)
						}

						if err := system.Close(); err != nil {
							b.Fatal(err)
						}

						size, err := dirSize(dir)
						if err != nil {
							b.Fatal(err)
						}

						bytesPerInsert := float64(size) / float64(b.N) / float64(valuesPerN)

						b.ReportMetric(bytesPerInsert, "bytes/op")
						b.ReportMetric(0, "ns/op")
						b.Logf("Run bench=%s with n=%d directory size: %d (%.0f per insert)", b.Name(), b.N, size, bytesPerInsert)
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
	const badgerValueThreshold = 256
	const badgerValueLogFileSize = 32 * 1024 * 1024

	return []TestedDatabaseSystem{
		{
			Name: "bolt",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewBoltDatabaseSystem(dir, nil, NewNoopBoltCodec())
			},
		},
		{
			Name: "bolt_snappy",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewBoltDatabaseSystem(dir, nil, NewSnappyBoltCodec())
			},
		},
		{
			Name: "bolt_zstd",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewBoltDatabaseSystem(dir, nil, NewZSTDBoltCodec())
			},
		},

		{
			Name: "badger",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewBadgerDatabaseSystem(dir, func(options *badger.Options) {
					options.Compression = badgeroptions.None
				})
			},
		},
		{
			Name: "badger_snappy",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewBadgerDatabaseSystem(dir, func(options *badger.Options) {
					options.Compression = badgeroptions.Snappy
				})
			},
		},
		{
			Name: "badger_zstd",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewBadgerDatabaseSystem(dir, func(options *badger.Options) {
					options.Compression = badgeroptions.ZSTD
				})
			},
		},
		{
			Name: "badger_snappy_threshold",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewBadgerDatabaseSystem(dir, func(options *badger.Options) {
					options.Compression = badgeroptions.Snappy
					options.ValueThreshold = badgerValueThreshold
					options.ValueLogFileSize = badgerValueLogFileSize
				})
			},
		},
		{
			Name: "badger_zstd_threshold",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewBadgerDatabaseSystem(dir, func(options *badger.Options) {
					options.Compression = badgeroptions.ZSTD
					options.ValueThreshold = badgerValueThreshold
					options.ValueLogFileSize = badgerValueLogFileSize
				})
			},
		},

		{
			Name: "margaret",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewMargaretDatabaseSystem(dir, NewMargaretCodec())
			},
		},
		{
			Name: "margaret_snappy",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewMargaretDatabaseSystem(dir, NewMargaretSnappyCodec())
			},
		},
		{
			Name: "margaret_zstd",
			DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
				return NewMargaretDatabaseSystem(dir, NewMargaretZSTDCodec())
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

	const sizeOfInsertedData = 1000

	for _, n := range []int{1, 1000} {
		numberOfAppendsToPerform := n
		benchmarks = append(benchmarks, []Benchmark{
			{
				Name: fmt.Sprintf("append_%04d_values", numberOfAppendsToPerform),
				Func: func(b *testing.B, databaseSystem DatabaseSystem) error {
					if err := databaseSystem.Update(func(updater Updater) error {
						for i := 0; i < numberOfAppendsToPerform; i++ {
							if err := updater.Append(fixtures.RandomBytes(sizeOfInsertedData)); err != nil {
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

	for _, n := range []int{1, 1000} {
		numberOfSequencesToRead := n
		benchmarks = append(benchmarks, []Benchmark{
			{
				Name: fmt.Sprintf("read_%04d_random_sequences", numberOfSequencesToRead),
				SetupFunc: func(b *testing.B, databaseSystem DatabaseSystem) error {
					for i := 0; i < readRandomSequencesMaxSequence/readRandomSequencesBatchSize; i++ {
						if err := databaseSystem.Update(func(updater Updater) error {
							for i := 0; i <= readRandomSequencesBatchSize; i++ {
								if err := updater.Append(fixtures.RandomBytes(sizeOfInsertedData)); err != nil {
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
						for i := 0; i < numberOfSequencesToRead; i++ {
							value, err := reader.Get(Sequence(rand.Intn(readRandomSequencesMaxSequence + 1)))
							if err != nil {
								return errors.Wrap(err, "error calling get")
							}

							require.NotEmpty(b, value)
						}

						return nil
					}); err != nil {
						return errors.Wrap(err, "error calling read")
					}

					return nil
				},
			},
		}...)
	}

	return benchmarks
}

type StorageSystem struct {
	Name string
	Path string
}

func getStorageSystems() ([]StorageSystem, error) {
	fast := os.Getenv("STORAGE_FAST")
	if fast == "" {
		return nil, errors.New("please set STORAGE_FAST")
	}

	return []StorageSystem{
		{
			Name: "fast",
			Path: fast,
		},
	}, nil
}

type DataConstructor struct {
	Name string
	Fn   func() []byte
}

func getDataConstructors() []DataConstructor {
	return []DataConstructor{
		{
			Name: "random",
			Fn: func() []byte {
				return fixtures.RandomBytes(1000)
			},
		},
		{
			Name: "similar_to_ssb_messages",
			Fn: func() []byte {
				return []byte(
					fmt.Sprintf(
						`{
						"previous": "%%%s.sha256",
						"author": "@%s.ed25519",
						"sequence": %d,
						"timestamp": %d,
						"hash": "sha256",
						"content": {
							"type": "post",
							"text": "%s"
						}
					}`,
						base64.StdEncoding.EncodeToString(fixtures.RandomBytes(32)),
						base64.StdEncoding.EncodeToString(fixtures.RandomBytes(32)),
						rand.Uint64()%10000,
						rand.Uint64(),
						base64.StdEncoding.EncodeToString(fixtures.RandomBytes(100)),
					),
				)
			},
		},
	}
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
