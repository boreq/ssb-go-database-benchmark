package db_benchmark

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/boreq/db_benchmark/fixtures"
	"github.com/boreq/errors"
	"github.com/dgraph-io/badger/v4"
	badgeroptions "github.com/dgraph-io/badger/v4/options"
	"github.com/stretchr/testify/require"
)

func BenchmarkPerformance(b *testing.B) {
	testedDatabaseSystems := getDatabaseSystems(b)
	benchmarks := getBenchmarks()
	dataConstructors := getDataConstructors(b)
	storageSystems := getStorageSystems(b)

	for i := 0; i < b.N; i++ {
		for _, testedDatabaseSystem := range testedDatabaseSystems {
			b.Run(testedDatabaseSystem.Name, func(b *testing.B) {
				for _, storageSystem := range storageSystems {
					b.Run(storageSystem.Name, func(b *testing.B) {
						for _, dataConstructor := range dataConstructors {
							b.Run(dataConstructor.Name, func(b *testing.B) {
								for _, benchmark := range benchmarks {
									b.Run(benchmark.Name, func(b *testing.B) {
										env := BenchmarkEnvironment{
											DataConstructor: dataConstructor,
										}

										dir := fixtures.Directory(b, storageSystem.Path)

										system, err := testedDatabaseSystem.DatabaseSystemConstructor(dir)
										if err != nil {
											b.Fatal(err)
										}

										if benchmark.SetupFunc != nil {
											if err := benchmark.SetupFunc(b, system, env); err != nil {
												b.Fatal(err)
											}
										}

										b.ResetTimer()
										b.StartTimer()

										for i := 0; i < b.N; i++ {
											if err := benchmark.Func(b, system, env); err != nil {
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
			})
		}
	}
}

func BenchmarkSize(b *testing.B) {
	testedDatabaseSystems := getDatabaseSystems(b)
	dataConstructors := getDataConstructors(b)

	for i := 0; i < b.N; i++ {
		for _, testedDatabaseSystem := range testedDatabaseSystems {
			b.Run(testedDatabaseSystem.Name, func(b *testing.B) {
				for _, dataConstructor := range dataConstructors {
					b.Run(dataConstructor.Name, func(b *testing.B) {
						const maxValuesPerTransaction = 1000

						dir := fixtures.Directory(b, "")

						system, err := testedDatabaseSystem.DatabaseSystemConstructor(dir)
						if err != nil {
							b.Fatal(err)
						}

						b.ResetTimer()
						b.StartTimer()

						var insertedValues int

						for {
							valuesToInsert := b.N - insertedValues
							if valuesToInsert > maxValuesPerTransaction {
								valuesToInsert = maxValuesPerTransaction
							}

							if err := system.Update(func(updater Updater) error {
								for n := 0; n < valuesToInsert; n++ {
									if err := updater.Append(dataConstructor.Fn()); err != nil {
										return errors.Wrap(err, "error calling append")
									}
								}
								return nil
							}); err != nil {
								b.Fatal(err)
							}

							insertedValues += valuesToInsert
							if insertedValues >= b.N {
								break
							}
						}

						if err := system.Sync(); err != nil {
							b.Fatal(err)
						}

						b.StopTimer()

						if err := system.Close(); err != nil {
							b.Fatal(err)
						}

						size, err := dirSize(dir)
						if err != nil {
							b.Fatal(err)
						}

						bytesPerInsert := float64(size) / float64(b.N)
						b.Logf("Run bench=%s with b.n=%d directory size: %d (%.0f per insert)", b.Name(), b.N, size, bytesPerInsert)

						b.ReportMetric(bytesPerInsert, "bytes/op")
						b.ReportMetric(0, "ns/op")
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

func getDatabaseSystems(tb testing.TB) []TestedDatabaseSystem {
	var v []TestedDatabaseSystem

	if os.Getenv("ENABLE_BBOLT") != "" {
		for _, transactionSize := range []int{5000} {
			v = append(v,
				[]TestedDatabaseSystem{
					{
						Name: "bbolt_" + strconv.Itoa(transactionSize),
						DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
							return NewBoltDatabaseSystem(dir, nil, NewNoopBoltCodec(), transactionSize)
						},
					},
				}...,
			)

			if os.Getenv("ENABLE_BOLT_ON_COMPRESSION") != "" {
				v = append(v,
					[]TestedDatabaseSystem{
						{
							Name: "bbolt_snappy_" + strconv.Itoa(transactionSize),
							DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
								return NewBoltDatabaseSystem(dir, nil, NewSnappyBoltCodec(), transactionSize)
							},
						},
						{
							Name: "bbolt_zstd_" + strconv.Itoa(transactionSize),
							DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
								return NewBoltDatabaseSystem(dir, nil, NewZSTDBoltCodec(), transactionSize)
							},
						},
					}...,
				)
			}
		}
	} else {
		tb.Log("ENABLE_BBOLT is not set")
	}

	if os.Getenv("ENABLE_BADGER") != "" {
		for _, transactionSize := range []int{5000} {
			v = append(v,
				[]TestedDatabaseSystem{
					{
						Name: "badger_" + strconv.Itoa(transactionSize),
						DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
							return NewBadgerDatabaseSystem(dir, func(options *badger.Options) {
								options.Compression = badgeroptions.None
							}, transactionSize)
						},
					},
					{
						Name: "badger_snappy_" + strconv.Itoa(transactionSize),
						DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
							return NewBadgerDatabaseSystem(dir, func(options *badger.Options) {
								options.Compression = badgeroptions.Snappy
							}, transactionSize)
						},
					},
					{
						Name: "badger_zstd_" + strconv.Itoa(transactionSize),
						DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
							return NewBadgerDatabaseSystem(dir, func(options *badger.Options) {
								options.Compression = badgeroptions.ZSTD
							}, transactionSize)
						},
					},
				}...,
			)
		}
	} else {
		tb.Log("ENABLE_BADGER is not set")
	}

	if os.Getenv("ENABLE_MARGARET") != "" {
		v = append(v,
			[]TestedDatabaseSystem{
				{
					Name: "margaret",
					DatabaseSystemConstructor: func(dir string) (DatabaseSystem, error) {
						return NewMargaretDatabaseSystem(dir, NewMargaretCodec())
					},
				},
			}...,
		)

		if os.Getenv("ENABLE_BOLT_ON_COMPRESSION") != "" {
			v = append(v,
				[]TestedDatabaseSystem{
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
				}...,
			)
		}
	} else {
		tb.Log("ENABLE_MARGARET is not set")
	}

	return v
}

type BenchmarkEnvironment struct {
	DataConstructor DataConstructor
}

type Benchmark struct {
	Name      string
	SetupFunc BenchmarkFunc
	Func      BenchmarkFunc
}

type BenchmarkFunc func(b *testing.B, databaseSystem DatabaseSystem, env BenchmarkEnvironment) error

func getBenchmarks() []Benchmark {
	var benchmarks []Benchmark

	const numberOfAppendsToPerform = 5000

	benchmarks = append(benchmarks, []Benchmark{
		{
			Name: "append",
			Func: func(b *testing.B, databaseSystem DatabaseSystem, env BenchmarkEnvironment) error {
				for _, n := range batch(numberOfAppendsToPerform, databaseSystem.PreferredTransactionSize()) {
					if err := databaseSystem.Update(func(updater Updater) error {
						for i := 0; i < n; i++ {
							if err := updater.Append(env.DataConstructor.Fn()); err != nil {
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
		},
	}...)

	const readRandomSequencesMaxSequence = 100000
	const readRandomSequencesNumberOfSequencesToRead = 5000

	benchmarks = append(benchmarks, []Benchmark{
		{
			Name: "read_random",
			SetupFunc: func(b *testing.B, databaseSystem DatabaseSystem, env BenchmarkEnvironment) error {
				for _, n := range batch(readRandomSequencesMaxSequence, databaseSystem.PreferredTransactionSize()) {
					if err := databaseSystem.Update(func(updater Updater) error {
						for i := 0; i <= n; i++ {
							if err := updater.Append(env.DataConstructor.Fn()); err != nil {
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
			Func: func(b *testing.B, databaseSystem DatabaseSystem, env BenchmarkEnvironment) error {
				for _, n := range batch(readRandomSequencesNumberOfSequencesToRead, databaseSystem.PreferredTransactionSize()) {
					if err := databaseSystem.Read(func(reader Reader) error {
						for i := 0; i < n; i++ {
							value, err := reader.Get(Sequence(rand.Intn(readRandomSequencesMaxSequence + 1)))
							if err != nil {
								return errors.Wrap(err, "error calling get")
							}
							if len(value) == 0 {
								b.Fatal("got an empty value")
							}
						}
						return nil
					}); err != nil {
						return errors.Wrap(err, "error calling read")
					}
				}
				return nil
			},
		},
		{
			Name: "read_sequential",
			SetupFunc: func(b *testing.B, databaseSystem DatabaseSystem, env BenchmarkEnvironment) error {
				for _, n := range batch(readRandomSequencesMaxSequence, databaseSystem.PreferredTransactionSize()) {
					if err := databaseSystem.Update(func(updater Updater) error {
						for i := 0; i <= n; i++ {
							if err := updater.Append(env.DataConstructor.Fn()); err != nil {
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
			Func: func(b *testing.B, databaseSystem DatabaseSystem, env BenchmarkEnvironment) error {
				for _, n := range batch(readRandomSequencesNumberOfSequencesToRead, databaseSystem.PreferredTransactionSize()) {
					if err := databaseSystem.Read(func(reader Reader) error {
						for i := 0; i < n; i++ {
							value, err := reader.Get(Sequence(i))
							if err != nil {
								return errors.Wrap(err, "error calling get")
							}
							if len(value) == 0 {
								b.Fatal("got an empty value")
							}
						}
						return nil
					}); err != nil {
						return errors.Wrap(err, "error calling read")
					}
				}
				return nil
			},
		},
	}...)

	return benchmarks
}

type StorageSystem struct {
	Name string
	Path string
}

func getStorageSystems(tb testing.TB) []StorageSystem {
	var v []StorageSystem

	fast := os.Getenv("STORAGE_FAST")
	if fast == "" {
		tb.Log("STORAGE_FAST not set")
	} else {
		v = append(v,
			StorageSystem{
				Name: "fast_storage",
				Path: fast,
			},
		)
	}

	slow := os.Getenv("STORAGE_SLOW")
	if slow == "" {
		tb.Log("STORAGE_SLOW not set")
	} else {
		v = append(v,
			StorageSystem{
				Name: "slow_storage",
				Path: slow,
			},
		)
	}

	return v
}

type DataConstructor struct {
	Name string
	Fn   func() []byte
}

func getDataConstructors(tb testing.TB) []DataConstructor {
	var v []DataConstructor

	if os.Getenv("ENABLE_DATA_RANDOM") != "" {
		v = append(v,
			DataConstructor{
				Name: "random_data",
				Fn: func() []byte {
					return fixtures.RandomBytes(1000)
				},
			},
		)
	} else {
		tb.Log("ENABLE_DATA_RANDOM is not set")
	}

	if os.Getenv("ENABLE_DATA_LIKE_SSB") != "" {
		v = append(v,
			DataConstructor{
				Name: "data_similar_to_ssb_messages",
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
		)
	} else {
		tb.Log("ENABLE_DATA_LIKE_SSB is not set")
	}

	return v
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

func TestBatch(t *testing.T) {
	require.Equal(t,
		[]int{
			100,
			100,
		},
		batch(200, 100),
	)

	require.Equal(t,
		[]int{
			100,
			100,
			50,
		},
		batch(250, 100),
	)

	require.Equal(t,
		[]int{
			33,
		},
		batch(33, 100),
	)

	require.Equal(t,
		[]int{
			33,
			33,
			33,
			1,
		},
		batch(100, 33),
	)
}

func batch(total, batchSize int) []int {
	var batches []int

	for {
		if total > batchSize {
			batches = append(batches, batchSize)
			total -= batchSize
		} else {
			batches = append(batches, total)
			break
		}
	}

	return batches
}
