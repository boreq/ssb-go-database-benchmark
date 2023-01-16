package db_benchmark

import "github.com/klauspost/compress/zstd"

var (
	zstdDecoder *zstd.Decoder
	zstdEncoder *zstd.Encoder
)

func init() {
	var err error

	zstdDecoder, err = zstd.NewReader(nil)
	if err != nil {
		panic(err)
	}

	zstdEncoder, err = zstd.NewWriter(nil)
	if err != nil {
		panic(err)
	}
}
