package db_benchmark

import "encoding/binary"

type DatabaseSystem interface {
	Update(func(updater Updater) error) error
	Read(func(reader Reader) error) error
	Close() error
}

type Updater interface {
	Append(value []byte) error
}

type Reader interface {
	Get(seq Sequence) ([]byte, error)
}

type Sequence uint64

func marshalSequence(v Sequence) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func unmarshalSequence(b []byte) Sequence {
	return Sequence(binary.LittleEndian.Uint64(b))
}
