package db_benchmark

import (
	"path"

	"github.com/boreq/errors"
	"github.com/golang/snappy"
	"go.etcd.io/bbolt"
)

type BoltCodec interface {
	Encode(b []byte) ([]byte, error)
	Decode(b []byte) ([]byte, error)
}

type BoltDatabaseSystem struct {
	db              *bbolt.DB
	codec           BoltCodec
	transactionSize int
}

func NewBoltDatabaseSystem(dir string, fn func(options *bbolt.Options), codec BoltCodec, transactionSize int) (*BoltDatabaseSystem, error) {
	options := *bbolt.DefaultOptions

	if fn != nil {
		fn(&options)
	}

	f := path.Join(dir, "database.bolt")
	db, err := bbolt.Open(f, 0600, &options)
	if err != nil {
		return nil, errors.Wrap(err, "error opening the database")
	}

	return &BoltDatabaseSystem{db: db, codec: codec, transactionSize: transactionSize}, nil
}

func (b *BoltDatabaseSystem) PreferredTransactionSize() int {
	return b.transactionSize
}

func (b *BoltDatabaseSystem) Update(fn func(updater Updater) error) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		updater, err := NewTxBoltDatabaseSystem(tx, b.codec)
		if err != nil {
			return errors.Wrap(err, "error creating a tx database system")
		}

		return fn(updater)
	})
}

func (b *BoltDatabaseSystem) Read(fn func(reader Reader) error) error {
	return b.db.View(func(tx *bbolt.Tx) error {
		updater, err := NewTxBoltDatabaseSystem(tx, b.codec)
		if err != nil {
			return errors.Wrap(err, "error creating a tx database system")
		}

		return fn(updater)
	})
}

func (b *BoltDatabaseSystem) Close() error {
	return b.db.Close()
}

func (b *BoltDatabaseSystem) Sync() error {
	return b.db.Sync()
}

var boltBucketName = []byte("values")

type TxBoltDatabaseSystem struct {
	bucket *bbolt.Bucket
	codec  BoltCodec
}

func NewTxBoltDatabaseSystem(tx *bbolt.Tx, codec BoltCodec) (*TxBoltDatabaseSystem, error) {
	s := &TxBoltDatabaseSystem{
		codec: codec,
	}

	if tx.Writable() {
		bucket, err := tx.CreateBucketIfNotExists(boltBucketName)
		if err != nil {
			return nil, errors.Wrap(err, "error creating the bucket")
		}

		s.bucket = bucket
	} else {
		s.bucket = tx.Bucket(boltBucketName)
	}

	return s, nil
}

func (t *TxBoltDatabaseSystem) Append(value []byte) error {
	seq, err := t.getNextSequence()
	if err != nil {
		return errors.Wrap(err, "error calling get next sequence")
	}

	encodedValue, err := t.codec.Encode(value)
	if err != nil {
		return errors.Wrap(err, "error calling encode")
	}

	return t.bucket.Put(marshalSequence(seq), encodedValue)
}

func (t *TxBoltDatabaseSystem) Get(seq Sequence) ([]byte, error) {
	encodedValue := t.bucket.Get(marshalSequence(seq))

	value, err := t.codec.Decode(encodedValue)
	if err != nil {
		return nil, errors.Wrap(err, "error calling encode")
	}

	return value, nil
}

func (t *TxBoltDatabaseSystem) getNextSequence() (Sequence, error) {
	seqInt, err := t.bucket.NextSequence()
	if err != nil {
		return 0, errors.Wrap(err, "error calling next sequence")
	}

	return Sequence(seqInt - 1), nil
}

type NoopBoltCodec struct {
}

func NewNoopBoltCodec() NoopBoltCodec {
	return NoopBoltCodec{}
}

func (n NoopBoltCodec) Encode(b []byte) ([]byte, error) {
	return b, nil
}

func (n NoopBoltCodec) Decode(b []byte) ([]byte, error) {
	return b, nil
}

type SnappyBoltCodec struct {
}

func NewSnappyBoltCodec() *SnappyBoltCodec {
	return &SnappyBoltCodec{}
}

func (s SnappyBoltCodec) Encode(b []byte) ([]byte, error) {
	return snappy.Encode(nil, b), nil
}

func (s SnappyBoltCodec) Decode(b []byte) ([]byte, error) {
	return snappy.Decode(nil, b)
}

type ZSTDBoltCodec struct {
}

func NewZSTDBoltCodec() *ZSTDBoltCodec {
	return &ZSTDBoltCodec{}
}

func (s ZSTDBoltCodec) Encode(b []byte) ([]byte, error) {
	return zstdEncoder.EncodeAll(b, nil), nil
}

func (s ZSTDBoltCodec) Decode(b []byte) ([]byte, error) {
	return zstdDecoder.DecodeAll(b, nil)
}
