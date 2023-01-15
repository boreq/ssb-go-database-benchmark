package db_benchmark

import (
	"github.com/boreq/errors"
	"go.etcd.io/bbolt"
	"path"
)

type BoltDatabaseSystem struct {
	db *bbolt.DB
}

func NewBoltDatabaseSystem(dir string) (*BoltDatabaseSystem, error) {
	f := path.Join(dir, "database.bolt")
	db, err := bbolt.Open(f, 0600, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error opening the database")
	}

	return &BoltDatabaseSystem{db: db}, nil
}

func (b *BoltDatabaseSystem) Update(fn func(updater Updater) error) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		updater, err := NewTxBoltDatabaseSystem(tx)
		if err != nil {
			return errors.Wrap(err, "error creating a tx database system")
		}

		return fn(updater)
	})
}

func (b *BoltDatabaseSystem) Read(fn func(reader Reader) error) error {
	return b.db.View(func(tx *bbolt.Tx) error {
		updater, err := NewTxBoltDatabaseSystem(tx)
		if err != nil {
			return errors.Wrap(err, "error creating a tx database system")
		}

		return fn(updater)
	})
}

func (b *BoltDatabaseSystem) Close() error {
	return b.db.Close()
}

var boltBucketName = []byte("values")

type TxBoltDatabaseSystem struct {
	bucket *bbolt.Bucket
}

func NewTxBoltDatabaseSystem(tx *bbolt.Tx) (*TxBoltDatabaseSystem, error) {
	s := &TxBoltDatabaseSystem{}

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

	return t.bucket.Put(marshalSequence(seq), value)
}

func (t *TxBoltDatabaseSystem) Get(seq Sequence) ([]byte, error) {
	return t.bucket.Get(marshalSequence(seq)), nil
}

func (t *TxBoltDatabaseSystem) getNextSequence() (Sequence, error) {
	seqInt, err := t.bucket.NextSequence()
	if err != nil {
		return 0, errors.Wrap(err, "error calling next sequence")
	}

	return Sequence(seqInt - 1), nil
}
