package db_benchmark

import (
	"github.com/boreq/errors"
	"github.com/dgraph-io/badger/v3"
)

type BadgerDatabaseSystem struct {
	db *badger.DB
}

func NewBadgerDatabaseSystem(dir string, fn func(*badger.Options)) (*BadgerDatabaseSystem, error) {
	opt := badger.
		DefaultOptions(dir).
		WithLoggingLevel(badger.ERROR)

	if fn != nil {
		fn(&opt)
	}

	db, err := badger.Open(opt)
	if err != nil {
		return nil, errors.Wrap(err, "error opening the database")
	}

	return &BadgerDatabaseSystem{db: db}, nil
}

func (b *BadgerDatabaseSystem) Update(fn func(updater Updater) error) error {
	return b.db.Update(func(tx *badger.Txn) error {
		updater, err := NewTxBadgerDatabaseSystem(tx)
		if err != nil {
			return errors.Wrap(err, "error creating a tx database system")
		}

		return fn(updater)
	})
}

func (b *BadgerDatabaseSystem) Read(fn func(reader Reader) error) error {
	return b.db.View(func(tx *badger.Txn) error {
		updater, err := NewTxBadgerDatabaseSystem(tx)
		if err != nil {
			return errors.Wrap(err, "error creating a tx database system")
		}

		return fn(updater)
	})
}

func (b *BadgerDatabaseSystem) Close() error {
	return b.db.Close()
}

func (b *BadgerDatabaseSystem) Sync() error {
	return b.db.Sync()
}

var badgerValuePrefix = []byte("value")
var badgerLastSequenceKey = []byte("last_sequence")

type TxBadgerDatabaseSystem struct {
	tx *badger.Txn
}

func NewTxBadgerDatabaseSystem(tx *badger.Txn) (*TxBadgerDatabaseSystem, error) {
	return &TxBadgerDatabaseSystem{tx: tx}, nil
}

func (t *TxBadgerDatabaseSystem) Append(value []byte) error {
	seq, err := t.getNextSequence()
	if err != nil {
		return errors.Wrap(err, "error calling get next sequence")
	}

	if err := t.tx.Set(t.valueKey(seq), value); err != nil {
		return errors.Wrap(err, "error calling set")
	}

	if err := t.setLastSequence(seq); err != nil {
		return errors.Wrap(err, "error calling set last sequence")
	}

	return nil
}

func (t *TxBadgerDatabaseSystem) Get(seq Sequence) ([]byte, error) {
	item, err := t.tx.Get(t.valueKey(seq))
	if err != nil {
		return nil, errors.Wrap(err, "error calling get")
	}

	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, errors.Wrap(err, "error calling value copy")
	}

	return value, nil
}

func (t *TxBadgerDatabaseSystem) getNextSequence() (Sequence, error) {
	item, err := t.tx.Get(badgerLastSequenceKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return 0, nil
		}

		return 0, errors.Wrap(err, "error calling next sequence")
	}

	var lastSequence Sequence

	if err := item.Value(func(val []byte) error {
		tmp := unmarshalSequence(val)
		lastSequence = tmp
		return nil
	}); err != nil {
		return 0, errors.Wrap(err, "error calling item value")
	}

	return lastSequence + 1, nil
}

func (t *TxBadgerDatabaseSystem) setLastSequence(seq Sequence) error {
	return t.tx.Set(badgerLastSequenceKey, marshalSequence(seq))
}

func (t *TxBadgerDatabaseSystem) valueKey(seq Sequence) []byte {
	return append(badgerValuePrefix, marshalSequence(seq)...)
}
