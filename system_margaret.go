package db_benchmark

import (
	"bytes"
	"github.com/boreq/errors"
	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/offset2"
	"io"
)

type MargaretDatabaseSystem struct {
	log *offset2.OffsetLog
}

func NewMargaretDatabaseSystem(dir string) (*MargaretDatabaseSystem, error) {
	log, err := offset2.Open(dir, newMargaretCodec())
	if err != nil {
		return nil, errors.Wrap(err, "error calling open")
	}

	return &MargaretDatabaseSystem{log: log}, nil
}

func (b *MargaretDatabaseSystem) Update(fn func(updater Updater) error) error {
	v := NewMargaretReaderUpdater(b.log)
	return fn(v)
}

func (b *MargaretDatabaseSystem) Read(fn func(reader Reader) error) error {
	v := NewMargaretReaderUpdater(b.log)
	return fn(v)
}

func (b *MargaretDatabaseSystem) Close() error {
	return b.log.Close()
}

type MargaretReaderUpdater struct {
	log *offset2.OffsetLog
}

func NewMargaretReaderUpdater(log *offset2.OffsetLog) *MargaretReaderUpdater {
	return &MargaretReaderUpdater{log: log}
}

func (m *MargaretReaderUpdater) Append(value []byte) error {
	_, err := m.log.Append(value)
	return err
}

func (m *MargaretReaderUpdater) Get(seq Sequence) ([]byte, error) {
	v, err := m.log.Get(int64(seq))
	if err != nil {
		return nil, errors.Wrap(err, "error calling get")
	}

	return v.([]byte), nil
}

type margaretCodec struct {
}

func newMargaretCodec() *margaretCodec {
	return &margaretCodec{}
}

func (m margaretCodec) Marshal(value interface{}) ([]byte, error) {
	return value.([]byte), nil
}

func (m margaretCodec) Unmarshal(data []byte) (interface{}, error) {
	return data, nil
}

func (m margaretCodec) NewDecoder(reader io.Reader) margaret.Decoder {
	return newMargaretDecoder(reader)
}

func (m margaretCodec) NewEncoder(writer io.Writer) margaret.Encoder {
	return newMargaretEncoder(writer)
}

type margaretEncoder struct{ w io.Writer }

func newMargaretEncoder(w io.Writer) margaretEncoder {
	return margaretEncoder{w: w}
}

func (enc margaretEncoder) Encode(v interface{}) error {
	_, err := io.Copy(enc.w, bytes.NewReader(v.([]byte)))
	return err
}

type margaretDecoder struct{ r io.Reader }

func newMargaretDecoder(r io.Reader) margaretDecoder {
	return margaretDecoder{r: r}
}

func (dec margaretDecoder) Decode() (interface{}, error) {
	b, err := io.ReadAll(dec.r)
	if err != nil {
		return nil, err
	}
	return b, nil
}
