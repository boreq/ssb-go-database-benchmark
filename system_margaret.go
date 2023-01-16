package db_benchmark

import (
	"bytes"
	"io"

	"github.com/boreq/errors"
	"github.com/golang/snappy"
	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/offset2"
)

type MargaretDatabaseSystem struct {
	log *offset2.OffsetLog
}

func NewMargaretDatabaseSystem(dir string, codec margaret.Codec) (*MargaretDatabaseSystem, error) {
	log, err := offset2.Open(dir, codec)
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

func (b *MargaretDatabaseSystem) Sync() error {
	return nil
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

type MargaretCodec struct {
}

func NewMargaretCodec() *MargaretCodec {
	return &MargaretCodec{}
}

func (m MargaretCodec) Marshal(value interface{}) ([]byte, error) {
	return value.([]byte), nil
}

func (m MargaretCodec) Unmarshal(data []byte) (interface{}, error) {
	return data, nil
}

func (m MargaretCodec) NewDecoder(reader io.Reader) margaret.Decoder {
	return NewMargaretDecoder(reader)
}

func (m MargaretCodec) NewEncoder(writer io.Writer) margaret.Encoder {
	return NewMargaretEncoder(writer)
}

type MargaretEncoder struct{ w io.Writer }

func NewMargaretEncoder(w io.Writer) MargaretEncoder {
	return MargaretEncoder{w: w}
}

func (enc MargaretEncoder) Encode(v interface{}) error {
	_, err := io.Copy(enc.w, bytes.NewReader(v.([]byte)))
	return err
}

type MargaretDecoder struct{ r io.Reader }

func NewMargaretDecoder(r io.Reader) MargaretDecoder {
	return MargaretDecoder{r: r}
}

func (dec MargaretDecoder) Decode() (interface{}, error) {
	b, err := io.ReadAll(dec.r)
	if err != nil {
		return nil, err
	}
	return b, nil
}

type MargaretZSTDCodec struct {
}

func NewMargaretZSTDCodec() *MargaretZSTDCodec {
	return &MargaretZSTDCodec{}
}

func (m MargaretZSTDCodec) Marshal(value interface{}) ([]byte, error) {
	return zstdEncoder.EncodeAll(value.([]byte), nil), nil
}

func (m MargaretZSTDCodec) Unmarshal(data []byte) (interface{}, error) {
	return zstdDecoder.DecodeAll(data, nil)
}

func (m MargaretZSTDCodec) NewDecoder(reader io.Reader) margaret.Decoder {
	return NewMargaretZSTDDecoder(reader)
}

func (m MargaretZSTDCodec) NewEncoder(writer io.Writer) margaret.Encoder {
	return NewMargaretZSTDEncoder(writer)
}

type MargaretZSTDEncoder struct{ w io.Writer }

func NewMargaretZSTDEncoder(w io.Writer) MargaretZSTDEncoder {
	return MargaretZSTDEncoder{w: w}
}

func (enc MargaretZSTDEncoder) Encode(v interface{}) error {
	b := zstdEncoder.EncodeAll(v.([]byte), nil)
	_, err := io.Copy(enc.w, bytes.NewReader(b))
	return err
}

type MargaretZSTDDecoder struct{ r io.Reader }

func NewMargaretZSTDDecoder(r io.Reader) MargaretZSTDDecoder {
	return MargaretZSTDDecoder{r: r}
}

func (dec MargaretZSTDDecoder) Decode() (interface{}, error) {
	b, err := io.ReadAll(dec.r)
	if err != nil {
		return nil, errors.Wrap(err, "error reading all")
	}
	return zstdDecoder.DecodeAll(b, nil)
}

type MargaretSnappyCodec struct {
}

func NewMargaretSnappyCodec() *MargaretSnappyCodec {
	return &MargaretSnappyCodec{}
}

func (m MargaretSnappyCodec) Marshal(value interface{}) ([]byte, error) {
	return snappy.Encode(nil, value.([]byte)), nil
}

func (m MargaretSnappyCodec) Unmarshal(data []byte) (interface{}, error) {
	return snappy.Decode(nil, data)
}

func (m MargaretSnappyCodec) NewDecoder(reader io.Reader) margaret.Decoder {
	return NewMargaretSnappyDecoder(reader)
}

func (m MargaretSnappyCodec) NewEncoder(writer io.Writer) margaret.Encoder {
	return NewMargaretSnappyEncoder(writer)
}

type MargaretSnappyEncoder struct{ w io.Writer }

func NewMargaretSnappyEncoder(w io.Writer) MargaretSnappyEncoder {
	return MargaretSnappyEncoder{w: w}
}

func (enc MargaretSnappyEncoder) Encode(v interface{}) error {
	b := snappy.Encode(nil, v.([]byte))
	_, err := io.Copy(enc.w, bytes.NewReader(b))
	return err
}

type MargaretSnappyDecoder struct{ r io.Reader }

func NewMargaretSnappyDecoder(r io.Reader) MargaretSnappyDecoder {
	return MargaretSnappyDecoder{r: r}
}

func (dec MargaretSnappyDecoder) Decode() (interface{}, error) {
	b, err := io.ReadAll(dec.r)
	if err != nil {
		return nil, err
	}

	return snappy.Decode(nil, b)
}
