package honeycomb

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/fxamacker/cbor/v2"
)

type DB struct {
	db *badger.DB

	option *Option
}

type Option struct {
	Version     []byte
	Seperator   []byte
	Marshaller  func(any) ([]byte, error)
	Unmarshaler func([]byte, any) error
}

// DefaultOption returns the default option, which uses CBOR as the marshaller and unmarshaller and ":" as the seperator.
func DefaultOption() *Option {
	return &Option{
		Version:     []byte("default"),
		Seperator:   []byte(":"),
		Marshaller:  cbor.Marshal,
		Unmarshaler: cbor.Unmarshal,
	}
}

func NewDB(db *badger.DB, options *Option) *DB {
	return &DB{
		db:     db,
		option: options,
	}
}

func (db *DB) Container(namespace []byte) *Container {
	return NewContainer(append(append(db.option.Version, db.option.Seperator...), namespace...), db)
}
