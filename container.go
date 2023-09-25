package honeycomb

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
)

type Container struct {
	namespace []byte
	db        *DB
}

func NewContainer(namespace []byte, db *DB) *Container {
	return &Container{
		namespace: namespace,
		db:        db,
	}
}

func (c *Container) Container(namespace []byte) *Container {
	return NewContainer(append(append(c.namespace, c.db.option.Seperator...), namespace...), c.db)
}

func (c *Container) Has(key []byte) bool {
	var has = false
	err := c.db.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(append(c.namespace, key...))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				has = false
				return nil
			}
			return err
		}
		has = true
		return nil
	})
	return has && err == nil
}

func (c *Container) Get(key []byte) ([]byte, error) {
	var value []byte
	err := c.db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(append(c.namespace, key...))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			value = val
			return nil
		})
	})
	return value, err
}

func (c *Container) GetObject(key []byte, dst any) error {
	return c.db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(append(c.namespace, key...))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return c.db.option.Unmarshaler(val, dst)
		})
	})
}

func (c *Container) Set(key, value []byte) error {
	return c.db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(append(c.namespace, key...), value)
	})
}

func (c *Container) SetObject(key []byte, value any) error {
	data, err := c.db.option.Marshaller(value)
	if err != nil {
		return err
	}
	return c.db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(append(c.namespace, key...), data)
	})
}

func (c *Container) Delete(key []byte) error {
	return c.db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(append(c.namespace, key...))
	})
}

func (c *Container) Iter(iterFunc func(key, value []byte) error) error {
	return c.db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(c.namespace); it.ValidForPrefix(c.namespace); it.Next() {
			item := it.Item()
			key := item.Key()
			if err := item.Value(func(val []byte) error {
				return iterFunc(key[len(c.namespace):], val)
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *Container) IterPrefix(prefix []byte, iterFunc func(key, value []byte) error) error {
	return c.db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(append(c.namespace, prefix...)); it.ValidForPrefix(append(c.namespace, prefix...)); it.Next() {
			item := it.Item()
			if err := item.Value(func(val []byte) error {
				return iterFunc(item.Key()[len(c.namespace)+len(prefix):], val)
			}); err != nil {
				return err
			}
		}
		return nil
	})
}
