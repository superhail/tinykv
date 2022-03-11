package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	DBPath string
	db     *badger.DB
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	ret := StandAloneStorage{DBPath: conf.DBPath}
	return &ret
}

func (s *StandAloneStorage) Start() error {
	if s.db != nil { // if already started just return
		return nil
	}
	opts := badger.DefaultOptions
	opts.Dir = s.DBPath
	opts.ValueDir = s.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if s.db == nil {
		return errors.New("StandAloneStorage::Stop(): cannot stop if not started")
	}
	err := s.db.Close()
	if err != nil {
		return err
	}
	/* after closing it, set s.db to nil */
	s.db = nil
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	if s.db == nil {
		return nil, errors.New("StandAloneStorage::Reader(): cannot create reader, if no started")
	}
	txn := s.db.NewTransaction(false)
	reader := StandAloneStorageReader{txn: txn}

	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			switch m.Data.(type) {
			case storage.Put:
				key := append([]byte(m.Cf() + "_"), m.Key()...)
				set_err := txn.Set(key, m.Value())
				if (set_err != nil) {
					return set_err
				}
			case storage.Delete:
				key := append([]byte(m.Cf() + "_"), m.Key()...)
				delete_err := txn.Delete(key)
				if (delete_err != nil) {
					return delete_err
				}
			}
		}
		return nil
	})
	return err
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := r.txn.Get(append([]byte(cf + "_"), key...))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	val, err := item.Value()
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
	return
}
