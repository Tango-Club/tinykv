package standalone_storage

import (
	"log"
	"os"
	"sync"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db     *badger.DB
	config *config.Config
	once   *sync.Once
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		config: conf,
		once:   &sync.Once{},
	}
}

func (s *StandAloneStorage) Start() error {

	dbPath := s.config.DBPath

	// stat file and if the file is not exist then createDB Otherwise just open
	_, err := os.Stat(dbPath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatalf("standard alone sotrage start failed :%s", err)
		}
		s.db = engine_util.CreateDB(dbPath, false)
		return nil
	}

	opt := badger.DefaultOptions
	if s.config.Raft {
		opt.ValueThreshold = 0
	}
	opt.Dir = dbPath
	opt.ValueDir = dbPath
	s.db, err = badger.Open(opt)

	return err
}

func (s *StandAloneStorage) Stop() error {
	s.once.Do(func() {
		s.db.Close()
	})
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{
		db: s.db,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := &engine_util.WriteBatch{}
	for _, v := range batch {
		if v.Value() == nil {
			wb.DeleteCF(v.Cf(), v.Key())
		} else {
			wb.SetCF(v.Cf(), v.Key(), v.Value())
		}
	}
	err := wb.WriteToDB(s.db)
	return err
}

// check the interface is implement by StandAloneStorageReader
var _ storage.StorageReader = new(StandAloneStorageReader)

type StandAloneStorageReader struct {
	db  *badger.DB
	txn *badger.Txn
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()
	val, err := engine_util.GetCFFromTxn(txn, cf, key)
	if err != nil {
		if err.Error() == badger.ErrKeyNotFound.Error() {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	if s.txn == nil {
		s.txn = s.db.NewTransaction(false)
	}
	return engine_util.NewCFIterator(cf, s.txn)
}

//Close
func (s *StandAloneStorageReader) Close() {
	if s.txn != nil {
		s.txn.Discard()
	}
}
