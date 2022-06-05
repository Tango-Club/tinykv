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

func (storage *StandAloneStorage) Start() error {
	dbPath := storage.config.DBPath

	// stat file and if the file is not exist then createDB otherwise just open
	_, err := os.Stat(dbPath)
	if err != nil {
		if os.IsExist(err) {
			log.Fatalf("standard alone sotrage start failed :%s", err)
		}
		storage.db = engine_util.CreateDB(dbPath, false)
		return nil
	}

	opt := badger.DefaultOptions
	if storage.config.Raft {
		opt.ValueThreshold = 0
	}
	opt.Dir = dbPath
	opt.ValueDir = dbPath

	storage.db, err = badger.Open(opt)
	return err
}

func (storage *StandAloneStorage) Stop() error {
	storage.once.Do(func() {
		storage.db.Close()
	})
	return nil
}

func (storage *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{
		db:  storage.db,
		txn: storage.db.NewTransaction(false),
	}, nil
}

func (storage *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := &engine_util.WriteBatch{}
	for _, v := range batch {
		if v.Value() == nil {
			wb.DeleteCF(v.Cf(), v.Key())
		} else {
			wb.SetCF(v.Cf(), v.Key(), v.Value())
		}
	}
	err := wb.WriteToDB(storage.db)
	return err
}

// check the interface is implement by StandAloneStorageReader
var _ storage.StorageReader = new(StandAloneStorageReader)

type StandAloneStorageReader struct {
	db  *badger.DB
	txn *badger.Txn
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err != nil {
		if err.Error() == badger.ErrKeyNotFound.Error() {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader *StandAloneStorageReader) Close() {
	reader.txn.Discard()
}
