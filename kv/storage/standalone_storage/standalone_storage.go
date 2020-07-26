package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	// "github.com/pingcap-incubator/tinykv/log"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).

	Db   *badger.DB
	Conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	return &StandAloneStorage{
		Conf: conf,
		Db:   engine_util.CreateDB("", conf),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	//
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).

	s.Db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	return NewStandAloneReader(s.Db.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	wb := engine_util.WriteBatch{}
	for _, b := range batch {
		wb.SetCF(b.Cf(), b.Key(), b.Value())
	}
	return wb.WriteToDB(s.Db)
}
