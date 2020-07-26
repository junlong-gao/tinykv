package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}

	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val}, err
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).

	err := server.storage.Write(req.Context,
		[]storage.Modify{
			{Data: storage.Put{
				Key: req.Key, Value: req.Value, Cf: req.Cf}}})
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).

	err := server.storage.Write(req.Context,
		[]storage.Modify{
			{Data: storage.Delete{
				Key: req.Key, Cf: req.Cf}}})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, err
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		log.Fatal(err)
	}

	ret := new(kvrpcpb.RawScanResponse)
	iter := reader.IterCF(req.Cf)
	for iter.Seek(req.StartKey); iter.Valid() && req.Limit > 0; iter.Next() {
		req.Limit--
		key := iter.Item().Key()
		val, err := iter.Item().Value()
		if err != nil {
			log.Error(err)
			ret.Error = err.Error()
			return ret, err
		}
		ret.Kvs = append(ret.Kvs, &kvrpcpb.KvPair{Key: key, Value: val})
	}
	return ret, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	server.Latches.WaitForLatches([][]byte{req.Key})
	defer server.Latches.ReleaseLatches([][]byte{req.Key})

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		panic(err)
	}

	var resp kvrpcpb.GetResponse
	txn := mvcc.NewMvccTxn(reader, req.Version)
	l, err := txn.GetLock(req.Key)
	if err != nil {
		panic(err)
	}
	if l != nil && l.Ts <= req.Version {
		resp.Error = &kvrpcpb.KeyError{Locked: l.Info(req.Key)}
	}

	v, err := txn.GetValue(req.Key)
	if err != nil {
		panic(err)
	}
	if v == nil || len(v) == 0 {
		resp.NotFound = true
	} else {
		resp.NotFound = false
		resp.Value = v
	}

	return &resp, nil
}

func (server *Server) prewriteCell(
	k []byte, v []byte,
	primary []byte,
	txn *mvcc.MvccTxn,
	lockTtl uint64,
	ctx *kvrpcpb.Context) (kerr *kvrpcpb.KeyError, err error) {
	w, lastWriteTs, err := txn.MostRecentWrite(k)
	if err != nil {
		return nil, err
	}

	if lastWriteTs > txn.StartTS {
		// As the to-commit ts will be larger than lastWriteTs,
		// this is a write-write conflict
		return &kvrpcpb.KeyError{
			Locked:    nil,
			Retryable: "",
			Abort:     "",
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:    w.StartTS,
				ConflictTs: lastWriteTs,
				Key:        k,
				Primary:    primary,
			},
		}, nil
	}

	l, err := txn.GetLock(k)
	if err != nil {
		return nil, err
	}
	if l != nil && l.Ts <= txn.StartTS {
		return &kvrpcpb.KeyError{Locked: l.Info(k)}, nil
	}

	// Now it is safe to persist the lock for the cell and write a new version of the data:
	txn.PutLock(k, &mvcc.Lock{
		Primary: primary,
		Ts:      txn.StartTS,
		Ttl:     lockTtl,
		Kind:    mvcc.WriteKindPut,
	})
	txn.PutValue(k, v)
	err = server.storage.Write(ctx, txn.Writes())
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	if len(req.Mutations) == 0 {
		return &kvrpcpb.PrewriteResponse{}, nil
	}

	var keys [][]byte
	writeBuffer := make(map[string][]byte)
	for _, mu := range req.Mutations {
		if _, ok := writeBuffer[string(mu.Key)]; !ok {
			keys = append(keys, mu.Value)
		}
		writeBuffer[string(mu.Key)] = mu.Value
	}

	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	var resp kvrpcpb.PrewriteResponse
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		// XXX this can be region error
		panic(err)
	}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for k, val := range writeBuffer {
		kerr, err := server.prewriteCell([]byte(k), val, req.PrimaryLock, txn, req.LockTtl, req.Context)
		if err != nil {
			panic(err)
			// it really should be: resp.RegionError = err, unfortunately, layering loses the error details
		}
		if kerr != nil {
			resp.Errors = append(resp.Errors, kerr)
			return &resp, nil
		}
	}

	return &resp, nil
}

func (server *Server) commit(k []byte, startTs, commitTs uint64, txn *mvcc.MvccTxn) *kvrpcpb.KeyError {
	l, err := txn.GetLock(k)
	if err != nil {
		panic(err)
	}

	// check if it is rolled back or has been committed by a different txn:
	w, ct, err := txn.MostRecentWrite(k)
	if err != nil {
		panic(err)
	}

	if w != nil {
		if w.Kind == mvcc.WriteKindRollback {
			return nil
		}

		if commitTs == ct {
			if startTs != w.StartTS {
				panic("Protocol error")
			}
			// do nothing, already committed, don't check the lock or write
			// to kv. skip this key
			return nil
		}

		if ct > startTs {
			// committing a w-w conflict, even if it does not overlap with any
			// writes before.
			return &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    w.StartTS,
					ConflictTs: commitTs,
					Key:        k,
				},
			}
		}
	}

	// either w is nil or the latest write is below this req.StartVersion

	// check if it has a lock:
	if l == nil {
		// no lock at all. this commit has no prewrite as it passes the
		// previous write check
		return nil
	}

	if l.Ts != startTs {
		// conflict, someone is committing with us
		return &kvrpcpb.KeyError{Retryable: string(k)}
	}

	// commit:
	txn.DeleteLock(k)
	txn.PutWrite(k, commitTs, &mvcc.Write{
		StartTS: startTs,
		Kind:    mvcc.WriteKindPut,
	})
	return nil
}

/*
Commit a pre-write txn. Give up and leave the kv unchanged if
1. any of the k does not have a matching startTS lock (aborted), or
2. any of the k has a committed conflict write

In particular, this rpc is idempotent: on committed write with the
same startTS and commitTS, it does nothing.
*/
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	if len(req.Keys) == 0 {
		return &kvrpcpb.CommitResponse{}, nil
	}

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		panic(err)
	}

	var resp kvrpcpb.CommitResponse

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, k := range req.Keys {
		kerr := server.commit(k, req.StartVersion, req.CommitVersion, txn)
		if kerr != nil {
			resp.Error = kerr
			return &resp, nil
		}
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		panic(err)
	}

	return &resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		panic(err)
	}

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	var resp kvrpcpb.ScanResponse
	for len(resp.Pairs) < int(req.Limit) {
		k, v, err := scanner.Next()
		if err != nil {
			panic(err)
		}
		if k == nil {
			return &resp, nil
		}
		if v == nil {
			// deleted
			continue
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Error: nil,
			Key:   k,
			Value: v,
		})
	}

	return &resp, nil
}

func (server *Server) rollback(key []byte, txn *mvcc.MvccTxn) *kvrpcpb.KeyError {
	l, err := txn.GetLock(key)
	if err != nil {
		panic(err)
	}
	if l != nil && l.Ts == txn.StartTS {
		txn.DeleteLock(key)
	}

	// check if it is rolled back at this timestamp:
	v, err := txn.Reader.GetCF(engine_util.CfWrite, mvcc.EncodeKey(key, txn.StartTS))
	if err != nil {
		panic(err)
	}
	if v != nil {
		w, err := mvcc.ParseWrite(v)
		if err != nil {
			panic(err)
		}
		if w.Kind == mvcc.WriteKindRollback {
			return nil
		} else {
			// already committed
			return &kvrpcpb.KeyError{Abort: string(key)}
		}
	}

	w, _, err := txn.CurrentWrite(key)
	if err != nil {
		panic(err)
	}
	if w != nil {
		// already committed, do nothing
		return &kvrpcpb.KeyError{Abort: string(key)}
	} else {
		// there is no such commit record, rollback
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
		txn.DeleteValue(key)
	}

	return nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		panic(err)
	}
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	w, err := reader.GetCF(engine_util.CfWrite, mvcc.EncodeKey(req.PrimaryKey, req.LockTs))
	if err != nil {
		panic(err)
	}

	if w != nil {
		commitTs := uint64(0)
		v, err := mvcc.ParseWrite(w)
		if err != nil {
			panic(err)
		}
		if v.Kind == mvcc.WriteKindPut {
			commitTs = v.StartTS
		}

		return &kvrpcpb.CheckTxnStatusResponse{
			CommitVersion: commitTs,
			Action:        kvrpcpb.Action_NoAction,
		}, nil
	}

	l, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		panic(err)
	}
	if l == nil {
		// no lock, issue a rollback record
		kerr := server.rollback(req.PrimaryKey, txn)
		if kerr != nil {
			panic(kerr)
		}
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			panic(err)
		}
		return &kvrpcpb.CheckTxnStatusResponse{
			Action: kvrpcpb.Action_LockNotExistRollback,
		}, nil
	}

	if l.Ttl+mvcc.PhysicalTime(req.LockTs) >= mvcc.PhysicalTime(req.CurrentTs) {
		return &kvrpcpb.CheckTxnStatusResponse{
			Action: kvrpcpb.Action_NoAction,
		}, nil
	}

	// we don't have a commit record and have a lock expired, we can rollback
	kerr := server.rollback(req.PrimaryKey, txn)
	if kerr != nil {
		panic(kerr)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		panic(err)
	}
	return &kvrpcpb.CheckTxnStatusResponse{
		Action: kvrpcpb.Action_TTLExpireRollback,
	}, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	var resp kvrpcpb.BatchRollbackResponse
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		panic(err)
	}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, k := range req.Keys {
		kerr := server.rollback(k, txn)
		if kerr != nil {
			resp.Error = kerr
			return &resp, nil
		}
	}

	server.storage.Write(req.Context, txn.Writes())

	return &resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		panic(err)
	}
	iter := reader.IterCF(engine_util.CfLock)
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for iter.Seek([]byte("")); iter.Valid(); iter.Next() {
		v, err := iter.Item().Value()
		if err != nil {
			panic(err)
		}
		l, err := mvcc.ParseLock(v)
		if err != nil {
			panic(err)
		}
		if l.Ts != req.StartVersion {
			continue
		}

		kerr := &kvrpcpb.KeyError{}
		if req.CommitVersion == 0 {
			kerr = server.rollback(iter.Item().Key(), txn)
		} else {
			kerr = server.commit(iter.Item().Key(), req.StartVersion, req.CommitVersion, txn)
		}
		if kerr != nil {
			return &kvrpcpb.ResolveLockResponse{
				RegionError: nil,
				Error:       kerr,
			}, nil
		}

	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		panic(err)
	}
	return &kvrpcpb.ResolveLockResponse{}, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
