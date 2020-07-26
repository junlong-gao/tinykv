package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	currentKey []byte
	snapshot   uint64
	reader     storage.StorageReader
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{currentKey: startKey, reader: txn.Reader, snapshot: txn.StartTS}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for scan.currentKey != nil {
		var retv []byte
		var retk []byte
		iter := scan.reader.IterCF(engine_util.CfWrite)
		for iter.Seek(EncodeKey(scan.currentKey, scan.snapshot)); iter.Valid() && bytes.Equal(DecodeUserKey(iter.Item().Key()), scan.currentKey); iter.Next() {
			v, err := iter.Item().Value()
			if err != nil {
				panic(err)
			}
			w, err := ParseWrite(v)
			if err != nil {
				panic(err)
			}

			retk = DecodeUserKey(iter.Item().Key())
			if w.Kind == WriteKindDelete {
				// fall through, use nil value indicate deletion, caller should skip this key
			} else if w.Kind == WriteKindPut {
				// find the value and fall though
				val, err := scan.reader.GetCF(engine_util.CfDefault, EncodeKey(retk, w.StartTS))
				if err != nil {
					panic(err)
				}

				retv = val
			} else {
				// this is a rollback, skip the key here
				continue
			}

			// skip this key
			iter.Seek(EncodeKey(scan.currentKey, 0))
			if iter.Valid() && bytes.Equal(DecodeUserKey(iter.Item().Key()), scan.currentKey) {
				iter.Next()
			}
			break
		}

		if iter.Valid() {
			newKey := DecodeUserKey(iter.Item().Key())
			scan.currentKey = newKey
		} else {
			scan.currentKey = nil
		}

		if retk != nil {
			return retk, retv, nil
		} else {
			continue
		}
	}

	return nil, nil, nil
}
