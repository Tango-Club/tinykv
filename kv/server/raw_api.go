package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

var (
	NotFoundResponese = &kvrpcpb.RawGetResponse{
		NotFound: true,
	}
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return NotFoundResponese, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return NotFoundResponese, err
	}

	if val == nil {
		return NotFoundResponese, nil
	}

	return &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: false,
	}, nil
}

func RawModify[T kvrpcpb.RawDeleteResponse | kvrpcpb.RawPutResponse](server *Server, modify []storage.Modify) (*T, error) {
	err := server.storage.Write(nil, modify)
	if err != nil {
		return &T{
			Error: err.Error(),
		}, err
	}
	return &T{}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	return RawModify[kvrpcpb.RawPutResponse](server, []storage.Modify{{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		},
	}})
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	return RawModify[kvrpcpb.RawDeleteResponse](server, []storage.Modify{{
		Data: storage.Delete{
			Cf:  req.GetCf(),
			Key: req.GetKey(),
		},
	}})
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	iter.Seek(req.GetStartKey())

	var kvs []*kvrpcpb.KvPair
	limit := req.Limit
	for limit > 0 && iter.Valid() {
		val, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}

		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: val,
		})

		iter.Next()
		limit--
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
